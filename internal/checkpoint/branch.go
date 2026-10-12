package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// Branch entry lifecycle states.
//
// A registration moves through these states only during branch decommissioning
// (the unfork handoff, see handoff.go):
//
//	"" (active)     — written by Fork; fully protective, the historical state.
//	"solidifying"    — a handoff is in progress: the branch is copying the
//	                   ancestor SSTs it still references into its own store and
//	                   publishing an independent checkpoint. The entry keeps the
//	                   same GC protection as an active entry.
//	"released"       — terminal tombstone: the independent checkpoint has been
//	                   verified and GC may reclaim the ancestor checkpoint and its
//	                   SSTs. The entry is deleted on the next handoff retry;
//	                   GC treats it as absent in the meantime.
const (
	BranchStateSolidifying = "solidifying"
	BranchStateReleased    = "released"
)

// BranchEntry is stored at "branches/<id>" in the source object store.
// It records the source checkpoint that the branch was created from so that
// GCOrphanSSTs can protect the SST files the branch still needs.
//
// The handoff fields are zero for a freshly forked branch and only become
// set while Unfork runs its recoverable handoff. Adding them here (rather
// than in a side object) makes "entry present and not released" the single
// GC protection gate: a half-finished handoff can never look released.
type BranchEntry struct {
	// AncestorCheckpointKey is the v2 CheckpointIndex key in this (source) store
	// that the branch was bootstrapped from.
	// GC reads this index to find which SST files must be retained.
	AncestorCheckpointKey string `json:"ancestor_checkpoint_key"`

	// HandoffID identifies the decommission handoff that owns this entry.
	// Set when State moves to "solidifying". Every later transition is fenced
	// on this ID, so a stale retry from an earlier generation (e.g. after
	// the branch was re-forked under a new handoff) can never release a
	// registration it does not own.
	HandoffID string `json:"handoff_id,omitempty"`

	// State is "" (active), BranchStateSolidifying, or BranchStateReleased.
	State string `json:"state,omitempty"`

	// IndependentCheckpointKey is the branch-store checkpoint index that no longer
	// references any ancestor SSTs. Recorded on the released tombstone so the
	// release decision stays inspectable after the branch-side marker is gone.
	IndependentCheckpointKey string `json:"independent_checkpoint_key,omitempty"`

	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// Released reports whether the entry is a terminal tombstone, which GC must not
// pin and which may be deleted.
func (e *BranchEntry) Released() bool {
	return e != nil && e.State == BranchStateReleased
}

// RegisterBranch writes a branch registry entry to the source store.
// Call before starting the branch node to protect source SSTs from GC.
//
// Re-registering an existing active registration or a released tombstone
// overwrites it (last-writer-wins, as before the handoff states existed);
// the write is fenced by the read ETag so it cannot clobber a concurrent
// writer. An entry whose decommission handoff is in flight
// (state "solidifying") is rejected: abandon or finish that handoff first.
func (mgr *Manager) RegisterBranch(ctx context.Context, store object.Store, id, ancestorCheckpointKey string) error {
	for attempt := 0; attempt < 3; attempt++ {
		old, etag, err := mgr.ReadBranchEntry(ctx, store, id)
		if err != nil {
			return fmt.Errorf("checkpoint: register branch %q: %w", id, err)
		}
		if old != nil && old.State == BranchStateSolidifying {
			return fmt.Errorf("checkpoint: register branch %q: %w", id, ErrHandoffInProgress)
		}
		entry := BranchEntry{
			AncestorCheckpointKey: ancestorCheckpointKey,
			UpdatedAt:            time.Now().UTC(),
		}
		// No existing entry → create-only; otherwise fenced overwrite.
		writeETag := etag
		if old == nil {
			writeETag = ""
		}
		if err := mgr.writeBranchEntry(ctx, store, id, &entry, writeETag); err != nil {
			if errors.Is(err, object.ErrPreconditionFailed) {
				continue // someone else wrote concurrently; re-read and retry
			}
			return fmt.Errorf("checkpoint: register branch %q: %w", id, err)
		}
		return nil
	}
	return fmt.Errorf("checkpoint: register branch %q: %w", id, object.ErrPreconditionFailed)
}

// UnregisterBranch removes the branch registry entry from the source store.
// Call when the branch is decommissioned and its source SSTs are no longer needed.
//
// Prefer Unfork/RunHandoff for branches that still reference ancestor SSTs:
// unregistering alone lets the next GC reclaim SSTs a live branch or its
// restart may still depend on. This direct delete remains for callers that have
// already made the branch independent by other means.
func (mgr *Manager) UnregisterBranch(ctx context.Context, store object.Store, id string) error {
	if err := store.Delete(ctx, branchKey(id)); err != nil {
		return fmt.Errorf("checkpoint: unregister branch %q: %w", id, err)
	}
	return nil
}

// ReadBranchEntry returns one branch entry and its ETag (empty for stores that
// do not implement ConditionalStore). Missing entries return (nil, "", nil).
func (mgr *Manager) ReadBranchEntry(ctx context.Context, store object.Store, id string) (*BranchEntry, string, error) {
	if cs, ok := store.(object.ConditionalStore); ok {
		cur, err := cs.GetETag(ctx, branchKey(id))
		if errors.Is(err, object.ErrNotFound) {
			return nil, "", nil
		}
		if err != nil {
			return nil, "", fmt.Errorf("checkpoint: read branch %q: %w", id, err)
		}
		var e BranchEntry
		decErr := json.NewDecoder(cur.Body).Decode(&e)
		_ = cur.Body.Close()
		if decErr != nil {
			return nil, "", fmt.Errorf("checkpoint: decode branch %q: %w", id, decErr)
		}
		return &e, cur.ETag, nil
	}
	rc, err := store.Get(ctx, branchKey(id))
	if errors.Is(err, object.ErrNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("checkpoint: read branch %q: %w", id, err)
	}
	var e BranchEntry
	decErr := json.NewDecoder(rc).Decode(&e)
	_ = rc.Close()
	if decErr != nil {
		return nil, "", fmt.Errorf("checkpoint: decode branch %q: %w", id, decErr)
	}
	return &e, "", nil
}

// writeBranchEntry writes the entry, fencing on etag when the store supports
// conditional writes: etag == "" means create (PutIfAbsent), a non-empty
// etag means update only if unchanged (PutIfMatch). Plain stores fall back to
// a read-check-write when creating, and to a plain write otherwise, matching
// the manifest's strategy for stores such as older radosgw.
func (mgr *Manager) writeBranchEntry(ctx context.Context, store object.Store, id string, entry *BranchEntry, etag string) error {
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	key := branchKey(id)
	cs, ok := store.(object.ConditionalStore)
	if !ok {
		if etag == "" {
			if old, _, rerr := mgr.ReadBranchEntry(ctx, store, id); rerr != nil {
				return rerr
			} else if old != nil {
				return object.ErrPreconditionFailed
			}
		}
		return store.Put(ctx, key, bytes.NewReader(b))
	}
	if etag == "" {
		err = cs.PutIfAbsent(ctx, key, bytes.NewReader(b))
	} else {
		err = cs.PutIfMatch(ctx, key, bytes.NewReader(b), etag)
	}
	if err != nil {
		return err
	}
	return nil
}

// deleteBranchEntryIfHandoff removes the branches/<id> entry only while it still
// belongs to handoffID (state "released"). It is a no-op when the entry is
// already gone or never belonged to this handoff; the latter protects a freshly
// re-registered branch against a stale handoff's cleanup. The ETag recheck
// narrows the re-register window on stores without conditional deletes.
func (mgr *Manager) deleteBranchEntryIfHandoff(ctx context.Context, store object.Store, id, handoffID string) error {
	entry, _, err := mgr.ReadBranchEntry(ctx, store, id)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil
	}
	if !entry.Released() || entry.HandoffID != handoffID {
		// A new generation owns the key now; leave it alone.
		return nil
	}
	if err := store.Delete(ctx, branchKey(id)); err != nil {
		return fmt.Errorf("checkpoint: delete released branch %q: %w", id, err)
	}
	return nil
}

// ReadBranchEntries returns all branch registry entries in the store, including
// released tombstones (callers decide how to treat them; GC skips them via
// BranchEntry.Released).
func (mgr *Manager) ReadBranchEntries(ctx context.Context, store object.Store) (map[string]BranchEntry, error) {
	keys, err := store.List(ctx, "branches/")
	if err != nil {
		return nil, fmt.Errorf("checkpoint: list branches: %w", err)
	}
	entries := make(map[string]BranchEntry, len(keys))
	for _, k := range keys {
		rc, err := store.Get(ctx, k)
		if err == object.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("checkpoint: read branch %q: %w", k, err)
		}
		var e BranchEntry
		decErr := json.NewDecoder(rc).Decode(&e)
		rc.Close()
		if decErr != nil {
			return nil, fmt.Errorf("checkpoint: decode branch %q: %w", k, decErr)
		}
		id := strings.TrimPrefix(k, "branches/")
		entries[id] = e
	}
	return entries, nil
}

func branchKey(id string) string { return "branches/" + id }
