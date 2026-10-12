package checkpoint

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// Recoverable branch-decommission handoff.
//
// Background: a forked branch node's checkpoints record the SSTs they still
// share with the source store as AncestorSSTFiles. Unfork used to delete the
// source registration immediately, so the very next source GC could reclaim
// ancestor SSTs that a running branch, or its restart from its own
// checkpoint, still needed.
//
// The handoff replaces that single delete with a durable state machine:
//
//  1. Begin: pin branch identity (branchID + random HandoffID), the ancestor
//     checkpoint key (from the source entry) and the current branch revision
//     (the branch store's manifest/latest). The source entry moves
//     active → "solidifying"; GC keeps protecting it throughout.
//  2. Solidify: every ancestor SST the branch head checkpoint still references is
//     copied (hash-verified, content-addressed) into the branch store, and the
//     head checkpoint index is rewritten to reference all SSTs locally — a
//     verifiable independent checkpoint. A live branch node performs the same
//     promotion itself (see SSTUploader.PromoteInherited), so checkpoints it
//     writes afterwards are independent as well.
//  3. Complete: the independent head is verified from the stores (zero ancestor
//     references, every SST and meta object present, revision ≥ the pinned
//     one). Only then is the source entry CAS-fenced to a "released"
//     tombstone and deleted, allowing GC to reclaim.
//
// All progress lives in the stores, so a crash/cancel during object copy,
// checkpoint publish or registration update resumes the SAME handoff (same
// HandoffID); generation fencing makes a stale retry fail against a newer
// fork/decommission generation instead of releasing the wrong registration.

const (
	// HandoffActiveKey is the branch-store object holding the in-progress
	// handoff. Its presence is also the signal a live branch node watches
	// for to promote its inherited SSTs.
	HandoffActiveKey = "branch-handoff/active.json"
	// HandoffReleasedKey records the finished handoff permanently. It lets a
	// duplicate decommission return the original result, and keeps a restarted
	// branch node promoting inherited SSTs even after active.json is gone.
	HandoffReleasedKey = "branch-handoff/released.json"

	HandoffPhaseSolidifying = "solidifying"
	HandoffPhaseSolidified  = "solidified"
	HandoffPhaseDone        = "done"

	handoffSchemaVersion uint32 = 1
)

// HandoffState is the durable handoff record stored in the branch object
// store (HandoffActiveKey while running, HandoffReleasedKey once done).
type HandoffState struct {
	SchemaVersion uint32 `json:"schema_version"`

	// HandoffID is the random generation identifier fixed at Begin. Retries
	// must keep using it; every entry/marker transition is fenced on it.
	HandoffID string `json:"handoff_id"`
	// BranchID is the source-store registration this handoff owns.
	BranchID string `json:"branch_id"`

	// AncestorCheckpointKey is pinned from the source entry at Begin.
	AncestorCheckpointKey string `json:"ancestor_checkpoint_key"`
	// PinnedCheckpointKey/PinnedTerm/PinnedRevision pin the branch revision
	// current at Begin. The independent checkpoint must cover at least this point.
	PinnedCheckpointKey string `json:"pinned_checkpoint_key"`
	PinnedTerm          uint64 `json:"pinned_term"`
	PinnedRevision      int64  `json:"pinned_revision"`

	// Phase is solidifying → solidified (active marker) → done (released).
	Phase string `json:"phase"`

	// IndependentCheckpointKey is the branch-store checkpoint index proven
	// independent of ancestor SSTs.
	IndependentCheckpointKey string `json:"independent_checkpoint_key,omitempty"`

	// CopiedSSTs tracks ancestor SST keys already copied to the branch
	// store, so an interrupted copy resumes instead of restarting.
	CopiedSSTs []string `json:"copied_ssts,omitempty"`

	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// HandoffStatus is a queryable snapshot of a branch decommission, safe to
// show to operators while it is in progress or after it finished.
type HandoffStatus struct {
	BranchID string
	// SourceEntryState is "not-registered", "active", "solidifying" or
	// "released".
	SourceEntryState string
	// Phase is the branch-side phase: "", "solidifying", "solidified"
	// or "done".
	Phase string

	HandoffID                string
	AncestorCheckpointKey    string
	PinnedCheckpointKey      string
	IndependentCheckpointKey string
	CopiedSSTs               int
	ReferencedAncestorSSTs   int
}

// Decommission handoff sentinel errors.
var (
	// ErrBranchNotRegistered: no source registration and no finished handoff
	// exists for this branch id.
	ErrBranchNotRegistered = errors.New("checkpoint: branch is not registered")
	// ErrHandoffConflict: stored state belongs to a different handoff
	// generation than the one being resumed; the caller must not touch it.
	ErrHandoffConflict = errors.New("checkpoint: branch handoff belongs to another generation")
	// ErrHandoffInProgress: a fork was attempted while the branch entry
	// is mid-handoff.
	ErrHandoffInProgress = errors.New("checkpoint: branch decommission is in progress")
	// ErrNotIndependent: the checkpoint claimed independent still references
	// ancestor SSTs or misses objects in the branch store.
	ErrNotIndependent = errors.New("checkpoint: checkpoint is not independent of the ancestor store")
)

func newHandoffID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand should never fail; fall back to time rather than panic.
		return fmt.Sprintf("h-%d", time.Now().UnixNano())
	}
	return "h-" + hex.EncodeToString(b[:])
}

// ── JSON/CAS primitives ──────────────────────────────────────────────────────────

// getJSONETag reads key into dst and returns its ETag. Missing keys
// return ("", object.ErrNotFound).
func getJSONETag(ctx context.Context, store object.Store, key string, dst any) (string, error) {
	if cs, ok := store.(object.ConditionalStore); ok {
		cur, err := cs.GetETag(ctx, key)
		if err != nil {
			return "", err
		}
		err = json.NewDecoder(cur.Body).Decode(dst)
		_ = cur.Body.Close()
		return cur.ETag, err
	}
	rc, err := store.Get(ctx, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	return "", json.NewDecoder(rc).Decode(dst)
}

// putJSONCAS writes v to key. etag=="" creates the object only if absent;
// a non-empty etag updates only if the object is unchanged. Stores without
// conditional writes get a read-check Put for creation and a plain Put for
// updates (same trade-offs as the manifest writer).
func putJSONCAS(ctx context.Context, store object.Store, key string, v any, etag string) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	cs, ok := store.(object.ConditionalStore)
	if !ok {
		if etag == "" {
			if rc, gerr := store.Get(ctx, key); gerr == nil {
				_ = rc.Close()
				return object.ErrPreconditionFailed
			} else if !errors.Is(gerr, object.ErrNotFound) {
				return gerr
			}
		}
		return store.Put(ctx, key, bytes.NewReader(b))
	}
	if etag == "" {
		return cs.PutIfAbsent(ctx, key, bytes.NewReader(b))
	}
	return cs.PutIfMatch(ctx, key, bytes.NewReader(b), etag)
}

func (mgr *Manager) readHandoff(ctx context.Context, store object.Store, key string) (*HandoffState, string, error) {
	var st HandoffState
	etag, err := getJSONETag(ctx, store, key, &st)
	if err != nil {
		return nil, "", err
	}
	return &st, etag, nil
}

// ReadActiveHandoff returns the in-progress handoff marker in the branch
// store, or (nil, "", nil) when none exists. Branch nodes use it to learn
// when they must promote inherited SSTs before their next checkpoint.
func (mgr *Manager) ReadActiveHandoff(ctx context.Context, branchStore object.Store) (*HandoffState, error) {
	st, _, err := mgr.readHandoff(ctx, branchStore, HandoffActiveKey)
	if errors.Is(err, object.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read handoff: %w", err)
	}
	return st, nil
}

// ReadReleasedHandoff returns the finished handoff marker, or nil when
// none exists. Branch nodes read it after a restart following a completed
// decommission.
func (mgr *Manager) ReadReleasedHandoff(ctx context.Context, branchStore object.Store) (*HandoffState, error) {
	st, _, err := mgr.readHandoff(ctx, branchStore, HandoffReleasedKey)
	if errors.Is(err, object.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read released handoff: %w", err)
	}
	return st, nil
}

func (mgr *Manager) saveActive(ctx context.Context, store object.Store, st *HandoffState, etag string) (string, error) {
	st.UpdatedAt = time.Now().UTC()
	if err := putJSONCAS(ctx, store, HandoffActiveKey, st, etag); err != nil {
		return "", err
	}
	// Best-effort fresh ETag for the next CAS; "" makes the next
	// write an unconditional Put on plain stores and forces a re-read on
	// conditional stores, which is the safe fallback either way.
	_, newETag, err := mgr.readHandoff(ctx, store, HandoffActiveKey)
	if err == nil {
		return newETag, nil
	}
	return "", nil
}

// checkpointAtLeast reports whether (term,rev) is at or beyond the pin.
func checkpointAtLeast(term uint64, rev int64, minTerm uint64, minRev int64) bool {
	return term > minTerm || (term == minTerm && rev >= minRev)
}

// ── Begin ────────────────────────────────────────────────────────────────────────

// BeginHandoff pins the handoff generation and returns its durable state.
// It is idempotent: an existing in-progress handoff for the same branch is
// resumed, a finished one returns its done state, and state belonging to a
// different generation is rejected.
func (mgr *Manager) BeginHandoff(ctx context.Context, sourceStore, branchStore object.Store, branchID string) (*HandoffState, error) {
	const maxAttempts = 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, etag, err := mgr.ReadBranchEntry(ctx, sourceStore, branchID)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			// No registration. A released marker for this branch means the
			// handoff completed but its final entry delete raced with a crash;
			// the duplicate call returns the original result.
			if rel, _ := mgr.ReadReleasedHandoff(ctx, branchStore); rel != nil && rel.BranchID == branchID {
				return rel, nil
			}
			return nil, ErrBranchNotRegistered
		}

		// Released tombstone: finish the cleanup and report done.
		if entry.Released() {
			if rel, _ := mgr.ReadReleasedHandoff(ctx, branchStore); rel != nil && rel.HandoffID == entry.HandoffID {
				if err := mgr.deleteBranchEntryIfHandoff(ctx, sourceStore, branchID, rel.HandoffID); err != nil {
					return nil, err
				}
				return rel, nil
			}
			// Tombstone without a matching released marker: recover by
			// rebuilding the active marker from the tombstone's generation.
			st, err := mgr.newPinnedState(ctx, branchStore, branchID, entry.AncestorCheckpointKey, entry.HandoffID)
			if err != nil {
				return nil, err
			}
			st.Phase = HandoffPhaseSolidified
			st.IndependentCheckpointKey = entry.IndependentCheckpointKey
			if _, err := mgr.saveActive(ctx, branchStore, st, ""); err != nil {
				if errors.Is(err, object.ErrPreconditionFailed) {
					continue
				}
				return nil, err
			}
			return st, nil
		}

		// Resume an existing handoff.
		if entry.HandoffID != "" {
			active, err := mgr.ReadActiveHandoff(ctx, branchStore)
			if err != nil {
				return nil, err
			}
			if active != nil {
				if active.BranchID != branchID || active.HandoffID != entry.HandoffID {
					return nil, fmt.Errorf("%w: entry=%q marker=%q", ErrHandoffConflict, entry.HandoffID, active.HandoffID)
				}
				return active, nil
			}
			// Marker missing. If the released marker already belongs to this
			// generation, the handoff is done (Complete cleans the entry).
			if rel, _ := mgr.ReadReleasedHandoff(ctx, branchStore); rel != nil && rel.HandoffID == entry.HandoffID {
				return rel, nil
			}
			// Crash between entry CAS and marker creation (or marker lost):
			// rebuild it under the SAME generation id, re-pinning the
			// current branch revision — same handoff, newer observation.
			st, err := mgr.newPinnedState(ctx, branchStore, branchID, entry.AncestorCheckpointKey, entry.HandoffID)
			if err != nil {
				return nil, err
			}
			if _, err := mgr.saveActive(ctx, branchStore, st, ""); err != nil {
				if errors.Is(err, object.ErrPreconditionFailed) {
					continue // peer created it concurrently; resolve next round
				}
				return nil, err
			}
			return st, nil
		}

		// Fresh handoff.
		st, err := mgr.newPinnedState(ctx, branchStore, branchID, entry.AncestorCheckpointKey, newHandoffID())
		if err != nil {
			return nil, err
		}
		if _, err := mgr.saveActive(ctx, branchStore, st, ""); err != nil {
			if errors.Is(err, object.ErrPreconditionFailed) {
				// Marker already exists. Only legitimate if it belongs to this
				// same branch with no owning entry — a leaked marker from an
				// aborted attempt. Adopt it only if ids can be reconciled
				// through the entry transition below; otherwise it's foreign.
				active, rerr := mgr.ReadActiveHandoff(ctx, branchStore)
				if rerr != nil || active == nil || active.BranchID != branchID {
					return nil, fmt.Errorf("checkpoint: begin handoff: existing marker is owned by another branch: %w", ErrHandoffConflict)
				}
				st = active
			} else {
				return nil, err
			}
		}

		// Fence the source entry into solidifying under its read ETag.
		updated := *entry
		updated.State = BranchStateSolidifying
		updated.HandoffID = st.HandoffID
		updated.UpdatedAt = time.Now().UTC()
		if err := mgr.writeBranchEntry(ctx, sourceStore, branchID, &updated, etag); err != nil {
			if errors.Is(err, object.ErrPreconditionFailed) {
				continue // entry moved; re-resolve on the next attempt
			}
			return nil, err
		}
		return st, nil
	}
	return nil, fmt.Errorf("checkpoint: begin handoff for %q: contention did not settle: %w", branchID, ErrHandoffConflict)
}

// newPinnedState reads the branch manifest/latest and pins its checkpoint.
func (mgr *Manager) newPinnedState(ctx context.Context, branchStore object.Store, branchID, ancestorKey, handoffID string) (*HandoffState, error) {
	manifest, err := mgr.ReadManifest(ctx, branchStore)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read branch manifest: %w", err)
	}
	if manifest == nil || manifest.CheckpointKey == "" {
		return nil, errors.New("checkpoint: branch store has no checkpoint to decommission")
	}
	now := time.Now().UTC()
	return &HandoffState{
		SchemaVersion:         handoffSchemaVersion,
		HandoffID:             handoffID,
		BranchID:              branchID,
		AncestorCheckpointKey: ancestorKey,
		PinnedCheckpointKey:   manifest.CheckpointKey,
		PinnedTerm:            manifest.Term,
		PinnedRevision:        manifest.Revision,
		Phase:                 HandoffPhaseSolidifying,
		CreatedAt:             now,
		UpdatedAt:             now,
	}, nil
}

// ── Solidify ──────────────────────────────────────────────────────────────────────

// SolidifyHandoff makes the branch manifest head checkpoint independent of the
// ancestor store: copies the ancestor SSTs it references into the branch
// store, rewrites the head index with all SSTs local, verifies the result,
// and flips the marker to "solidified". Idempotent and resumable.
func (mgr *Manager) SolidifyHandoff(ctx context.Context, sourceStore, branchStore object.Store, st *HandoffState) (*HandoffState, error) {
	if st.Phase == HandoffPhaseDone {
		return st, nil
	}
	copied := make(map[string]struct{}, len(st.CopiedSSTs))
	for _, k := range st.CopiedSSTs {
		copied[k] = struct{}{}
	}

	for round := 0; ; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Re-read the marker every round so concurrent progress (another
		// retry, or the live branch node promoting on its own) is adopted
		// rather than fought.
		active, etag, err := mgr.readHandoff(ctx, branchStore, HandoffActiveKey)
		if errors.Is(err, object.ErrNotFound) {
			if rel, _ := mgr.ReadReleasedHandoff(ctx, branchStore); rel != nil && rel.HandoffID == st.HandoffID {
				return rel, nil
			}
			return st, fmt.Errorf("checkpoint: handoff %q marker disappeared", st.HandoffID)
		}
		if err != nil {
			return st, err
		}
		if active.HandoffID != st.HandoffID || active.BranchID != st.BranchID {
			return nil, fmt.Errorf("%w: resume=%q marker=%q", ErrHandoffConflict, st.HandoffID, active.HandoffID)
		}
		st = active
		for _, k := range st.CopiedSSTs {
			copied[k] = struct{}{}
		}

		// If the node (or a previous retry) already claims solidification,
		// verify it before accepting; a failed verification just repairs.
		if st.Phase == HandoffPhaseSolidified {
			if err := mgr.verifyIndependent(ctx, branchStore, st.IndependentCheckpointKey, st.PinnedTerm, st.PinnedRevision); err == nil {
				return st, nil
			}
			st.Phase = HandoffPhaseSolidifying
		}

		manifest, err := mgr.ReadManifest(ctx, branchStore)
		if err != nil {
			return st, fmt.Errorf("checkpoint: read branch manifest: %w", err)
		}
		if manifest == nil {
			return st, errors.New("checkpoint: branch store has no manifest")
		}
		if !checkpointAtLeast(manifest.Term, manifest.Revision, st.PinnedTerm, st.PinnedRevision) {
			return nil, fmt.Errorf("%w: branch manifest term=%d rev=%d precedes pinned term=%d rev=%d",
				ErrHandoffConflict, manifest.Term, manifest.Revision, st.PinnedTerm, st.PinnedRevision)
		}
		headKey := manifest.CheckpointKey

		var idx CheckpointIndex
		idxETag, err := getJSONETag(ctx, branchStore, headKey, &idx)
		if err != nil {
			return st, fmt.Errorf("checkpoint: read branch head index %q: %w", headKey, err)
		}

		// Copy any ancestor SST the head still references that this
		// handoff has not copied before. Content-addressed keys make the copy
		// idempotent across retries and across the live node promoting.
		markerMoved := false
		for _, key := range idx.AncestorSSTFiles {
			if _, ok := copied[key]; ok {
				continue
			}
			if err := copySSTVerified(ctx, sourceStore, branchStore, key); err != nil {
				return st, err
			}
			copied[key] = struct{}{}
			st.CopiedSSTs = append(st.CopiedSSTs, key)
			sort.Strings(st.CopiedSSTs)
			etag, err = mgr.saveActive(ctx, branchStore, st, etag)
			if err != nil {
				if errors.Is(err, object.ErrPreconditionFailed) {
					markerMoved = true
					break // restart the round on fresh state
				}
				return st, err
			}
		}
		if markerMoved {
			continue
		}

		// Rewrite the head index so every referenced SST is local. The
		// ETag fence makes a concurrent checkpoint write at the same revision
		// lose this PUT; the next round then adopts the node's index —
		// which is itself independent once the node saw the marker.
		if len(idx.AncestorSSTFiles) > 0 {
			merged := mergeSSTKeys(idx.SSTFiles, idx.AncestorSSTFiles)
			idx.SSTFiles = merged
			idx.AncestorSSTFiles = nil
			if err := putJSONCAS(ctx, branchStore, headKey, &idx, idxETag); err != nil {
				if errors.Is(err, object.ErrPreconditionFailed) {
					continue
				}
				return st, fmt.Errorf("checkpoint: publish independent index %q: %w", headKey, err)
			}
		}

		if err := mgr.verifyIndependent(ctx, branchStore, headKey, st.PinnedTerm, st.PinnedRevision); err != nil {
			// A raced branch checkpoint can have rewritten the index after our
			// CAS (its upload was already in flight when the marker went
			// up). Retry: the node promotes before every later checkpoint.
			if errors.Is(err, ErrNotIndependent) {
				continue
			}
			return st, err
		}

		st.Phase = HandoffPhaseSolidified
		st.IndependentCheckpointKey = headKey
		if _, err := mgr.saveActive(ctx, branchStore, st, etag); err != nil {
			if errors.Is(err, object.ErrPreconditionFailed) {
				continue
			}
			return st, err
		}
		return st, nil
	}
}

// parseCheckpointKey parses "checkpoint/{term}/{rev}/manifest.json".
func parseCheckpointKey(key string) (term uint64, revision int64, err error) {
	const prefix = "checkpoint/"
	const suffix = "/manifest.json"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
		return 0, 0, fmt.Errorf("checkpoint: %q is not a checkpoint index key", key)
	}
	parts := strings.SplitN(strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix), "/", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("checkpoint: %q is not a checkpoint index key", key)
	}
	t, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("checkpoint: bad term in %q", key)
	}
	r, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("checkpoint: bad revision in %q", key)
	}
	return t, r, nil
}

func mergeSSTKeys(local, ancestor []string) []string {
	set := make(map[string]struct{}, len(local)+len(ancestor))
	for _, k := range local {
		set[k] = struct{}{}
	}
	for _, k := range ancestor {
		set[k] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// copySSTVerified copies key from source to branch at the same
// content-addressed key, skipping an identical key already present, and verifies
// the streamed content matches the SHA-256 prefix encoded in the key.
func copySSTVerified(ctx context.Context, sourceStore, branchStore object.Store, key string) error {
	if !strings.HasPrefix(key, "sst/") {
		return fmt.Errorf("checkpoint: unexpected ancestor sst key %q", key)
	}
	hashPrefix := strings.SplitN(strings.TrimPrefix(key, "sst/"), "/", 2)[0]

	if rc, err := branchStore.Get(ctx, key); err == nil {
		_ = rc.Close()
		return nil // already copied (content-addressed key ⇒ same bytes)
	} else if !errors.Is(err, object.ErrNotFound) {
		return fmt.Errorf("checkpoint: probe branch sst %q: %w", key, err)
	}

	rc, err := sourceStore.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("checkpoint: read ancestor sst %q: %w", key, err)
	}
	defer rc.Close()

	tmp := new(bytes.Buffer)
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), rc); err != nil {
		return fmt.Errorf("checkpoint: download ancestor sst %q: %w", key, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)[:8]); got != hashPrefix {
		return fmt.Errorf("checkpoint: ancestor sst %q content hash mismatch: want %s got %s", key, hashPrefix, got)
	}
	if err := branchStore.Put(ctx, key, bytes.NewReader(tmp.Bytes())); err != nil {
		return fmt.Errorf("checkpoint: copy sst %q to branch store: %w", key, err)
	}
	return nil
}

// verifyIndependent asserts that the checkpoint index at key references no
// ancestor SSTs, every referenced object exists in the branch store, and its
// revision is at least the pinned one.
func (mgr *Manager) verifyIndependent(ctx context.Context, branchStore object.Store, key string, minTerm uint64, minRev int64) error {
	idx, err := mgr.ReadCheckpointIndex(ctx, branchStore, key)
	if err != nil {
		return fmt.Errorf("%w: read index %q: %v", ErrNotIndependent, key, err)
	}
	if len(idx.AncestorSSTFiles) != 0 {
		return fmt.Errorf("%w: index %q still references %d ancestor SST(s)", ErrNotIndependent, key, len(idx.AncestorSSTFiles))
	}
	if !checkpointAtLeast(idx.Term, idx.Revision, minTerm, minRev) {
		return fmt.Errorf("%w: index %q term=%d rev=%d precedes pin term=%d rev=%d",
			ErrNotIndependent, key, idx.Term, idx.Revision, minTerm, minRev)
	}
	for _, k := range idx.SSTFiles {
		rc, err := branchStore.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("%w: local sst %q: %v", ErrNotIndependent, k, err)
		}
		_ = rc.Close()
	}
	prefix := strings.TrimSuffix(key, "manifest.json")
	for _, name := range idx.PebbleMeta {
		rc, err := branchStore.Get(ctx, prefix+name)
		if err != nil {
			return fmt.Errorf("%w: pebble meta %q: %v", ErrNotIndependent, prefix+name, err)
		}
		_ = rc.Close()
	}
	return nil
}

// MarkHandoffSolidified lets a live branch node announce that the checkpoint
// it just wrote at checkpointKey is independent (it promoted all inherited
// SSTs before writing it). Never moves the marker backwards: an announcement
// at an older checkpoint than one already recorded is ignored. A missing
// marker means no handoff is active; that is not an error.
func (mgr *Manager) MarkHandoffSolidified(ctx context.Context, branchStore object.Store, checkpointKey string, term uint64, revision int64) error {
	for attempt := 0; attempt < 5; attempt++ {
		st, etag, err := mgr.readHandoff(ctx, branchStore, HandoffActiveKey)
		if errors.Is(err, object.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("checkpoint: read handoff marker: %w", err)
		}
		if st.Phase == HandoffPhaseSolidified {
			if t, r, perr := parseCheckpointKey(st.IndependentCheckpointKey); perr == nil &&
				checkpointAtLeast(t, r, term, revision) {
				return nil // recorded checkpoint is at least as new
			}
		}
		st.Phase = HandoffPhaseSolidified
		st.IndependentCheckpointKey = checkpointKey
		if err := putJSONCAS(ctx, branchStore, HandoffActiveKey, st, etag); err == nil {
			return nil
		} else if !errors.Is(err, object.ErrPreconditionFailed) {
			return err
		}
	}
	return fmt.Errorf("checkpoint: mark handoff solidified: %w", object.ErrPreconditionFailed)
}

// ── Complete ────────────────────────────────────────────────────────────────────

// CompleteHandoff verifies the branch manifest head is independent and releases
// the source registration behind a generation-fenced tombstone. Idempotent.
func (mgr *Manager) CompleteHandoff(ctx context.Context, sourceStore, branchStore object.Store, branchID string) (*HandoffState, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, etag, err := mgr.ReadBranchEntry(ctx, sourceStore, branchID)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			if rel, _ := mgr.ReadReleasedHandoff(ctx, branchStore); rel != nil && rel.BranchID == branchID {
				return rel, nil
			}
			return nil, ErrBranchNotRegistered
		}

		// Resolve the branch-side state. Active wins; a done marker means a
		// previous attempt crashed between releasing and cleanup.
		st, _ := mgr.ReadActiveHandoff(ctx, branchStore)
		if st == nil {
			if rel, _ := mgr.ReadReleasedHandoff(ctx, branchStore); rel != nil {
				st = rel
			}
		}
		if st == nil {
			return nil, fmt.Errorf("checkpoint: branch %q entry is %q without a handoff marker", branchID, entry.State)
		}
		if st.BranchID != branchID || entry.HandoffID != st.HandoffID {
			return nil, fmt.Errorf("%w: entry=%q marker=%q", ErrHandoffConflict, entry.HandoffID, st.HandoffID)
		}

		// The gate: the manifest head itself must be an independent
		// checkpoint covering at least the pin, and the recorded proof must
		// verify as well. Until both hold, protection stays in place.
		manifest, err := mgr.ReadManifest(ctx, branchStore)
		if err != nil {
			return nil, fmt.Errorf("checkpoint: read branch manifest: %w", err)
		}
		if manifest == nil {
			return nil, errors.New("checkpoint: branch store has no manifest")
		}
		headKey := manifest.CheckpointKey
		if err := mgr.verifyIndependent(ctx, branchStore, headKey, st.PinnedTerm, st.PinnedRevision); err != nil {
			return nil, err
		}
		if st.IndependentCheckpointKey != "" {
			if err := mgr.verifyIndependent(ctx, branchStore, st.IndependentCheckpointKey, st.PinnedTerm, st.PinnedRevision); err != nil {
				return nil, err
			}
		}

		if !entry.Released() {
			tomb := *entry
			tomb.State = BranchStateReleased
			tomb.HandoffID = st.HandoffID
			tomb.IndependentCheckpointKey = headKey
			tomb.UpdatedAt = time.Now().UTC()
			if err := mgr.writeBranchEntry(ctx, sourceStore, branchID, &tomb, etag); err != nil {
				if errors.Is(err, object.ErrPreconditionFailed) {
					continue
				}
				return nil, err
			}
			entry = &tomb
		}

		// Publish the permanent done marker and retire the active one.
		done := *st
		done.Phase = HandoffPhaseDone
		done.IndependentCheckpointKey = headKey
		done.UpdatedAt = time.Now().UTC()
		if err := putJSONCAS(ctx, branchStore, HandoffReleasedKey, &done, ""); err != nil &&
			!errors.Is(err, object.ErrPreconditionFailed) {
			return nil, fmt.Errorf("checkpoint: write released marker: %w", err)
		}
		_ = branchStore.Delete(ctx, HandoffActiveKey)

		if err := mgr.deleteBranchEntryIfHandoff(ctx, sourceStore, branchID, st.HandoffID); err != nil {
			return nil, err
		}
		return &done, nil
	}
}

// RunHandoff drives one decommission from begin through release. Safe to
// retry after any interruption: it always resumes the existing handoff.
func (mgr *Manager) RunHandoff(ctx context.Context, sourceStore, branchStore object.Store, branchID string) (*HandoffState, error) {
	st, err := mgr.BeginHandoff(ctx, sourceStore, branchStore, branchID)
	if err != nil {
		return nil, err
	}
	if st.Phase != HandoffPhaseDone {
		st, err = mgr.SolidifyHandoff(ctx, sourceStore, branchStore, st)
		if err != nil {
			return st, err
		}
		st, err = mgr.CompleteHandoff(ctx, sourceStore, branchStore, branchID)
		if err != nil {
			return st, err
		}
	} else {
		// Done marker found at begin; make sure no tombstone lingers.
		if err := mgr.deleteBranchEntryIfHandoff(ctx, sourceStore, branchID, st.HandoffID); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// HandoffStatus returns a queryable snapshot without mutating anything.
func (mgr *Manager) HandoffStatus(ctx context.Context, sourceStore, branchStore object.Store, branchID string) (*HandoffStatus, error) {
	status := &HandoffStatus{BranchID: branchID, SourceEntryState: "not-registered"}

	entry, _, err := mgr.ReadBranchEntry(ctx, sourceStore, branchID)
	if err != nil {
		return nil, err
	}
	if entry != nil {
		switch {
		case entry.Released():
			status.SourceEntryState = "released"
		case entry.State == BranchStateSolidifying:
			status.SourceEntryState = "solidifying"
		default:
			status.SourceEntryState = "active"
		}
		status.HandoffID = entry.HandoffID
		status.AncestorCheckpointKey = entry.AncestorCheckpointKey
		status.IndependentCheckpointKey = entry.IndependentCheckpointKey
	}

	if branchStore != nil {
		st, _ := mgr.ReadActiveHandoff(ctx, branchStore)
		if st == nil {
			st, _ = mgr.ReadReleasedHandoff(ctx, branchStore)
		}
		if st != nil && st.BranchID == branchID {
			status.Phase = st.Phase
			status.HandoffID = st.HandoffID
			status.PinnedCheckpointKey = st.PinnedCheckpointKey
			if status.AncestorCheckpointKey == "" {
				status.AncestorCheckpointKey = st.AncestorCheckpointKey
			}
			status.CopiedSSTs = len(st.CopiedSSTs)
			if status.IndependentCheckpointKey == "" {
				status.IndependentCheckpointKey = st.IndependentCheckpointKey
			}
		}
		if manifest, err := mgr.ReadManifest(ctx, branchStore); err == nil && manifest != nil {
			if idx, err := mgr.ReadCheckpointIndex(ctx, branchStore, manifest.CheckpointKey); err == nil {
				status.ReferencedAncestorSSTs = len(idx.AncestorSSTFiles)
			}
		}
	}
	return status, nil
}
