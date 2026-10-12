// Package gc runs resumable, generation-fenced object-storage garbage
// collection.
//
// A GC run is planned once and then executed against the pinned plan, so a
// timeout, client cancellation, process exit or leader change can never widen
// the deletion set on retry:
//
//   - The manifest snapshot, the retention set (newest keep checkpoints, the
//     manifest checkpoint and active branch pins), the protected SSTs, the
//     follower-safe WAL horizon and the leadership generation are fixed when
//     the run starts.
//   - Checkpoint directories, candidate orphan SSTs and WAL segments are
//     journaled item by item (pending → deleting → deleted/retained). The
//     journal lives in the object store itself, so it survives process
//     restarts and is shared by the automatic leader GC and the CLI.
//   - A delete whose outcome the store did not confirm is reconciled against
//     the object's actual existence before anything is retried; retries only
//     touch items the pinned plan still lists as unconfirmed.
//   - The three deletion phases advance in reference order — checkpoints,
//     then the SSTs only the deleted checkpoints referenced, then WAL — and
//     every batch is re-checked against the current manifest, branch
//     registrations, database-held SSTs and follower horizon immediately
//     before it is sent.
//   - Only an execution that still owns the pinned leadership generation may
//     continue or write the journal; a superseded execution stops without
//     touching the newer generation's state.
package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// Object keys of the GC journal. All other code lists only "sst/", "wal/",
// "checkpoint/", "branches/" or reads "manifest/latest", so these never enter
// another subsystem's listings.
const (
	// ActiveKey holds the run that is currently in progress or failed before
	// completion. At most one active run exists per store; new invocations
	// resume it instead of scanning a fresh plan.
	ActiveKey = "gc/active.json"
	// LatestKey holds the most recent terminal result (success or failure),
	// which is how operators query the outcome of past runs.
	LatestKey = "gc/latest.json"
	// ResultKeyPrefix stores the full terminal record of every run, keyed by
	// run ID, including runs a newer generation superseded.
	ResultKeyPrefix = "gc/results/"

	stateFormatVersion = 1
)

// Kind identifies who started a run.
type Kind string

const (
	// KindAuto is a run the serving leader started from its checkpoint loop.
	KindAuto Kind = "auto"
	// KindCLI is a run started by the "t4 gc" command.
	KindCLI Kind = "cli"
)

// Status is the lifecycle state of a run.
type Status string

const (
	// StatusRunning is a run that may still delete objects.
	StatusRunning Status = "running"
	// StatusSucceeded is a run whose items are all confirmed deleted or
	// retained.
	StatusSucceeded Status = "succeeded"
	// StatusFailed is a run that stopped on an error or cancellation with
	// items unconfirmed; the next invocation resumes it against the same
	// plan. Its result is already queryable.
	StatusFailed Status = "failed"
	// StatusSuperseded is a run pinned to a leadership generation that lost
	// leadership; it is never resumed.
	StatusSuperseded Status = "superseded"
)

// Phase is the per-item deletion state.
type Phase string

const (
	// PhasePending items are planned but not yet confirmed deleted.
	PhasePending Phase = "pending"
	// PhaseDeleting was journaled immediately before a delete request; a
	// crash or timeout in this state is reconciled by checking object facts.
	PhaseDeleting Phase = "deleting"
	// PhaseUncertain is reserved for deletes the store answered ambiguously;
	// reconciliation moves the item either back to pending or to deleted.
	PhaseUncertain Phase = "uncertain"
	// PhaseDeleted items were confirmed gone, either by the store response or
	// by an existence check. They are never retried.
	PhaseDeleted Phase = "deleted"
	// PhaseRetained items were in the pinned plan but turned out to still be
	// needed (new branch registration, new checkpoint reference, follower
	// horizon regressed, ...); they are never deleted by this run.
	PhaseRetained Phase = "retained"
)

// phaseRank orders phases so concurrent journal updates never regress an
// item's progress. Terminal phases tie.
func phaseRank(p Phase) int {
	switch p {
	case PhaseDeleting:
		return 1
	case PhaseUncertain:
		return 2
	case PhaseDeleted, PhaseRetained:
		return 3
	default:
		return 0
	}
}

// isTerminalPhase reports whether the item state is final for this run.
func isTerminalPhase(p Phase) bool {
	return p == PhaseDeleted || p == PhaseRetained
}

// Item is one unit of deletion journaled by a run.
type Item struct {
	// Key is the logical item: a checkpoint index key (".../manifest.json"),
	// an SST object key or a WAL segment key.
	Key string `json:"key"`
	// Objects are the concrete object-storage keys deleted for this item.
	// For checkpoint directories it holds every object under the checkpoint
	// prefix, enumerated immediately before deletion; SST and WAL items hold
	// Key itself.
	Objects []string `json:"objects,omitempty"`
	// Sources records, for an orphan-SST candidate, the expiring checkpoint
	// index keys that referenced it in the pinned plan. The SST is deletable
	// only after every source checkpoint is confirmed deleted.
	Sources []string `json:"sources,omitempty"`
	// Phase is the item's current journal state.
	Phase Phase `json:"phase"`
	// Attempts counts delete requests sent for the item.
	Attempts int `json:"attempts,omitempty"`
	// LastError is the last store error recorded against the item.
	LastError string `json:"last_error,omitempty"`
	// UpdatedNano is when the item last changed state (Unix ns).
	UpdatedNano int64 `json:"updated_nano,omitempty"`
}

// ManifestSnapshot pins the manifest/latest content the run started with.
type ManifestSnapshot struct {
	CheckpointKey string `json:"checkpoint_key"`
	Revision      int64  `json:"revision"`
	LastSequence  int64  `json:"last_sequence"`
	Term          uint64 `json:"term"`
	LastWALKey    string `json:"last_wal_key,omitempty"`
}

// Run is the full journal record of one GC invocation: the pinned plan plus
// the per-item phase of all three phases.
type Run struct {
	FormatVersion int    `json:"format_version"`
	ID            string `json:"id"`
	Kind          Kind   `json:"kind"`
	// Term is the leadership generation (raft-style term) pinned at start for
	// automatic runs; 0 for CLI runs.
	Term uint64 `json:"term,omitempty"`
	Keep int    `json:"keep"`

	CreatedNano  int64  `json:"created_nano"`
	UpdatedNano  int64  `json:"updated_nano"`
	FinishedNano int64  `json:"finished_nano,omitempty"`
	Status       Status `json:"status"`
	Error        string `json:"error,omitempty"`

	// Pinned inputs.
	// Manifest is nil when the store has no manifest yet (nothing to pin).
	Manifest *ManifestSnapshot `json:"manifest,omitempty"`
	// PinnedCheckpoints are the checkpoint index keys retained by the plan:
	// the newest Keep, the one manifest/latest names, and active branch pins.
	PinnedCheckpoints []string `json:"pinned_checkpoints,omitempty"`
	// BranchPins pins branch ID → ancestor checkpoint key as read at start.
	BranchPins map[string]string `json:"branch_pins,omitempty"`
	// WALHorizonSeq is the follower-safe sequence horizon the WAL items were
	// selected against.
	WALHorizonSeq int64 `json:"wal_horizon_seq,omitempty"`

	// Items of the three phases, in reference order.
	Checkpoints []*Item `json:"checkpoints,omitempty"`
	SSTs        []*Item `json:"ssts,omitempty"`
	WALs        []*Item `json:"wals,omitempty"`
}

// Counts returns how many items of each phase were confirmed deleted or
// retained and how many are still unconfirmed.
type Counts struct {
	CheckpointsDeleted int
	SSTsDeleted        int
	WALsDeleted        int
	Retained           int
	Pending            int
}

func countItems(items []*Item) (deleted, retained, pending int) {
	for _, it := range items {
		switch it.Phase {
		case PhaseDeleted:
			deleted++
		case PhaseRetained:
			retained++
		default:
			pending++
		}
	}
	return
}

// Counts summarizes the run's item phases.
func (r *Run) Counts() Counts {
	var c Counts
	var rd, sd, wd int
	rd, cr, cp := countItems(r.Checkpoints)
	sd, sr, sp := countItems(r.SSTs)
	wd, wr, wp := countItems(r.WALs)
	c.CheckpointsDeleted, c.SSTsDeleted, c.WALsDeleted = rd, sd, wd
	c.Retained = cr + sr + wr
	c.Pending = cp + sp + wp
	return c
}

func (r *Run) empty() bool {
	return len(r.Checkpoints) == 0 && len(r.SSTs) == 0 && len(r.WALs) == 0
}

// resultKey is the per-run terminal record key.
func (r *Run) resultKey() string {
	return ResultKeyPrefix + r.ID + ".json"
}

// newRunID builds a collision-resistant run ID that sorts chronologically.
func newRunID(kind Kind) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%016x-%s", kind, time.Now().UnixNano(), hex.EncodeToString(b[:])), nil
}

// ── persistence ────────────────────────────────────────────────────────────

// ErrConcurrentWrite is returned when the active-run journal changed between
// read and write (another invocation is executing the same run). Callers
// reload and merge.
var ErrConcurrentWrite = errors.New("gc: active run changed concurrently")

// ErrNotFoundActive is returned when no active run exists.
var ErrNotFoundActive = errors.New("gc: no active run")

func encodeRun(r *Run) ([]byte, error) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return b, nil
}

func decodeRun(b []byte) (*Run, error) {
	var r Run
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("gc: decode run: %w", err)
	}
	if r.FormatVersion > stateFormatVersion {
		return nil, fmt.Errorf("gc: run format_version=%d newer than supported %d", r.FormatVersion, stateFormatVersion)
	}
	return &r, nil
}

// loadRun reads and decodes the run object at key. Missing objects return
// nil, "", nil.
func loadRun(ctx context.Context, store object.Store, key string) (*Run, string, error) {
	if cs, ok := store.(object.ConditionalStore); ok {
		cur, err := cs.GetETag(ctx, key)
		if errors.Is(err, object.ErrNotFound) {
			return nil, "", nil
		}
		if err != nil {
			return nil, "", fmt.Errorf("gc: load %q: %w", key, err)
		}
		b, err := io.ReadAll(cur.Body)
		_ = cur.Body.Close()
		if err != nil {
			return nil, "", err
		}
		r, err := decodeRun(b)
		if err != nil {
			return nil, "", err
		}
		return r, cur.ETag, nil
	}
	rc, err := store.Get(ctx, key)
	if errors.Is(err, object.ErrNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("gc: load %q: %w", key, err)
	}
	b, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, "", err
	}
	r, err := decodeRun(b)
	if err != nil {
		return nil, "", err
	}
	return r, "", nil
}

// createActive writes run to ActiveKey only while it is absent. On stores
// without conditional writes it re-reads and refuses to overwrite a foreign
// run.
func createActive(ctx context.Context, store object.Store, run *Run) error {
	b, err := encodeRun(run)
	if err != nil {
		return err
	}
	if cs, ok := store.(object.ConditionalStore); ok {
		err := cs.PutIfAbsent(ctx, ActiveKey, bytes.NewReader(b))
		if errors.Is(err, object.ErrPreconditionFailed) {
			return ErrConcurrentWrite
		}
		return err
	}
	cur, _, err := loadRun(ctx, store, ActiveKey)
	if err != nil {
		return err
	}
	if cur != nil {
		return ErrConcurrentWrite
	}
	return store.Put(ctx, ActiveKey, bytes.NewReader(b))
}

// saveActive writes run to ActiveKey, requiring the journal to still carry
// etag (empty etag means a store without conditional writes, in which case
// the run ID must still match). A foreign or changed journal yields
// ErrConcurrentWrite without overwriting it.
func saveActive(ctx context.Context, store object.Store, run *Run, etag string) (string, error) {
	run.UpdatedNano = time.Now().UnixNano()
	b, err := encodeRun(run)
	if err != nil {
		return "", err
	}
	if cs, ok := store.(object.ConditionalStore); ok {
		if etag == "" {
			// Should not happen for a loaded active run on a conditional
			// store, but keep a safe fallback.
			if err := store.Put(ctx, ActiveKey, bytes.NewReader(b)); err != nil {
				return "", err
			}
		} else if err := cs.PutIfMatch(ctx, ActiveKey, bytes.NewReader(b), etag); err != nil {
			if errors.Is(err, object.ErrPreconditionFailed) {
				return "", ErrConcurrentWrite
			}
			return "", err
		}
		// The write changed the content and therefore the content-hash
		// etag; re-read it so the next CAS compares the current value
		// instead of pointlessly losing the race once.
		cur, err := cs.GetETag(ctx, ActiveKey)
		if err != nil {
			return "", err
		}
		_ = cur.Body.Close()
		return cur.ETag, nil
	}
	cur, _, err := loadRun(ctx, store, ActiveKey)
	if err != nil {
		return "", err
	}
	if cur == nil || cur.ID != run.ID {
		return "", ErrConcurrentWrite
	}
	if err := store.Put(ctx, ActiveKey, bytes.NewReader(b)); err != nil {
		return "", err
	}
	return "", nil
}

// removeActive deletes the active journal only while it still belongs to run.
// A journal another generation already replaced is left untouched.
func removeActive(ctx context.Context, store object.Store, run *Run) error {
	cur, _, err := loadRun(ctx, store, ActiveKey)
	if err != nil {
		return err
	}
	if cur == nil || cur.ID != run.ID {
		// Already replaced by a newer run: never touch it.
		return nil
	}
	return store.Delete(ctx, ActiveKey)
}

// writeResult stores the terminal run under its per-run key.
func writeResult(ctx context.Context, store object.Store, run *Run) error {
	b, err := encodeRun(run)
	if err != nil {
		return err
	}
	return store.Put(ctx, run.resultKey(), bytes.NewReader(b))
}

// publishLatest points LatestKey at run unless LatestKey already carries a
// result that is newer than run, so a late old-generation writer cannot
// clobber a newer generation's outcome.
func publishLatest(ctx context.Context, store object.Store, run *Run) error {
	cur, _, err := loadRun(ctx, store, LatestKey)
	if err != nil {
		return err
	}
	if cur != nil && latestIsNewer(cur, run) {
		return nil
	}
	b, err := encodeRun(run)
	if err != nil {
		return err
	}
	// Best effort on stores without conditional writes: the newer-wins check
	// above plus per-run result keys are the protection that matters.
	return store.Put(ctx, LatestKey, bytes.NewReader(b))
}

// latestIsNewer reports whether cur should stay the latest pointer over run.
// A different run ranks by generation, then by finish time, then by ID.
func latestIsNewer(cur, run *Run) bool {
	if cur.ID == run.ID {
		return true
	}
	if cur.Term != run.Term {
		return cur.Term > run.Term
	}
	if cur.FinishedNano != run.FinishedNano {
		return cur.FinishedNano > run.FinishedNano
	}
	return cur.ID > run.ID
}

// mergeProgress folds another writer's journal update into run without
// regressing item phases. It returns false when the other journal is a
// different run — the caller must stop in that case.
func mergeProgress(base, other *Run) bool {
	if base.ID != other.ID {
		return false
	}
	mergeItems := func(dst, src []*Item) []*Item {
		byKey := make(map[string]*Item, len(dst))
		for _, it := range dst {
			byKey[it.Key] = it
		}
		for _, so := range src {
			do, ok := byKey[so.Key]
			if !ok {
				// Newly enumerated item the other writer discovered; adopt it.
				dst = append(dst, so)
				byKey[so.Key] = so
				continue
			}
			// Terminal states are sticky. Otherwise the newer observation
			// wins (a batch reconciliation that moved deleting → pending must
			// not be regressed by a stale pre-batch journal), with phase rank
			// breaking equal timestamps.
			if !isTerminalPhase(do.Phase) && (isTerminalPhase(so.Phase) ||
				so.UpdatedNano > do.UpdatedNano ||
				(so.UpdatedNano == do.UpdatedNano && phaseRank(so.Phase) > phaseRank(do.Phase))) {
				do.Phase = so.Phase
			}
			if so.Attempts > do.Attempts {
				do.Attempts = so.Attempts
			}
			if so.LastError != "" {
				do.LastError = so.LastError
			}
			if len(so.Objects) > len(do.Objects) {
				do.Objects = so.Objects
			}
			if so.UpdatedNano > do.UpdatedNano {
				do.UpdatedNano = so.UpdatedNano
			}
		}
		sort.Slice(dst, func(i, j int) bool { return dst[i].Key < dst[j].Key })
		return dst
	}
	base.Checkpoints = mergeItems(base.Checkpoints, other.Checkpoints)
	base.SSTs = mergeItems(base.SSTs, other.SSTs)
	base.WALs = mergeItems(base.WALs, other.WALs)
	if other.Status == StatusSucceeded || other.Status == StatusSuperseded {
		base.Status = other.Status
	}
	return true
}
