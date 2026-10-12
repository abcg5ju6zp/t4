package gc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// Logger is the narrow leveled-logger surface the runner needs. logrus
// loggers satisfy it.
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...interface{}) {}
func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Warnf(string, ...interface{})  {}

// Options configures a Runner.
type Options struct {
	// Kind is the invocation kind recorded in the run.
	Kind Kind
	// Term is the leadership generation pinned for automatic runs. Ignored
	// for CLI runs.
	Term uint64
	// Keep is the number of newest checkpoints retained (existing default:
	// 2 for the leader, 3 for the CLI).
	Keep int

	// WALHorizon is evaluated at plan time and again immediately before the
	// WAL delete batch. It returns the sequence up to which WAL segments may
	// be removed — the checkpoint sequence, capped by the minimum connected
	// follower's applied sequence on a leader; ok=false skips WAL GC (CLI
	// with no manifest).
	WALHorizon func(ctx context.Context) (seq int64, ok bool, err error)
	// LiveSSTs reports SST keys the current database still holds, so the SST
	// phase never removes content a live table needs. May be nil.
	LiveSSTs func() map[string]struct{}
	// Fence reports that the pinned leadership generation is still valid.
	// Called before every journal write and delete batch; nil means no
	// leadership fence (CLI).
	Fence func() bool

	Log Logger
}

// Runner executes resumable, generation-fenced GC against one store.
type Runner struct {
	mgr   *checkpoint.Manager
	store object.Store
	opts  Options
	log   Logger

	run  *Run
	etag string
}

// New returns a Runner. mgr is used for the same reads GCCheckpoints uses;
// every mutation goes through store.
func New(mgr *checkpoint.Manager, store object.Store, opts Options) *Runner {
	log := opts.Log
	if log == nil {
		log = noopLogger{}
	}
	return &Runner{mgr: mgr, store: store, opts: opts, log: log}
}

// ErrFenced is returned when the pinned leadership generation is no longer
// valid (leadership lost, lease expired, term advanced). The caller must
// stop; the run stays in the store for the owning generation to finalize.
var ErrFenced = errors.New("gc: leadership generation no longer valid")

// Run resumes an in-progress run or starts a new one, then drives the three
// deletion phases in reference order. A nil run with nil error means the
// store had no GC candidates at all: behavior is identical to the old
// no-op path and nothing is journaled.
func (r *Runner) Run(ctx context.Context) (*Run, error) {
	if err := r.alive(ctx); err != nil {
		return nil, err
	}

	run, err := r.start(ctx)
	if err != nil {
		return nil, err
	}
	if run == nil {
		// Nothing to do in any phase: preserve the previous no-candidates
		// behavior (no journal objects, zero counts).
		return &Run{Status: StatusSucceeded}, nil
	}
	r.run = run

	if err := r.execute(ctx); err != nil {
		// Cancellation or a lost generation: never finalize or overwrite on a
		// generation we no longer own. The persisted deleting/pending phases
		// let the owning generation reconcile on the next invocation.
		return r.run, err
	}
	return r.run, nil
}

// start loads the active run and decides resume/supersede/create.
func (r *Runner) start(ctx context.Context) (*Run, error) {
	active, etag, err := loadRun(ctx, r.store, ActiveKey)
	if err != nil {
		return nil, err
	}

	if active != nil {
		switch active.Status {
		case StatusSucceeded, StatusSuperseded:
			// A terminal record at the active key is stale residue; move it
			// to results and clear the slot.
			r.archive(ctx, active)
			_ = removeActive(ctx, r.store, active)
			active = nil
		default:
			resume := true
			if r.opts.Kind == KindAuto && active.Kind == KindAuto && active.Term != r.opts.Term {
				if active.Term > r.opts.Term {
					// A stale execution must never touch a newer
					// generation's run or protection state.
					return nil, fmt.Errorf("%w: active run belongs to term %d", ErrFenced, active.Term)
				}
				// The active run belongs to an older leadership generation.
				// Finalize it as superseded before planning under the new
				// generation; its pinned protection must not be reused or
				// overwritten.
				r.log.Warnf("gc: superseding run %s from term %d (current term %d)", active.ID, active.Term, r.opts.Term)
				active.Status = StatusSuperseded
				active.Error = fmt.Sprintf("superseded by leadership generation %d", r.opts.Term)
				active.FinishedNano = time.Now().UnixNano()
				if err := writeResult(ctx, r.store, active); err != nil {
					return nil, fmt.Errorf("gc: archive superseded run: %w", err)
				}
				if err := publishLatest(ctx, r.store, active); err != nil {
					return nil, fmt.Errorf("gc: publish superseded run: %w", err)
				}
				if err := removeActive(ctx, r.store, active); err != nil {
					return nil, fmt.Errorf("gc: clear superseded run: %w", err)
				}
				active, resume = nil, false
			}
			if resume {
				if r.opts.Kind == KindAuto && !r.fenceValid() {
					return nil, ErrFenced
				}
				r.etag = etag
				if err := r.prepareResume(ctx, active); err != nil {
					return nil, err
				}
				return active, nil
			}
		}
	}

	plan, err := r.buildPlan(ctx)
	if err != nil {
		return nil, err
	}
	if plan.empty() {
		return nil, nil
	}
	if err := createActive(ctx, r.store, plan); err != nil {
		if errors.Is(err, ErrConcurrentWrite) {
			// Someone else created a run between our list and create: reload
			// and resume theirs instead of planning again.
			other, otherEtag, lerr := loadRun(ctx, r.store, ActiveKey)
			if lerr != nil {
				return nil, lerr
			}
			if other == nil {
				return nil, ErrConcurrentWrite
			}
			if r.opts.Kind == KindAuto && other.Kind == KindAuto && other.Term != r.opts.Term {
				return nil, ErrFenced
			}
			r.etag = otherEtag
			if err := r.prepareResume(ctx, other); err != nil {
				return nil, err
			}
			return other, nil
		}
		return nil, fmt.Errorf("gc: persist plan: %w", err)
	}
	r.log.Infof("gc: run %s planned: %d checkpoint(s), %d orphan sst(s), %d wal segment(s), term=%d",
		plan.ID, len(plan.Checkpoints), len(plan.SSTs), len(plan.WALs), plan.Term)
	// Reload to pick up the store-assigned etag used for later CAS writes.
	loaded, etag, err := loadRun(ctx, r.store, ActiveKey)
	if err != nil {
		return nil, err
	}
	r.etag = etag
	return loaded, nil
}

// archive best-effort copies a stale active run to its per-run result key.
func (r *Runner) archive(ctx context.Context, run *Run) {
	if err := writeResult(ctx, r.store, run); err != nil {
		r.log.Warnf("gc: archive run %s: %v", run.ID, err)
	}
}

// prepareResume turns a journaled run back into an executable one:
// uncertain/deleting items are reconciled against object facts first, and a
// failed run is reopened.
func (r *Runner) prepareResume(ctx context.Context, run *Run) error {
	r.run = run
	r.log.Infof("gc: resuming run %s (term=%d, status=%s)", run.ID, run.Term, run.Status)
	if err := r.reconcileInFlight(ctx, run.Checkpoints); err != nil {
		return err
	}
	if err := r.reconcileInFlight(ctx, run.SSTs); err != nil {
		return err
	}
	if err := r.reconcileInFlight(ctx, run.WALs); err != nil {
		return err
	}
	run.Status = StatusRunning
	run.Error = ""
	return r.save(ctx)
}

// reconcileInFlight checks objects of items left in deleting/uncertain
// states by a process that died or timed out waiting for a delete response:
// every object gone means the item is confirmed deleted; an object still
// present sends the item back to pending. The check uses a detached context
// when the caller's context is already cancelled (Ctrl-C, lease deadline).
func (r *Runner) reconcileInFlight(ctx context.Context, items []*Item) error {
	factCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		factCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}
	now := time.Now().UnixNano()
	for _, it := range items {
		if it.Phase != PhaseDeleting && it.Phase != PhaseUncertain {
			continue
		}
		if len(it.Objects) == 0 {
			it.Phase = PhasePending
			it.UpdatedNano = now
			continue
		}
		present, err := r.anyPresent(factCtx, it.Objects)
		if err != nil {
			// Cannot establish facts: leave the item unconfirmed for the
			// next invocation rather than guessing.
			it.Phase = PhaseUncertain
			it.LastError = err.Error()
			it.UpdatedNano = now
			continue
		}
		if present {
			it.Phase = PhasePending
			r.log.Warnf("gc: item %q delete was unconfirmed and object still exists; will retry", it.Key)
		} else {
			it.Phase = PhaseDeleted
			r.log.Infof("gc: item %q confirmed deleted after ambiguous response", it.Key)
		}
		it.UpdatedNano = now
	}
	return nil
}

// execute drives checkpoint → orphan SST → WAL phases. A phase whose items
// cannot all reach a terminal state stops the run; later phases never run.
func (r *Runner) execute(ctx context.Context) error {
	failed, err := r.runCheckpointPhase(ctx)
	if err != nil {
		return err
	}
	if failed {
		return r.finishFailed(ctx, "checkpoint phase left unconfirmed items")
	}
	failed, err = r.runSSTPhase(ctx)
	if err != nil {
		return err
	}
	if failed {
		return r.finishFailed(ctx, "orphan sst phase left unconfirmed items")
	}
	failed, err = r.runWALPhase(ctx)
	if err != nil {
		return err
	}
	if failed {
		return r.finishFailed(ctx, "wal phase left unconfirmed items")
	}
	return r.finishSucceeded(ctx)
}

// ── phase: checkpoint directories ──────────────────────────────────────────

func (r *Runner) runCheckpointPhase(ctx context.Context) (bool, error) {
	pins, err := r.currentPins(ctx)
	if err != nil {
		return false, err
	}
	phaseFailed := false
	for _, it := range r.run.Checkpoints {
		if it.Phase == PhaseDeleted || it.Phase == PhaseRetained {
			continue
		}
		if _, protected := pins[it.Key]; protected {
			// A branch registration or manifest change since planning now
			// needs this checkpoint; the pinned plan can no longer remove it.
			r.retain(it, "now pinned by manifest or active branch")
			if err := r.save(ctx); err != nil {
				return false, err
			}
			continue
		}

		// Enumerate the directory from the current store. A previous attempt
		// may already have removed part of it: only the remaining objects are
		// retried.
		objs, err := r.store.List(ctx, checkpointPrefix(it.Key))
		if err != nil {
			return false, fmt.Errorf("gc: list checkpoint dir %q: %w", it.Key, err)
		}
		if len(objs) == 0 {
			it.Phase = PhaseDeleted
			it.LastError = ""
			it.UpdatedNano = time.Now().UnixNano()
			if err := r.save(ctx); err != nil {
				return false, err
			}
			continue
		}
		it.Objects = objs
		present, err := r.deleteItem(ctx, it)
		if err != nil {
			return false, err
		}
		if present {
			phaseFailed = true
		}
	}
	return phaseFailed, nil
}

// ── phase: orphan SSTs ──────────────────────────────────────────────────────

func (r *Runner) runSSTPhase(ctx context.Context) (bool, error) {
	cpState := make(map[string]Phase, len(r.run.Checkpoints))
	for _, cp := range r.run.Checkpoints {
		cpState[cp.Key] = cp.Phase
	}

	referenced, err := r.currentReferencedSSTs(ctx)
	if err != nil {
		return false, err
	}
	if r.opts.LiveSSTs != nil {
		for k := range r.opts.LiveSSTs() {
			referenced[k] = struct{}{}
		}
	}

	var pending []*Item
	for _, it := range r.run.SSTs {
		if it.Phase == PhaseDeleted || it.Phase == PhaseRetained {
			continue
		}
		// Reference order: an SST is removable only when every expiring
		// checkpoint that referenced it is confirmed gone.
		allSourcesGone := true
		for _, src := range it.Sources {
			if cpState[src] != PhaseDeleted {
				allSourcesGone = false
				break
			}
		}
		if !allSourcesGone {
			r.retain(it, "source checkpoint not confirmed deleted")
			continue
		}
		if _, needed := referenced[it.Key]; needed {
			// A newer checkpoint, an active branch or the current database
			// references the same content-addressed SST.
			r.retain(it, "referenced by a live checkpoint or the current database")
			continue
		}
		pending = append(pending, it)
	}
	if len(pending) == 0 {
		return false, r.saveIfChanged(ctx)
	}

	if err := r.alive(ctx); err != nil {
		return false, err
	}
	now := time.Now().UnixNano()
	keys := make([]string, 0, len(pending))
	for _, it := range pending {
		it.Phase = PhaseDeleting
		it.Attempts++
		it.UpdatedNano = now
		keys = append(keys, it.Key)
	}
	if err := r.save(ctx); err != nil {
		return false, err
	}

	delErr := r.store.DeleteMany(ctx, keys)
	survivors, err := r.reconcileBatch(ctx, keys, delErr)
	if err != nil {
		return false, err
	}
	now = time.Now().UnixNano()
	phaseFailed := false
	for _, it := range pending {
		if survivors[it.Key] {
			it.Phase = PhasePending
			if delErr != nil {
				it.LastError = delErr.Error()
			}
			phaseFailed = true
		} else {
			it.Phase = PhaseDeleted
			it.LastError = ""
		}
		it.UpdatedNano = now
	}
	if err := r.save(ctx); err != nil {
		return false, err
	}
	return phaseFailed, nil
}

// ── phase: WAL segments ─────────────────────────────────────────────────────

func (r *Runner) runWALPhase(ctx context.Context) (bool, error) {
	var currentDeletable map[string]struct{}
	if r.opts.WALHorizon != nil {
		horizon, ok, err := r.opts.WALHorizon(ctx)
		if err != nil {
			return false, fmt.Errorf("gc: re-resolve wal horizon: %w", err)
		}
		if ok {
			keys, err := wal.DeletableSegments(ctx, r.store, horizon)
			if err != nil {
				return false, err
			}
			currentDeletable = make(map[string]struct{}, len(keys))
			for _, k := range keys {
				currentDeletable[k] = struct{}{}
			}
		}
	}

	var pending []*Item
	for _, it := range r.run.WALs {
		if it.Phase == PhaseDeleted || it.Phase == PhaseRetained {
			continue
		}
		// The pinned list bounds what can ever be deleted; the current
		// horizon (follower watermarks may have regressed) bounds this
		// attempt. Items that drop out move to retained, never pending.
		if currentDeletable == nil {
			r.retain(it, "wal gc no longer available")
			continue
		}
		if _, stillDeletable := currentDeletable[it.Key]; !stillDeletable {
			r.retain(it, "outside current follower-safe wal horizon")
			continue
		}
		pending = append(pending, it)
	}
	if len(pending) == 0 {
		return false, r.saveIfChanged(ctx)
	}

	if err := r.alive(ctx); err != nil {
		return false, err
	}
	now := time.Now().UnixNano()
	keys := make([]string, 0, len(pending))
	for _, it := range pending {
		it.Phase = PhaseDeleting
		it.Attempts++
		it.UpdatedNano = now
		keys = append(keys, it.Key)
	}
	if err := r.save(ctx); err != nil {
		return false, err
	}

	delErr := r.store.DeleteMany(ctx, keys)
	survivors, err := r.reconcileBatch(ctx, keys, delErr)
	if err != nil {
		return false, err
	}
	now = time.Now().UnixNano()
	phaseFailed := false
	for _, it := range pending {
		if survivors[it.Key] {
			it.Phase = PhasePending
			if delErr != nil {
				it.LastError = delErr.Error()
			}
			phaseFailed = true
		} else {
			it.Phase = PhaseDeleted
			it.LastError = ""
		}
		it.UpdatedNano = now
	}
	if err := r.save(ctx); err != nil {
		return false, err
	}
	return phaseFailed, nil
}

// ── shared delete/reconcile mechanics ───────────────────────────────────────

// deleteItem journals the item as deleting, sends its object batch and
// reconciles the response. It reports whether any object remains present.
func (r *Runner) deleteItem(ctx context.Context, it *Item) (bool, error) {
	if err := r.alive(ctx); err != nil {
		return false, err
	}
	it.Phase = PhaseDeleting
	it.Attempts++
	it.LastError = ""
	it.UpdatedNano = time.Now().UnixNano()
	if err := r.save(ctx); err != nil {
		return false, err
	}

	delErr := r.store.DeleteMany(ctx, it.Objects)
	survivors, err := r.reconcileBatch(ctx, it.Objects, delErr)
	if err != nil {
		return false, err
	}
	it.UpdatedNano = time.Now().UnixNano()
	if delErr != nil || len(survivors) > 0 {
		it.LastError = ""
		if delErr != nil {
			it.LastError = delErr.Error()
		}
	}
	if len(survivors) == 0 {
		it.Phase = PhaseDeleted
		it.LastError = ""
	} else {
		// Keep only the objects the store still holds; only they are retried.
		remaining := make([]string, 0, len(survivors))
		for _, o := range it.Objects {
			if survivors[o] {
				remaining = append(remaining, o)
			}
		}
		it.Objects = remaining
		it.Phase = PhasePending
	}
	if err := r.save(ctx); err != nil {
		return false, err
	}
	return len(survivors) > 0, nil
}

// reconcileBatch establishes object facts after a DeleteMany. A nil error
// confirms every key. Otherwise each key is checked: absent keys are
// confirmed deleted (the batch partially succeeded before failing); present
// keys come back as survivors for a retry. Fact checks run on a detached
// context when the request context is already dead.
func (r *Runner) reconcileBatch(ctx context.Context, keys []string, delErr error) (map[string]bool, error) {
	survivors := make(map[string]bool)
	if delErr == nil {
		return survivors, nil
	}
	r.log.Warnf("gc: delete batch of %d object(s) returned an error; verifying object facts before retry: %v", len(keys), delErr)
	factCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		factCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}
	for _, k := range keys {
		present, err := objectExists(factCtx, r.store, k)
		if err != nil {
			// Unknown fact: treat as survivor so it is never wrongly
			// skipped, and surface the error to stop this phase.
			survivors[k] = true
			return survivors, fmt.Errorf("gc: verify delete of %q: %w", k, err)
		}
		if present {
			survivors[k] = true
		}
	}
	return survivors, nil
}

// anyPresent reports whether at least one key still exists.
func (r *Runner) anyPresent(ctx context.Context, keys []string) (bool, error) {
	for _, k := range keys {
		present, err := objectExists(ctx, r.store, k)
		if err != nil {
			return false, err
		}
		if present {
			return true, nil
		}
	}
	return false, nil
}

// objectExists checks key via the optional Stater interface (HEAD), falling
// back to a short GET on stores that only implement Store.
func objectExists(ctx context.Context, store object.Store, key string) (bool, error) {
	if st, ok := store.(object.Stater); ok {
		err := st.Stat(ctx, key)
		if err == nil {
			return true, nil
		}
		if errors.Is(err, object.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	rc, err := store.Get(ctx, key)
	if err == nil {
		_ = rc.Close()
		return true, nil
	}
	if errors.Is(err, object.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (r *Runner) retain(it *Item, why string) {
	it.Phase = PhaseRetained
	it.LastError = ""
	it.UpdatedNano = time.Now().UnixNano()
	r.log.Debugf("gc: retaining %s (%s)", it.Key, why)
}

// ── finalization ────────────────────────────────────────────────────────────

func (r *Runner) finishSucceeded(ctx context.Context) error {
	r.run.Status = StatusSucceeded
	r.run.Error = ""
	r.run.FinishedNano = time.Now().UnixNano()
	c := r.run.Counts()
	r.log.Infof("gc: run %s complete: %d checkpoint(s), %d sst(s), %d wal segment(s) deleted; %d retained",
		r.run.ID, c.CheckpointsDeleted, c.SSTsDeleted, c.WALsDeleted, c.Retained)
	if err := writeResult(ctx, r.store, r.run); err != nil {
		return fmt.Errorf("gc: write result: %w", err)
	}
	if err := publishLatest(ctx, r.store, r.run); err != nil {
		return fmt.Errorf("gc: publish result: %w", err)
	}
	if err := removeActive(ctx, r.store, r.run); err != nil {
		return fmt.Errorf("gc: clear active: %w", err)
	}
	r.pruneResults(ctx)
	return nil
}

func (r *Runner) finishFailed(ctx context.Context, msg string) error {
	r.run.Status = StatusFailed
	r.run.Error = msg
	r.run.FinishedNano = time.Now().UnixNano()
	r.log.Warnf("gc: run %s failed: %s (state retained for resume)", r.run.ID, msg)
	if err := writeResult(ctx, r.store, r.run); err != nil {
		return fmt.Errorf("gc: write failure result: %w", err)
	}
	if err := publishLatest(ctx, r.store, r.run); err != nil {
		return fmt.Errorf("gc: publish failure result: %w", err)
	}
	// Keep the active journal: the next invocation resumes against the same
	// pinned plan and only unconfirmed items.
	if err := r.save(ctx); err != nil {
		return err
	}
	return nil
}

// pruneResults bounds the number of per-run result records: result objects
// are tiny and unreferenced, but a run a day for years still accumulates.
// Only the lexicographically newest retainedResults keys (run IDs sort by
// creation time) survive. Errors are logged and otherwise ignored — pruning
// must never affect the deletion phases.
func (r *Runner) pruneResults(ctx context.Context) {
	const retainedResults = 20
	keys, err := r.store.List(ctx, ResultKeyPrefix)
	if err != nil {
		r.log.Warnf("gc: list results for pruning: %v", err)
		return
	}
	if len(keys) <= retainedResults {
		return
	}
	old := keys[:len(keys)-retainedResults]
	if err := r.store.DeleteMany(ctx, old); err != nil {
		r.log.Warnf("gc: prune %d old result record(s): %v", len(old), err)
	}
}

// ── fencing and journaling ──────────────────────────────────────────────────

func (r *Runner) alive(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.fenceErr()
}

func (r *Runner) fenceValid() bool {
	if r.opts.Fence == nil {
		return true
	}
	return r.opts.Fence()
}

func (r *Runner) fenceErr() error {
	if !r.fenceValid() {
		return ErrFenced
	}
	return nil
}

// save journals the current run. On a concurrent modification it reloads,
// merges item progress without regressing phases and retries; a journal that
// now belongs to another run means this generation has lost ownership.
func (r *Runner) save(ctx context.Context) error {
	if err := r.alive(ctx); err != nil {
		return err
	}
	for attempt := 0; attempt < 5; attempt++ {
		etag, err := saveActive(ctx, r.store, r.run, r.etag)
		if err == nil {
			r.etag = etag
			return nil
		}
		if !errors.Is(err, ErrConcurrentWrite) {
			return err
		}
		other, otherEtag, lerr := loadRun(ctx, r.store, ActiveKey)
		if lerr != nil {
			return lerr
		}
		if other == nil || !mergeProgress(r.run, other) {
			// The active journal belongs to a different (newer) run.
			return ErrFenced
		}
		r.etag = otherEtag
	}
	return ErrConcurrentWrite
}

// saveIfChanged journals pending retain decisions made without an explicit
// save (phases that only retained items).
func (r *Runner) saveIfChanged(ctx context.Context) error {
	return r.save(ctx)
}
