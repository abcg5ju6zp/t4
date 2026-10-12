package gc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// fixtureStore builds:
//   - four checkpoints (term 1, revisions 10..40): cp10 references sstA,
//     cp20 sstB, cp30 and cp40 sstC;
//   - manifest/latest pointing at cp40 with LastSequence 40;
//   - three WAL segments starting at 0/100/200.
//
// With keep=2 the run must delete cp10 and cp20 plus sstA/sstB, keep
// cp30/cp40 and sstC, and with horizon=150 delete only the first WAL segment
// (segment 100 ends at rev 199 > 150).
func fixtureStore(t *testing.T) *object.Mem {
	t.Helper()
	store := object.NewMem()
	populateFixture(t, store)
	return store
}

// populateFixture writes the fixture objects through store so encrypted
// wrappers are exercised on the same logical layout.
func populateFixture(t *testing.T, store object.Store) {
	t.Helper()
	ctx := context.Background()

	ssts := map[string][]byte{
		"sst/aaaaaaaa/1.sst": []byte("A"),
		"sst/bbbbbbbb/2.sst": []byte("BB"),
		"sst/cccccccc/3.sst": []byte("CCC"),
	}
	for k, v := range ssts {
		if err := store.Put(ctx, k, bytes.NewReader(v)); err != nil {
			t.Fatal(err)
		}
	}

	cpSSTs := map[int64][]string{
		10: {"sst/aaaaaaaa/1.sst"},
		20: {"sst/bbbbbbbb/2.sst"},
		30: {"sst/cccccccc/3.sst"},
		40: {"sst/cccccccc/3.sst"},
	}
	for rev, sstFiles := range cpSSTs {
		putCheckpoint(t, store, 1, rev, sstFiles)
	}
	putManifest(t, store, checkpoint.CheckpointIndexKey(1, 40), 40)

	for _, firstRev := range []int64{0, 100, 200} {
		if err := store.Put(ctx, wal.ObjectKey(1, firstRev), bytes.NewReader([]byte("wal"))); err != nil {
			t.Fatal(err)
		}
	}
}

func putCheckpoint(t *testing.T, store object.Store, term uint64, rev int64, sstFiles []string) string {
	t.Helper()
	key := checkpoint.CheckpointIndexKey(term, rev)
	idx := checkpoint.CheckpointIndex{
		FormatVersion: checkpoint.FormatVersionBase,
		Term:          term,
		Revision:      rev,
		LastSequence:  rev,
		SSTFiles:      sstFiles,
		PebbleMeta:    []string{"MANIFEST-000001", "OPTIONS-" + fmt.Sprintf("%06d", rev)},
	}
	b, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Put(ctx, key, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	prefix := key[:strings.LastIndex(key, "/")+1]
	for _, name := range idx.PebbleMeta {
		if err := store.Put(ctx, prefix+name, bytes.NewReader([]byte("meta"))); err != nil {
			t.Fatal(err)
		}
	}
	return key
}

func putManifest(t *testing.T, store object.Store, cpKey string, seq int64) {
	t.Helper()
	m := &checkpoint.Manifest{
		FormatVersion: checkpoint.FormatVersionBase,
		CheckpointKey: cpKey,
		Revision:      seq,
		LastSequence:  seq,
		Term:          1,
		LastWALKey:    wal.ObjectKey(1, 0),
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), checkpoint.ManifestKey, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
}

func autoOpts(term uint64, horizon int64) Options {
	return Options{
		Kind: KindAuto,
		Term: term,
		Keep: 2,
		WALHorizon: func(context.Context) (int64, bool, error) {
			return horizon, true, nil
		},
	}
}

// ── fault injection ────────────────────────────────────────────────────────

type faultStore struct {
	*object.Mem
	block chan struct{}
	// entered is closed once when a DeleteMany call reaches the block point.
	entered chan struct{}
	// failMatch decides which batches fail; nil fails every batch.
	failMatch func(keys []string) bool
	// partial, when set, is applied to the underlying mem store before a
	// failing batch returns: those keys really disappeared, so the response
	// is ambiguous for them.
	partial func(keys []string) []string
}

func (f *faultStore) DeleteMany(ctx context.Context, keys []string) error {
	if f.block != nil {
		if f.entered != nil {
			select {
			case f.entered <- struct{}{}:
			default:
			}
		}
		select {
		case <-f.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	match := f.failMatch == nil || f.failMatch(keys)
	var gone []string
	if match && f.partial != nil {
		gone = f.partial(keys)
	}
	if match {
		if len(gone) > 0 {
			_ = f.Mem.DeleteMany(ctx, gone)
		}
		return errors.New("simulated ambiguous delete failure")
	}
	return f.Mem.DeleteMany(ctx, keys)
}

func hasPrefix(keys []string, prefix string) bool {
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

func allPrefix(keys []string, prefix string) bool {
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) {
			return false
		}
	}
	return len(keys) > 0
}

// ── tests ──────────────────────────────────────────────────────────────────

func TestRunOneShot(t *testing.T) {
	store := fixtureStore(t)
	r := New(checkpoint.New(nil), store, autoOpts(1, 150))
	run, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Status != StatusSucceeded {
		t.Fatalf("status=%s want succeeded: %s", run.Status, run.Error)
	}
	c := run.Counts()
	if c.CheckpointsDeleted != 2 || c.SSTsDeleted != 2 || c.WALsDeleted != 1 {
		t.Fatalf("counts=%+v, want cp=2 sst=2 wal=1", c)
	}
	if run.WALHorizonSeq != 150 {
		t.Fatalf("pinned horizon=%d want 150", run.WALHorizonSeq)
	}

	active, err := ReadActive(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if active != nil {
		t.Fatalf("active journal not removed: %s", active.ID)
	}
	latest, err := ReadLatest(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.Status != StatusSucceeded {
		t.Fatalf("latest result not queryable: %+v", latest)
	}
	if _, err := ReadResult(context.Background(), store, run.ID); err != nil {
		t.Fatalf("per-run result: %v", err)
	}

	// Retention: newest two checkpoints and the SST they reference survive.
	for _, rev := range []int64{30, 40} {
		if _, err := store.Get(context.Background(), checkpoint.CheckpointIndexKey(1, rev)); err != nil {
			t.Fatalf("retained checkpoint rev=%d gone: %v", rev, err)
		}
	}
	for _, rev := range []int64{10, 20} {
		if _, err := store.Get(context.Background(), checkpoint.CheckpointIndexKey(1, rev)); err != object.ErrNotFound {
			t.Fatalf("expired checkpoint rev=%d not deleted", rev)
		}
	}
	if _, err := store.Get(context.Background(), "sst/cccccccc/3.sst"); err != nil {
		t.Fatalf("protected sstC deleted: %v", err)
	}
	for _, k := range []string{"sst/aaaaaaaa/1.sst", "sst/bbbbbbbb/2.sst"} {
		if _, err := store.Get(context.Background(), k); err != object.ErrNotFound {
			t.Fatalf("%s not deleted", k)
		}
	}
	if _, err := store.Get(context.Background(), wal.ObjectKey(1, 0)); err != object.ErrNotFound {
		t.Fatal("covered WAL segment retained")
	}
	for _, rev := range []int64{100, 200} {
		if _, err := store.Get(context.Background(), wal.ObjectKey(1, rev)); err != nil {
			t.Fatalf("WAL %d must survive: %v", rev, err)
		}
	}
}

func TestResumeOnlyUnconfirmedItems(t *testing.T) {
	mem := fixtureStore(t)
	fault := &faultStore{Mem: mem, failMatch: func(keys []string) bool {
		return hasPrefix(keys, "sst/")
	}}

	r1 := New(checkpoint.New(nil), fault, autoOpts(1, 150))
	run1, err := r1.Run(context.Background())
	if err != nil {
		t.Fatalf("run1: %v", err)
	}
	if run1.Status != StatusFailed {
		t.Fatalf("run1 status=%s want failed", run1.Status)
	}
	active, _ := ReadActive(context.Background(), fault)
	if active == nil || active.Status != StatusFailed {
		t.Fatal("failed run must stay active for resume")
	}
	latest, _ := ReadLatest(context.Background(), fault)
	if latest == nil || latest.Status != StatusFailed {
		t.Fatal("failed run result must be queryable")
	}
	// Checkpoint phase completed; SST and WAL phases did not.
	if _, err := mem.Get(context.Background(), "sst/aaaaaaaa/1.sst"); err != nil {
		t.Fatal("sstA must survive the failed run")
	}
	if _, err := mem.Get(context.Background(), wal.ObjectKey(1, 0)); err != nil {
		t.Fatal("WAL must not be deleted after an earlier-phase failure")
	}

	// Second invocation, fault cleared: same pinned plan, resume.
	r2 := New(checkpoint.New(nil), mem, autoOpts(1, 150))
	run2, err := r2.Run(context.Background())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if run2.Status != StatusSucceeded {
		t.Fatalf("run2 status=%s: %s", run2.Status, run2.Error)
	}
	if run2.ID != run1.ID {
		t.Fatalf("resume created a new run: %s != %s", run2.ID, run1.ID)
	}
	c := run2.Counts()
	// Cumulative confirmed deletes since the run began.
	if c.CheckpointsDeleted != 2 || c.SSTsDeleted != 2 || c.WALsDeleted != 1 {
		t.Fatalf("counts=%+v", c)
	}
	for _, k := range []string{"sst/aaaaaaaa/1.sst", "sst/bbbbbbbb/2.sst", wal.ObjectKey(1, 0)} {
		if _, err := mem.Get(context.Background(), k); err != object.ErrNotFound {
			t.Fatalf("%s survived resume", k)
		}
	}
	if a, _ := ReadActive(context.Background(), mem); a != nil {
		t.Fatal("active journal not cleared after resume success")
	}
}

func TestAmbiguousDeleteReconciledByObjectFact(t *testing.T) {
	mem := fixtureStore(t)
	sstA := "sst/aaaaaaaa/1.sst"
	fault := &faultStore{
		Mem:       mem,
		failMatch: func(keys []string) bool { return hasPrefix(keys, "sst/") },
		partial: func(keys []string) []string {
			// The store really deleted sstA before the response was lost;
			// sstB is untouched.
			var gone []string
			for _, k := range keys {
				if k == sstA {
					gone = append(gone, k)
				}
			}
			return gone
		},
	}

	r1 := New(checkpoint.New(nil), fault, autoOpts(1, 150))
	run1, err := r1.Run(context.Background())
	if err != nil {
		t.Fatalf("run1: %v", err)
	}
	if run1.Status != StatusFailed {
		t.Fatalf("status=%s want failed", run1.Status)
	}
	var sstAItem, sstBItem *Item
	for _, it := range run1.SSTs {
		switch it.Key {
		case sstA:
			sstAItem = it
		case "sst/bbbbbbbb/2.sst":
			sstBItem = it
		}
	}
	if sstAItem == nil || sstAItem.Phase != PhaseDeleted {
		t.Fatalf("sstA phase=%v, want deleted after fact check", phaseOf(sstAItem))
	}
	if sstBItem == nil || sstBItem.Phase != PhasePending {
		t.Fatalf("sstB phase=%v, want pending", phaseOf(sstBItem))
	}

	r2 := New(checkpoint.New(nil), mem, autoOpts(1, 150))
	run2, err := r2.Run(context.Background())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if run2.Status != StatusSucceeded {
		t.Fatalf("run2: %s %s", run2.Status, run2.Error)
	}
	// sstA was never attempted again: its item carries exactly one attempt.
	for _, it := range run2.SSTs {
		if it.Key == sstA && it.Attempts != 1 {
			t.Fatalf("sstA attempts=%d, want 1 (retry must skip confirmed items)", it.Attempts)
		}
	}
	if c := run2.Counts(); c.SSTsDeleted != 2 {
		t.Fatalf("sst deleted=%d want 2", c.SSTsDeleted)
	}
}

func phaseOf(it *Item) Phase {
	if it == nil {
		return "<nil>"
	}
	return it.Phase
}

func TestResumeRetainsCheckpointNewBranchPins(t *testing.T) {
	mem := fixtureStore(t)
	cp10 := checkpoint.CheckpointIndexKey(1, 10)
	fault := &faultStore{Mem: mem, failMatch: func(keys []string) bool {
		return hasPrefix(keys, cp10[:strings.LastIndex(cp10, "/")+1])
	}}
	r1 := New(checkpoint.New(nil), fault, autoOpts(1, 150))
	run1, err := r1.Run(context.Background())
	if err != nil {
		t.Fatalf("run1: %v", err)
	}
	if run1.Status != StatusFailed {
		t.Fatalf("run1 status=%s", run1.Status)
	}

	// A branch registers against the checkpoint the failed run planned to
	// delete.
	mgr := checkpoint.New(nil)
	if err := mgr.RegisterBranch(context.Background(), mem, "fork", cp10); err != nil {
		t.Fatal(err)
	}

	r2 := New(checkpoint.New(nil), mem, autoOpts(1, 150))
	run2, err := r2.Run(context.Background())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if run2.Status != StatusSucceeded {
		t.Fatalf("run2: %s %s", run2.Status, run2.Error)
	}
	if _, err := mem.Get(context.Background(), cp10); err != nil {
		t.Fatalf("branch-pinned checkpoint deleted on resume: %v", err)
	}
	if _, err := mem.Get(context.Background(), "sst/aaaaaaaa/1.sst"); err != nil {
		t.Fatal("SST of the branch-pinned checkpoint deleted on resume")
	}
	if _, err := mem.Get(context.Background(), checkpoint.CheckpointIndexKey(1, 20)); err != object.ErrNotFound {
		t.Fatal("unpinned checkpoint cp20 not deleted")
	}
	// WAL phase still advances: only cp10 was retained, no phase failed.
	if _, err := mem.Get(context.Background(), wal.ObjectKey(1, 0)); err != object.ErrNotFound {
		t.Fatal("WAL phase blocked by retained checkpoint")
	}
	for _, it := range run2.Checkpoints {
		if it.Key == cp10 && it.Phase != PhaseRetained {
			t.Fatalf("cp10 phase=%s want retained", it.Phase)
		}
	}
}

func TestResumeProtectsSSTReferencedByNewCheckpoint(t *testing.T) {
	mem := fixtureStore(t)
	sstA := "sst/aaaaaaaa/1.sst"
	fault := &faultStore{Mem: mem, failMatch: func(keys []string) bool {
		return hasPrefix(keys, "sst/")
	}}
	r1 := New(checkpoint.New(nil), fault, autoOpts(1, 150))
	if _, err := r1.Run(context.Background()); err != nil {
		t.Fatalf("run1: %v", err)
	}

	// Before resume a newer checkpoint appears that re-uses the
	// content-addressed sstA. It is not in the pinned protection set.
	putCheckpoint(t, mem, 1, 50, []string{sstA})
	putManifest(t, mem, checkpoint.CheckpointIndexKey(1, 50), 50)

	r2 := New(checkpoint.New(nil), mem, autoOpts(1, 150))
	run2, err := r2.Run(context.Background())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if run2.Status != StatusSucceeded {
		t.Fatalf("run2: %s %s", run2.Status, run2.Error)
	}
	if _, err := mem.Get(context.Background(), sstA); err != nil {
		t.Fatal("sstA referenced by a new checkpoint was deleted on resume")
	}
	if _, err := mem.Get(context.Background(), "sst/bbbbbbbb/2.sst"); err != object.ErrNotFound {
		t.Fatal("sstB should still be deleted")
	}
}

func TestWALRetainedWhenFollowerHorizonRegresses(t *testing.T) {
	mem := fixtureStore(t)
	// CP+SST phases succeed, WAL batch fails.
	fault := &faultStore{Mem: mem, failMatch: func(keys []string) bool {
		return allPrefix(keys, "wal/")
	}}
	r1 := New(checkpoint.New(nil), fault, autoOpts(1, 150))
	if run, err := r1.Run(context.Background()); err != nil || run.Status != StatusFailed {
		t.Fatalf("run1: run=%+v err=%v", run, err)
	}

	// A straggler follower pushed the safe horizon back to 50: segment 0
	// covers up to rev 99 and must be retained.
	r2 := New(checkpoint.New(nil), mem, autoOpts(1, 50))
	run2, err := r2.Run(context.Background())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if run2.Status != StatusSucceeded {
		t.Fatalf("run2: %s %s", run2.Status, run2.Error)
	}
	if _, err := mem.Get(context.Background(), wal.ObjectKey(1, 0)); err != nil {
		t.Fatal("WAL segment outside the current horizon was deleted")
	}
	for _, it := range run2.WALs {
		if it.Phase != PhaseRetained {
			t.Fatalf("wal %s phase=%s want retained", it.Key, it.Phase)
		}
	}
}

func TestLeadershipGenerationSupersedesAndFences(t *testing.T) {
	mem := fixtureStore(t)
	// Term 1 fails in the SST phase, leaving an active run pinned to term 1.
	fault1 := &faultStore{Mem: mem, failMatch: func(keys []string) bool {
		return hasPrefix(keys, "sst/")
	}}
	r1 := New(checkpoint.New(nil), fault1, autoOpts(1, 150))
	run1, err := r1.Run(context.Background())
	if err != nil {
		t.Fatalf("run1: %v", err)
	}
	if run1.Term != 1 {
		t.Fatalf("pinned term=%d want 1", run1.Term)
	}

	// Term 2 starts: old run is finalized superseded, a new plan is made.
	// By now cp10/cp20 are gone, so the term-2 plan contains only WAL items;
	// make its WAL batch fail so a term-2 active journal stays in place.
	fault2 := &faultStore{Mem: mem, failMatch: func(keys []string) bool {
		return allPrefix(keys, "wal/")
	}}
	r2 := New(checkpoint.New(nil), fault2, autoOpts(2, 150))
	run2, err := r2.Run(context.Background())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if run2.ID == run1.ID {
		t.Fatal("new generation reused the old run instead of superseding it")
	}
	if run2.Term != 2 {
		t.Fatalf("new run term=%d want 2", run2.Term)
	}
	old, err := ReadResult(context.Background(), mem, run1.ID)
	if err != nil {
		t.Fatalf("superseded result not persisted: %v", err)
	}
	if old.Status != StatusSuperseded {
		t.Fatalf("old run status=%s want superseded", old.Status)
	}

	// A stale term-1 execution must refuse the term-2 active run even if its
	// local lease callback wrongly reports validity: the journal generation
	// is the store-side fence.
	stale := New(checkpoint.New(nil), mem, Options{
		Kind: KindAuto, Term: 1, Keep: 2,
		WALHorizon: autoOpts(1, 150).WALHorizon,
		Fence:      func() bool { return true },
	})
	if _, err := stale.Run(context.Background()); !errors.Is(err, ErrFenced) {
		t.Fatalf("older generation err=%v want ErrFenced", err)
	}

	// Term 2 completes on a healthy store; its result wins the latest pointer.
	r2b := New(checkpoint.New(nil), mem, autoOpts(2, 150))
	run2b, err := r2b.Run(context.Background())
	if err != nil {
		t.Fatalf("run2b: %v", err)
	}
	if run2b.ID != run2.ID || run2b.Status != StatusSucceeded {
		t.Fatalf("term2 resume = %s/%s, want %s/succeeded", run2b.ID, run2b.Status, run2.ID)
	}
	latest, _ := ReadLatest(context.Background(), mem)
	if latest == nil || latest.ID != run2.ID {
		t.Fatal("newer generation's result was clobbered by the old one")
	}

	// A stale term-1 execution whose lease callback reports expiry stops
	// before touching anything.
	stale2 := New(checkpoint.New(nil), mem, Options{
		Kind: KindAuto, Term: 1, Keep: 2,
		WALHorizon: autoOpts(1, 150).WALHorizon,
		Fence:      func() bool { return false },
	})
	if _, err := stale2.Run(context.Background()); !errors.Is(err, ErrFenced) {
		t.Fatalf("fence-expired execution err=%v want ErrFenced", err)
	}
}

func TestCancellationResumesFromDeletingJournal(t *testing.T) {
	mem := fixtureStore(t)
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	blocked := &faultStore{Mem: mem, block: block, entered: entered, failMatch: nil}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := New(checkpoint.New(nil), blocked, autoOpts(1, 150)).Run(ctx)
		done <- err
	}()
	<-entered // the run journaled its plan and parked inside the first delete
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled run returned nil error")
	}

	// Journal survived with a checkpoint item in deleting state; a fresh
	// invocation reconciles facts and completes against the same plan.
	active, _ := ReadActive(context.Background(), mem)
	if active == nil {
		t.Fatal("cancelled run left no journal")
	}
	inFlight := false
	for _, it := range active.Checkpoints {
		if it.Phase == PhaseDeleting || it.Phase == PhaseUncertain || it.Phase == PhasePending {
			inFlight = true
		}
	}
	if !inFlight {
		t.Fatal("journal has no resumable checkpoint item")
	}
	close(block) // release the blocked delete for any future call (mem ctx is dead anyway)

	r2 := New(checkpoint.New(nil), mem, autoOpts(1, 150))
	run2, err := r2.Run(context.Background())
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if run2.ID != active.ID {
		t.Fatal("resume created a new run")
	}
	if run2.Status != StatusSucceeded {
		t.Fatalf("resume status=%s: %s", run2.Status, run2.Error)
	}
}

func TestNoCandidatesWritesNoJournal(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	putCheckpoint(t, store, 1, 10, []string{"sst/a/1.sst"})
	if err := store.Put(ctx, "sst/a/1.sst", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	putManifest(t, store, checkpoint.CheckpointIndexKey(1, 10), 10)
	if err := store.Put(ctx, wal.ObjectKey(1, 0), bytes.NewReader([]byte("w"))); err != nil {
		t.Fatal(err)
	}

	run, err := New(checkpoint.New(nil), store, autoOpts(1, 10)).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != "" || run.Status != StatusSucceeded {
		t.Fatalf("empty run=%+v", run)
	}
	keys, err := store.List(ctx, "gc/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("no-candidate run journaled objects: %v", keys)
	}
}

func TestEncryptedStoreBehavior(t *testing.T) {
	mem := object.NewMem()
	kp, err := object.NewStaticKeyProvider(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	store := object.NewEncryptedStore(mem, kp)
	populateFixture(t, store)

	run, err := New(checkpoint.New(nil), store, autoOpts(1, 150)).Run(context.Background())
	if err != nil {
		t.Fatalf("run over encrypted store: %v", err)
	}
	if run.Status != StatusSucceeded {
		t.Fatalf("status=%s: %s", run.Status, run.Error)
	}
	// Outcomes are identical to the plaintext run.
	for _, rev := range []int64{10, 20} {
		if _, err := mem.Get(context.Background(), checkpoint.CheckpointIndexKey(1, rev)); err != object.ErrNotFound {
			t.Fatalf("cp rev=%d not deleted", rev)
		}
	}
	if _, err := mem.Get(context.Background(), "sst/cccccccc/3.sst"); err != nil {
		t.Fatal("protected sst gone")
	}
	// The journal bodies are encrypted on the wire/raw store.
	raw, err := mem.Get(context.Background(), LatestKey)
	if err != nil {
		t.Fatalf("raw latest: %v", err)
	}
	head := make([]byte, 4)
	if _, err := raw.Read(head); err != nil {
		t.Fatal(err)
	}
	if string(head) != "T4E1" {
		t.Fatalf("journal not encrypted, head=%q", head)
	}
	// And readable (decoded) through the wrapper for status queries.
	latest, err := ReadLatest(context.Background(), store)
	if err != nil {
		t.Fatalf("read latest through encryption: %v", err)
	}
	if latest == nil || latest.Counts().CheckpointsDeleted != 2 {
		t.Fatalf("latest through encrypted wrapper: %+v", latest)
	}
}

func TestCLIResumesAutoRun(t *testing.T) {
	mem := fixtureStore(t)
	// An automatic run fails mid-SST phase.
	fault := &faultStore{Mem: mem, failMatch: func(keys []string) bool {
		return hasPrefix(keys, "sst/")
	}}
	auto := New(checkpoint.New(nil), fault, autoOpts(1, 150))
	run1, err := auto.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// The CLI has no generation: it resumes the leader's pinned run and only
	// finishes its unconfirmed items.
	cli := New(checkpoint.New(nil), mem, Options{Kind: KindCLI, Keep: 2, WALHorizon: func(context.Context) (int64, bool, error) {
		return 150, true, nil
	}})
	run2, err := cli.Run(context.Background())
	if err != nil {
		t.Fatalf("cli run: %v", err)
	}
	if run2.ID != run1.ID {
		t.Fatal("CLI did not resume the active run")
	}
	if run2.Status != StatusSucceeded {
		t.Fatalf("cli run status=%s: %s", run2.Status, run2.Error)
	}
}
