package checkpoint_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

func errorIs(err, target error) bool { return errors.Is(err, target) }

func jsonReader(v any) io.Reader {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return bytes.NewReader(b)
}

// forkFixture creates a source checkpoint, registers branch "b" against it,
// and builds a branch DB bootstrapped from that checkpoint with one branch
// write checkpointed — an index that still records AncestorSSTFiles.
func forkFixture(t *testing.T, branchTerm uint64, branchRev int64) (src, branch *object.Mem, srcDB, branchDB *pebble.DB, ancestorKey string) {
	t.Helper()
	ctx := context.Background()
	src = object.NewMem()
	branch = object.NewMem()

	srcDB = openDB(t)
	for i := range 3 {
		if err := srcDB.Set([]byte(fmt.Sprintf("src-%d", i)), []byte("v"), pebble.Sync); err != nil {
			t.Fatalf("src set: %v", err)
		}
	}
	if err := srcDB.Flush(); err != nil {
		t.Fatalf("src flush: %v", err)
	}
	if err := testCP.Write(ctx, srcDB, src, 1, 100, "", nil); err != nil {
		t.Fatalf("src Write: %v", err)
	}
	m, err := testCP.ReadManifest(ctx, src)
	if err != nil || m == nil {
		t.Fatalf("src manifest: %v", err)
	}
	ancestorKey = m.CheckpointKey
	if err := testCP.RegisterBranch(ctx, src, "b", ancestorKey); err != nil {
		t.Fatalf("RegisterBranch: %v", err)
	}

	branchDir := filepath.Join(t.TempDir(), "branchdb")
	if _, _, err := testCP.RestoreBranch(ctx, src, nil, ancestorKey, branchDir); err != nil {
		t.Fatalf("RestoreBranch: %v", err)
	}
	branchDB, err = pebble.Open(branchDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open branch db: %v", err)
	}
	t.Cleanup(func() { _ = branchDB.Close() })
	if err := branchDB.Set([]byte("branch-key"), []byte("branch-value"), pebble.Sync); err != nil {
		t.Fatalf("branch set: %v", err)
	}
	if err := branchDB.Flush(); err != nil {
		t.Fatalf("branch flush: %v", err)
	}
	if err := testCP.Write(ctx, branchDB, branch, branchTerm, branchRev, "", src); err != nil {
		t.Fatalf("branch Write: %v", err)
	}
	return src, branch, srcDB, branchDB, ancestorKey
}

func headIndex(t *testing.T, ctx context.Context, store object.Store) *checkpoint.CheckpointIndex {
	t.Helper()
	m, err := testCP.ReadManifest(ctx, store)
	if err != nil || m == nil {
		t.Fatalf("manifest: %v", err)
	}
	idx, err := testCP.ReadCheckpointIndex(ctx, store, m.CheckpointKey)
	if err != nil {
		t.Fatalf("read head index: %v", err)
	}
	return idx
}

func assertRestore(t *testing.T, ctx context.Context, store object.Store, key, value string) {
	t.Helper()
	target := filepath.Join(t.TempDir(), "restored")
	if _, _, err := testCP.Restore(ctx, store, mustHeadKey(t, ctx, store), target); err != nil {
		t.Fatalf("restore branch checkpoint from branch store only: %v", err)
	}
	db, err := pebble.Open(target, &pebble.Options{})
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer db.Close()
	v, closer, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("restored get %q: %v", key, err)
	}
	if string(v) != value {
		t.Fatalf("restored %q=%q want %q", key, v, value)
	}
	_ = closer.Close()
}

func mustHeadKey(t *testing.T, ctx context.Context, store object.Store) string {
	t.Helper()
	m, err := testCP.ReadManifest(ctx, store)
	if err != nil || m == nil {
		t.Fatalf("manifest: %v", err)
	}
	return m.CheckpointKey
}

// TestHandoffFullFlow is the happy path: ancestor SSTs are copied into
// the branch store, the head index is rewritten independent and verified, the
// source entry is released; afterwards source GC may delete the ancestor
// checkpoint and its SST copies without breaking a branch-only restore.
func TestHandoffFullFlow(t *testing.T) {
	ctx := context.Background()
	src, branch, srcDB, _, ancestorKey := forkFixture(t, 1, 200)

	idx := headIndex(t, ctx, branch)
	if len(idx.AncestorSSTFiles) == 0 {
		t.Fatalf("precondition: branch head references no ancestor SSTs")
	}

	st, err := testCP.RunHandoff(ctx, src, branch, "b")
	if err != nil {
		t.Fatalf("RunHandoff: %v", err)
	}
	if st.Phase != checkpoint.HandoffPhaseDone {
		t.Fatalf("phase=%s, want done", st.Phase)
	}

	// Source registration released and removed.
	if entries, _ := testCP.ReadBranchEntries(ctx, src); len(entries) != 0 {
		t.Fatalf("source branch entries after handoff: %v", entries)
	}
	rel, err := testCP.ReadReleasedHandoff(ctx, branch)
	if err != nil || rel == nil || rel.Phase != checkpoint.HandoffPhaseDone {
		t.Fatalf("released marker: %+v err=%v", rel, err)
	}
	if _, err := branch.Get(ctx, checkpoint.HandoffActiveKey); err != object.ErrNotFound {
		t.Fatalf("active marker still present: %v", err)
	}

	idx = headIndex(t, ctx, branch)
	if len(idx.AncestorSSTFiles) != 0 {
		t.Fatalf("head still references %d ancestor SSTs", len(idx.AncestorSSTFiles))
	}
	if len(idx.SSTFiles) == 0 {
		t.Fatalf("head has no local SSTs")
	}
	for _, k := range idx.SSTFiles {
		rc, err := branch.Get(ctx, k)
		if err != nil {
			t.Fatalf("local sst %q missing: %v", k, err)
		}
		_ = rc.Close()
	}
	assertRestore(t, ctx, branch, "branch-key", "branch-value")

	// Age the ancestor checkpoint on the source and run full GC.
	if err := srcDB.Set([]byte("src-new"), []byte("v"), pebble.Sync); err != nil {
		t.Fatalf("src set: %v", err)
	}
	if err := srcDB.Flush(); err != nil {
		t.Fatalf("src flush: %v", err)
	}
	if err := testCP.Write(ctx, srcDB, src, 1, 110, "", nil); err != nil {
		t.Fatalf("src Write 2: %v", err)
	}
	deleted, candidates, err := testCP.GCCheckpoints(ctx, src, 1)
	if err != nil {
		t.Fatalf("GCCheckpoints: %v", err)
	}
	if deleted == 0 {
		t.Fatalf("ancestor checkpoint %q never became collectable", ancestorKey)
	}
	if _, err := testCP.GCOrphanSSTs(ctx, src, candidates); err != nil {
		t.Fatalf("GCOrphanSSTs: %v", err)
	}
	if keys, _ := src.List(ctx, ancestorKey[:len(ancestorKey)-len("manifest.json")]); len(keys) != 0 {
		t.Fatalf("ancestor checkpoint objects remain: %v", keys)
	}

	// The branch restore depends on nothing from the source store.
	assertRestore(t, ctx, branch, "branch-key", "branch-value")
}

// flakySource fails the first few GETs of sst/ keys — interrupting an
// ancestor SST copy — then succeeds. It embeds Mem (a
// ConditionalStore) so CAS paths stay exercised.
type flakySource struct {
	*object.Mem
	failCount atomic.Int32
}

func (f *flakySource) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if strings.HasPrefix(key, "sst/") && f.failCount.Add(-1) >= 0 {
		return nil, fmt.Errorf("injected read failure for %q", key)
	}
	return f.Mem.Get(ctx, key)
}

// TestHandoffResumesSameGenerationAfterCopyFailure: an interrupted object
// copy leaves everything queryable and protective; the retry resumes the
// very same handoff id and finishes.
func TestHandoffResumesSameGenerationAfterCopyFailure(t *testing.T) {
	ctx := context.Background()
	flaky := &flakySource{Mem: object.NewMem()}
	branch := object.NewMem()

	srcDB := openDB(t)
	if err := srcDB.Set([]byte("k"), []byte("v"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := srcDB.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := testCP.Write(ctx, srcDB, flaky, 1, 100, "", nil); err != nil {
		t.Fatalf("src write: %v", err)
	}
	ancestor := mustHeadKey(t, ctx, flaky)
	if err := testCP.RegisterBranch(ctx, flaky, "b", ancestor); err != nil {
		t.Fatalf("register: %v", err)
	}
	// A newer source checkpoint exists, so the ancestor would be GC-eligible
	// were the branch not protecting it.
	if err := srcDB.Set([]byte("k2"), []byte("v"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := srcDB.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := testCP.Write(ctx, srcDB, flaky, 1, 101, "", nil); err != nil {
		t.Fatalf("src write 2: %v", err)
	}

	branchDir := filepath.Join(t.TempDir(), "b")
	if _, _, err := testCP.RestoreBranch(ctx, flaky, nil, ancestor, branchDir); err != nil {
		t.Fatalf("restore: %v", err)
	}
	bdb, err := pebble.Open(branchDir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()
	if err := bdb.Set([]byte("bk"), []byte("bv"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := testCP.Write(ctx, bdb, branch, 1, 200, "", flaky); err != nil {
		t.Fatalf("branch write: %v", err)
	}
	anc := headIndex(t, ctx, branch)
	if len(anc.AncestorSSTFiles) == 0 {
		t.Fatalf("precondition: ancestor refs present")
	}

	flaky.failCount.Store(1)
	st, err := testCP.RunHandoff(ctx, flaky, branch, "b")
	if err == nil {
		t.Fatalf("first run must surface the injected copy error")
	}
	if st == nil || st.HandoffID == "" {
		t.Fatalf("failed run must return the resumable state: %v", st)
	}
	firstID := st.HandoffID

	// Failure stays queryable and protective.
	entry, _, err := testCP.ReadBranchEntry(ctx, flaky, "b")
	if err != nil {
		t.Fatalf("read entry: %v", err)
	}
	if entry == nil || entry.State != checkpoint.BranchStateSolidifying || entry.HandoffID != firstID {
		t.Fatalf("entry after failure: %+v", entry)
	}
	deleted, _, err := testCP.GCCheckpoints(ctx, flaky, 1)
	if err != nil {
		t.Fatalf("gc during handoff: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("GC deleted %d checkpoint(s) while handoff unconfirmed", deleted)
	}

	// Retry: same generation, completes.
	st2, err := testCP.RunHandoff(ctx, flaky, branch, "b")
	if err != nil {
		t.Fatalf("retry handoff: %v", err)
	}
	if st2.HandoffID != firstID {
		t.Fatalf("generation changed: %q → %q", firstID, st2.HandoffID)
	}
	if st2.Phase != checkpoint.HandoffPhaseDone {
		t.Fatalf("phase=%s", st2.Phase)
	}
	idx := headIndex(t, ctx, branch)
	if len(idx.AncestorSSTFiles) != 0 {
		t.Fatalf("head still ancestor-dependent")
	}
	assertRestore(t, ctx, branch, "bk", "bv")
}

// TestGCSkipsReleasedTombstone: a released entry protects nothing even if
// its object has not been deleted yet.
func TestGCSkipsReleasedTombstone(t *testing.T) {
	ctx := context.Background()
	store := object.NewMem()
	db := openDB(t)
	if err := db.Set([]byte("k1"), []byte("v1"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := testCP.Write(ctx, db, store, 1, 1, "", nil); err != nil {
		t.Fatal(err)
	}
	pinned := mustHeadKey(t, ctx, store)
	tomb := checkpoint.BranchEntry{
		AncestorCheckpointKey: pinned,
		HandoffID:             "h-dead",
		State:                 checkpoint.BranchStateReleased,
	}
	// Put the tombstone directly, the way CompleteHandoff would.
	store.Put(ctx, "branches/b", jsonReader(tomb))
	if err := db.Set([]byte("k2"), []byte("v2"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := testCP.Write(ctx, db, store, 1, 2, "", nil); err != nil {
		t.Fatal(err)
	}
	deleted, _, err := testCP.GCCheckpoints(ctx, store, 1)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("released tombstone still pinned its checkpoint: deleted=%d", deleted)
	}
}

// TestHandoffAlreadyIndependent: a branch head with no ancestor refs
// completes without copying anything.
func TestHandoffAlreadyIndependent(t *testing.T) {
	ctx := context.Background()
	src, branch := object.NewMem(), object.NewMem()
	db := openDB(t)
	if err := db.Set([]byte("k"), []byte("v"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := testCP.Write(ctx, db, src, 1, 100, "", nil); err != nil {
		t.Fatal(err)
	}
	ancestor := mustHeadKey(t, ctx, src)
	if err := testCP.RegisterBranch(ctx, src, "i", ancestor); err != nil {
		t.Fatal(err)
	}
	bdb := openDB(t)
	if err := bdb.Set([]byte("x"), []byte("y"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := testCP.Write(ctx, bdb, branch, 1, 1, "", nil); err != nil {
		t.Fatal(err)
	}

	st, err := testCP.RunHandoff(ctx, src, branch, "i")
	if err != nil {
		t.Fatalf("handoff independent branch: %v", err)
	}
	if st.Phase != checkpoint.HandoffPhaseDone || len(st.CopiedSSTs) != 0 {
		t.Fatalf("st=%+v", st)
	}
	status, err := testCP.HandoffStatus(ctx, src, branch, "i")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.SourceEntryState != "not-registered" || status.ReferencedAncestorSSTs != 0 {
		t.Fatalf("status=%+v", status)
	}
}

// TestHandoffNotRegisteredAndDuplicate covers the deterministic outcomes
// for an unknown branch and for a repeated decommission.
func TestHandoffNotRegisteredAndDuplicate(t *testing.T) {
	ctx := context.Background()
	src, branch, _, _, _ := forkFixture(t, 1, 200)

	if _, err := testCP.RunHandoff(ctx, src, object.NewMem(), "never-forked"); !errorIs(err, checkpoint.ErrBranchNotRegistered) {
		t.Fatalf("want ErrBranchNotRegistered, got %v", err)
	}

	st, err := testCP.RunHandoff(ctx, src, branch, "b")
	if err != nil {
		t.Fatalf("first handoff: %v", err)
	}
	if st.Phase != checkpoint.HandoffPhaseDone {
		t.Fatalf("phase=%s", st.Phase)
	}
	// Duplicate decommission returns the same outcome without error.
	st2, err := testCP.RunHandoff(ctx, src, branch, "b")
	if err != nil {
		t.Fatalf("duplicate handoff: %v", err)
	}
	if st2.HandoffID != st.HandoffID || st2.Phase != checkpoint.HandoffPhaseDone {
		t.Fatalf("duplicate drifted: %+v vs %+v", st, st2)
	}
}

// TestHandoffGenerationFence: after a completed generation is re-forked,
// a stale retry carrying the old handoff id must not release the new
// registration.
func TestHandoffGenerationFence(t *testing.T) {
	ctx := context.Background()
	src, branch, _, _, ancestorKey := forkFixture(t, 1, 200)
	st, err := testCP.RunHandoff(ctx, src, branch, "b")
	if err != nil {
		t.Fatalf("first handoff: %v", err)
	}
	oldID := st.HandoffID

	// New generation: re-register the same branch id.
	if err := testCP.RegisterBranch(ctx, src, "b", ancestorKey); err != nil {
		t.Fatalf("re-fork: %v", err)
	}
	// Forge a stale active marker from the old generation.
	stale := &checkpoint.HandoffState{
		HandoffID:             oldID,
		BranchID:              "b",
		AncestorCheckpointKey: ancestorKey,
		PinnedCheckpointKey:   mustHeadKey(t, ctx, branch),
		Phase:                 checkpoint.HandoffPhaseSolidified,
	}
	if err := branch.Put(ctx, checkpoint.HandoffActiveKey, jsonReader(stale)); err != nil {
		t.Fatal(err)
	}

	if _, err := testCP.CompleteHandoff(ctx, src, branch, "b"); !errorIs(err, checkpoint.ErrHandoffConflict) {
		t.Fatalf("stale generation completion: want ErrHandoffConflict, got %v", err)
	}
	entry, _, _ := testCP.ReadBranchEntry(ctx, src, "b")
	if entry == nil || entry.HandoffID != "" || entry.Released() {
		t.Fatalf("new registration was touched by the stale handoff: %+v", entry)
	}
}
