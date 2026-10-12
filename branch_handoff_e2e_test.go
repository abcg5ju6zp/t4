package t4

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pebblelib "github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

// openBranchNode opens a single-node branch bootstrapped from srcCPKey
// in srcStore, writing its own checkpoints to branchStore.
func openBranchNode(t *testing.T, dir string, srcStore, branchStore object.Store, srcCPKey string) *Node {
	t.Helper()
	walSync := false
	n, err := Open(Config{
		DataDir:            dir,
		ObjectStore:        branchStore,
		AncestorStore:      srcStore,
		BranchPoint:        &BranchPoint{SourceStore: srcStore, CheckpointKey: srcCPKey},
		WALSyncUpload:      &walSync,
		CheckpointInterval: time.Hour,
		Logger:             NoopLogger,
	})
	if err != nil {
		t.Fatalf("open branch: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func headIndex(t *testing.T, ctx context.Context, cp *checkpoint.Manager, store object.Store) *checkpoint.CheckpointIndex {
	t.Helper()
	m, err := cp.ReadManifest(ctx, store)
	if err != nil || m == nil {
		t.Fatalf("read manifest: %v", err)
	}
	idx, err := cp.ReadCheckpointIndex(ctx, store, m.CheckpointKey)
	if err != nil {
		t.Fatalf("read head index: %v", err)
	}
	return idx
}

// TestUnforkHandoffEndToEnd covers the full operational story:
//
//   - a running branch node whose checkpoints reference ancestor SSTs;
//   - Unfork copying/verifying and releasing the source registration;
//   - the branch continuing to write afterwards without ever referencing the
//     ancestor store again;
//   - source GC reclaiming the ancestor checkpoint/SSTs while a
//     branch-store-only restore still works;
//   - a branch restart staying independent;
//   - duplicate decommission returning success deterministically.
func TestUnforkHandoffEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srcStore := object.NewMem()
	branchStore := object.NewMem()

	// Source node with a few checkpoints.
	src := openCheckpointTestNode(t, srcStore)
	for i := 0; i < 3; i++ {
		if _, err := src.Put(ctx, "/src/k", []byte("v0"), 0); err != nil {
			t.Fatal(err)
		}
		src.maybeCheckpoint(ctx)
	}
	cpKey, err := Fork(ctx, srcStore, "b")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}

	// Branch node: bootstrap from the fork point and write one checkpoint.
	branchDir := filepath.Join(t.TempDir(), "branch")
	b := openBranchNode(t, branchDir, srcStore, branchStore, cpKey)
	if _, err := b.Put(ctx, "/branch/k", []byte("bv1"), 0); err != nil {
		t.Fatalf("branch put: %v", err)
	}
	b.maybeCheckpoint(ctx)
	cp := checkpoint.New(NoopLogger)
	idx := headIndex(t, ctx, cp, branchStore)
	if len(idx.AncestorSSTFiles) == 0 {
		t.Fatalf("precondition: branch checkpoint references no ancestor SSTs")
	}

	// Run the decommission while the branch keeps serving. The node's
	// marker watcher may promote on its own; the store-driven path and
	// the node path converge on an independent head either way.
	st, err := Unfork(ctx, srcStore, branchStore, "b")
	if err != nil {
		t.Fatalf("Unfork: %v", err)
	}
	if st.Phase != "done" || st.SourceEntryState != "not-registered" {
		t.Fatalf("unfork result: %+v", st)
	}
	idx = headIndex(t, ctx, cp, branchStore)
	if len(idx.AncestorSSTFiles) != 0 {
		t.Fatalf("branch head still references %d ancestor SSTs", len(idx.AncestorSSTFiles))
	}

	// Source GC advances and reclaims the fork checkpoint and its SSTs.
	for i := 0; i < 3; i++ {
		if _, err := src.Put(ctx, "/src/new", []byte("vn"), 0); err != nil {
			t.Fatal(err)
		}
		src.maybeCheckpoint(ctx)
	}
	deleted, candidates, err := cp.GCCheckpoints(ctx, srcStore, 1)
	if err != nil {
		t.Fatalf("source GC: %v", err)
	}
	if deleted == 0 {
		t.Fatalf("fork checkpoint never became GC-eligible after unfork")
	}
	if entries, _ := cp.ReadBranchEntries(ctx, srcStore); len(entries) != 0 {
		t.Fatalf("branch entries: %v", entries)
	}
	if _, err := cp.GCOrphanSSTs(ctx, srcStore, candidates); err != nil {
		t.Fatalf("orphan sst gc: %v", err)
	}

	// The branch head restores entirely from its own store.
	restoreAndCheck(t, ctx, cp, branchStore, "/branch/k")

	// Branch keeps writing: its next checkpoint stays independent (the
	// node promotes inherited SSTs when it sees the handoff marker).
	if _, err := b.Put(ctx, "/branch/k2", []byte("bv2"), 0); err != nil {
		t.Fatalf("branch put 2: %v", err)
	}
	b.maybeCheckpoint(ctx)
	idx = headIndex(t, ctx, cp, branchStore)
	if len(idx.AncestorSSTFiles) != 0 {
		t.Fatalf("post-unfork branch checkpoint reintroduced ancestor refs: %d", len(idx.AncestorSSTFiles))
	}
	restoreAndCheck(t, ctx, cp, branchStore, "/branch/k2")

	// Restart the branch from its local data; checkpoints stay local.
	if err := b.Close(); err != nil {
		t.Fatalf("close branch: %v", err)
	}
	b2 := openBranchNode(t, branchDir, srcStore, branchStore, cpKey)
	if _, err := b2.Put(ctx, "/branch/k3", []byte("bv3"), 0); err != nil {
		t.Fatalf("branch put 3: %v", err)
	}
	b2.maybeCheckpoint(ctx)
	idx = headIndex(t, ctx, cp, branchStore)
	if len(idx.AncestorSSTFiles) != 0 {
		t.Fatalf("restarted branch checkpoint references ancestors: %d", len(idx.AncestorSSTFiles))
	}
	restoreAndCheck(t, ctx, cp, branchStore, "/branch/k3")
}

// TestUnforkDuplicateIsIdempotent: a second Unfork after completion
// returns done without error.
func TestUnforkDuplicateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	srcStore := object.NewMem()
	branchStore := object.NewMem()

	src := openCheckpointTestNode(t, srcStore)
	if _, err := src.Put(ctx, "/k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	src.maybeCheckpoint(ctx)
	cpKey, err := Fork(ctx, srcStore, "dup")
	if err != nil {
		t.Fatal(err)
	}
	b := openBranchNode(t, filepath.Join(t.TempDir(), "dup"), srcStore, branchStore, cpKey)
	if _, err := b.Put(ctx, "/bk", []byte("bv"), 0); err != nil {
		t.Fatal(err)
	}
	b.maybeCheckpoint(ctx)

	first, err := Unfork(ctx, srcStore, branchStore, "dup")
	if err != nil {
		t.Fatalf("first unfork: %v", err)
	}
	again, err := Unfork(ctx, srcStore, branchStore, "dup")
	if err != nil {
		t.Fatalf("duplicate unfork: %v", err)
	}
	if again.HandoffID != first.HandoffID || again.Phase != "done" {
		t.Fatalf("duplicate drifted: %+v vs %+v", first, again)
	}
}

// TestUnforkWithoutBranchStore rejects the incomplete call explicitly
// rather than deleting the registration outright.
func TestUnforkWithoutBranchStore(t *testing.T) {
	if _, err := Unfork(context.Background(), object.NewMem(), nil, "b"); err == nil {
		t.Fatalf("Unfork without branch store must fail")
	}
}

// restoreAndCheck restores the branch manifest head into a fresh Pebble
// directory using ONLY the branch store and asserts the user key is present
// in the index keyspace ('i' + key; the user value lives in the
// revision log).
func restoreAndCheck(t *testing.T, ctx context.Context, cp *checkpoint.Manager, store object.Store, key string) {
	t.Helper()
	m, err := cp.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if _, _, err := cp.Restore(ctx, store, m.CheckpointKey, target); err != nil {
		t.Fatalf("restore: %v", err)
	}
	db, err := pebblelib.Open(target, &pebblelib.Options{})
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer db.Close()
	idxKey := append([]byte{'i'}, []byte(key)...)
	if _, closer, err := db.Get(idxKey); err != nil {
		t.Fatalf("index key %q missing after branch-store-only restore: %v", key, err)
	} else {
		_ = closer.Close()
	}
}
