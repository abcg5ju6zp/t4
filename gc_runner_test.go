package t4

import (
	"context"
	"fmt"
	"testing"

	"github.com/t4db/t4/internal/gc"
	"github.com/t4db/t4/pkg/object"
)

// TestAutomaticGCRunsJournaledToCompletion pins the leader/single-node
// wiring: the automatic GC after a checkpoint runs through the resumable
// runner, deletes checkpoint archives beyond keep=2 in reference order, and
// leaves a queryable successful result with no active journal behind.
func TestAutomaticGCRunsJournaledToCompletion(t *testing.T) {
	store := object.NewMem()
	n := openCheckpointTestNode(t, store)
	ctx := context.Background()

	for i := range 4 {
		if _, err := n.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
		n.maybeCheckpoint(ctx)
	}

	cps, err := n.cp.ListRemote(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(cps) != 2 {
		t.Fatalf("checkpoints retained=%d, want 2: %v", len(cps), cps)
	}

	active, err := gc.ReadActive(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if active != nil {
		t.Fatalf("successful run left an active journal: %s (%s)", active.ID, active.Status)
	}
	latest, err := gc.ReadLatest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil {
		t.Fatal("successful automatic GC left no queryable result")
	}
	if latest.Kind != gc.KindAuto {
		t.Fatalf("result kind=%s want auto", latest.Kind)
	}
	if latest.Status != gc.StatusSucceeded {
		t.Fatalf("result status=%s: %s", latest.Status, latest.Error)
	}
	if c := latest.Counts(); c.CheckpointsDeleted < 1 {
		t.Fatalf("last run checkpoints deleted=%d, want >=1", c.CheckpointsDeleted)
	}
}

// TestAutomaticGCNoCandidatesLeavesNoJournal pins the parity rule: a run
// with no candidates anywhere writes no GC journal objects.
func TestAutomaticGCNoCandidatesLeavesNoJournal(t *testing.T) {
	store := object.NewMem()
	n := openCheckpointTestNode(t, store)
	ctx := context.Background()

	if _, err := n.Put(ctx, "only", []byte("v"), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	n.maybeCheckpoint(ctx) // one checkpoint: nothing old enough to collect

	keys, err := store.List(ctx, "gc/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("no-candidate GC wrote journal objects: %v", keys)
	}
}
