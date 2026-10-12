package t4

import (
	"context"
	"fmt"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

// Fork registers a new branch in sourceStore under branchID, pinning the
// latest checkpoint so GC will not delete its SST files. It returns the
// checkpoint key to use as BranchPoint.CheckpointKey when starting the branch
// node.
//
// The branch is forked from the latest committed checkpoint revision in
// sourceStore. If you need a branch at an older revision, pass that
// checkpoint's index key directly to BranchPoint.CheckpointKey and call
// checkpoint.RegisterBranch yourself.
//
// Call Fork before starting the branch node. When the branch is decommissioned,
// call Unfork: it runs a recoverable handoff that makes the branch
// independent of the source store's SSTs before releasing the registration,
// so a live branch (or its restart) can never outlive the SSTs its
// checkpoints still reference.
func Fork(ctx context.Context, sourceStore object.Store, branchID string) (checkpointKey string, err error) {
	cp := checkpoint.New(defaultLogger())
	manifest, err := cp.ReadManifest(ctx, sourceStore)
	if err != nil {
		return "", fmt.Errorf("fork: read source manifest: %w", err)
	}
	if manifest == nil {
		return "", fmt.Errorf("fork: no checkpoint found in source store")
	}
	if err := cp.RegisterBranch(ctx, sourceStore, branchID, manifest.CheckpointKey); err != nil {
		return "", fmt.Errorf("fork: register branch: %w", err)
	}
	return manifest.CheckpointKey, nil
}

// BranchHandoff is the durable, queryable state of a branch decommission.
type BranchHandoff struct {
	// BranchID is the source-store registration being retired.
	BranchID string
	// HandoffID is the fixed generation identifier of the handoff.
	// Retries reuse it; a different id means a different generation.
	HandoffID string
	// Phase is "solidifying", "solidified" or "done".
	Phase string
	// SourceEntryState is "active", "solidifying", "released" or
	// "not-registered".
	SourceEntryState string

	AncestorCheckpointKey   string
	PinnedCheckpointKey       string
	IndependentCheckpointKey string
	// CopiedSSTs is how many ancestor SSTs have already been made
	// branch-local; ReferencedAncestorSSTs is how many the current head
	// still references.
	CopiedSSTs             int
	ReferencedAncestorSSTs  int
}

func mapHandoff(s *checkpoint.HandoffStatus) *BranchHandoff {
	return &BranchHandoff{
		BranchID:                 s.BranchID,
		HandoffID:               s.HandoffID,
		Phase:                    s.Phase,
		SourceEntryState:         s.SourceEntryState,
		AncestorCheckpointKey:   s.AncestorCheckpointKey,
		PinnedCheckpointKey:       s.PinnedCheckpointKey,
		IndependentCheckpointKey: s.IndependentCheckpointKey,
		CopiedSSTs:              s.CopiedSSTs,
		ReferencedAncestorSSTs:  s.ReferencedAncestorSSTs,
	}
}

// Unfork retires a branch through a recoverable handoff:
//
//  1. it pins the branch identity (branchID + a random handoff id), the
//     ancestor checkpoint, and the branch's current checkpoint revision;
//  2. the ancestor SSTs the branch head still references are copied into
//     branchStore as its own objects, and a verifiable, fully-local
//     checkpoint is published as the branch manifest head;
//  3. only after that checkpoint verifies (no ancestor references, every
//     SST and metadata object present in branchStore) is the source
//     registration released, after which source GC may reclaim.
//
// A live branch node writing into branchStore cooperates automatically: it
// sees the handoff marker and promotes its inherited SSTs at its next
// checkpoint, so checkpoints written afterwards stay independent.
//
// Unfork is idempotent and safe to retry after any interruption (object
// copy, checkpoint publish, or registration update): the retry resumes the
// same handoff generation instead of mixing states with a newer fork.
// Duplicate decommission and an already-independent branch return success
// with the existing result; the source entry is never half-released.
//
// sourceStore is the store Fork registered the branch in; branchStore is
// the branch node's own object store.
func Unfork(ctx context.Context, sourceStore, branchStore object.Store, branchID string) (*BranchHandoff, error) {
	if sourceStore == nil {
		return nil, fmt.Errorf("unfork: source store is required")
	}
	if branchStore == nil {
		return nil, fmt.Errorf("unfork: branch store is required (the branch needs a place to publish its independent checkpoint)")
	}
	cp := checkpoint.New(defaultLogger())
	st, err := cp.RunHandoff(ctx, sourceStore, branchStore, branchID)
	if err != nil {
		return nil, fmt.Errorf("unfork %q: %w", branchID, err)
	}
	status, err := cp.HandoffStatus(ctx, sourceStore, branchStore, branchID)
	if err != nil {
		return nil, fmt.Errorf("unfork %q: read status: %w", branchID, err)
	}
	result := mapHandoff(status)
	if result.Phase == "" && st != nil {
		result.Phase = st.Phase
		if result.HandoffID == "" {
			result.HandoffID = st.HandoffID
		}
		if result.IndependentCheckpointKey == "" {
			result.IndependentCheckpointKey = st.IndependentCheckpointKey
		}
	}
	return result, nil
}

// UnforkStatus returns the current decommission state for branchID without
// modifying either store. Use it to inspect a handoff whose driver
// crashed mid-phase.
func UnforkStatus(ctx context.Context, sourceStore, branchStore object.Store, branchID string) (*BranchHandoff, error) {
	st, err := checkpoint.New(defaultLogger()).HandoffStatus(ctx, sourceStore, branchStore, branchID)
	if err != nil {
		return nil, fmt.Errorf("unfork status %q: %w", branchID, err)
	}
	return mapHandoff(st), nil
}
