package t4

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/t4db/t4/internal/checkpoint"
)

// branchHandoff is the live branch node's side of the recoverable
// unfork handoff. The decommission itself is driven against the object
// stores (checkpoint.Manager.RunHandoff, e.g. via `t4 branch unfork`),
// but a running branch must cooperate: its checkpoints keep classifying the
// SSTs it inherited at bootstrap as AncestorSSTFiles (they live in the
// source store), so a manifest checkpoint written after unfork could still
// reference SSTs the source is then free to GC.
//
// Cooperation is purely marker-driven — the branch's own object store
// contains either an active or a released handoff marker:
//
//   - beforeCheckpoint, called inside every checkpoint under checkpointMu,
//     promotes inherited SSTs into the local store and registry once a
//     marker is visible, so the checkpoint being written is independent;
//   - afterCheckpoint records that checkpoint as the solidified proof;
//   - watchLoop polls the marker and forces a checkpoint promptly instead
//     of waiting out the checkpoint interval.
//
// The marker lives in THIS node's ObjectStore, so no branch id is needed
// to address it: each branch store belongs to exactly one branch.
type branchHandoff struct {
	node *Node
	// promoted flips once this process has reacted to a marker; every
	// later checkpoint is independent without consulting the stores again.
	promoted atomic.Bool
	// pollInterval may be overridden in tests.
	pollInterval time.Duration
}

// branchNode reports whether this node was started as a source-store fork.
// A non-nil AncestorStore is the signal; BranchPoint without a persistent
// ancestor store (bootstrap-only) also counts until its first own
// checkpoint lands.
func (n *Node) branchNode() bool {
	return n.cfg.ObjectStore != nil &&
		(n.cfg.AncestorStore != nil || n.cfg.BranchPoint != nil)
}

func newBranchHandoff(n *Node) *branchHandoff {
	return &branchHandoff{node: n, pollInterval: branchHandoffPollInterval}
}

// branchHandoffPollInterval bounds how quickly a live branch notices a
// decommission marker. The watcher polls only while the node still
// classifies inherited SSTs as ancestor-owned (that set only shrinks),
// so a branch that has compacted away its inherited data — or one that
// is never decommissioned after that point — pays no polling.
const branchHandoffPollInterval = 10 * time.Second

// activeMarker returns the in-progress handoff marker, if any.
func (b *branchHandoff) activeMarker(ctx context.Context) (*checkpoint.HandoffState, error) {
	return b.node.cp.ReadActiveHandoff(ctx, b.node.cfg.ObjectStore)
}

// marker returns the current handoff marker from the branch store, preferring
// the active one and otherwise the released one. nil, nil means no
// handoff concerns this node. Called on the checkpoint path (bounded by
// the checkpoint interval), not on the watcher's tight loop.
func (b *branchHandoff) marker(ctx context.Context) (*checkpoint.HandoffState, error) {
	st, err := b.node.cp.ReadActiveHandoff(ctx, b.node.cfg.ObjectStore)
	if err != nil {
		return nil, err
	}
	if st != nil {
		return st, nil
	}
	return b.node.cp.ReadReleasedHandoff(ctx, b.node.cfg.ObjectStore)
}

// beforeCheckpoint promotes inherited SSTs when a handoff marker is
// visible. Returns promoted=true when the checkpoint about to be written was
// made independent by this call. Object-store errors fail the checkpoint
// attempt — a marker read that failed must never be treated as "no handoff".
func (b *branchHandoff) beforeCheckpoint(ctx context.Context) (bool, error) {
	if b.promoted.Load() {
		return false, nil
	}
	st, err := b.marker(ctx)
	if err != nil {
		return false, err
	}
	if st == nil {
		return false, nil
	}
	n := b.node
	if n.sstUploader != nil && n.sstUploader.HasInherited() {
		promoted, perr := n.sstUploader.PromoteInherited(ctx)
		if perr != nil {
			return false, perr
		}
		n.log.Infof("t4: branch handoff %s: promoted %d inherited SST(s) into the local store",
			st.HandoffID, promoted)
	}
	b.promoted.Store(true)
	return true, nil
}

// afterCheckpoint publishes the freshly written checkpoint as the handoff's
// solidification proof. Errors are non-fatal: the decommission driver
// independently solidifies and verifies the head checkpoint; this only
// saves it a round trip.
func (b *branchHandoff) afterCheckpoint(ctx context.Context, term uint64, revision int64) {
	key := checkpoint.CheckpointIndexKey(term, revision)
	if err := b.node.cp.MarkHandoffSolidified(ctx, b.node.cfg.ObjectStore, key, term, revision); err != nil {
		b.node.log.Warnf("t4: branch handoff: announce independent checkpoint rev=%d: %v", revision, err)
	}
}

// watchLoop forces a checkpoint promptly once an active handoff marker
// appears, so decommission does not have to wait for the (potentially
// long) checkpoint interval. It runs only while the node still has
// inherited SSTs that need promoting — that set only shrinks — and
// exits once promotion happened, so branches with nothing left to
// promote (including branches never decommissioned) pay nothing here.
func (b *branchHandoff) watchLoop(ctx context.Context) {
	interval := b.pollInterval
	if interval <= 0 {
		interval = branchHandoffPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if b.promoted.Load() {
				return
			}
			if b.node.sstUploader == nil || !b.node.sstUploader.HasInherited() {
				// No ancestor-owned SSTs left to promote; the set
				// never grows again, so no marker can matter anymore.
				return
			}
			st, err := b.activeMarker(ctx)
			if err != nil || st == nil {
				continue
			}
			b.node.forceCheckpoint(ctx)
		}
	}
}
