package gc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// buildPlan pins everything a run is allowed to act on: the manifest/latest
// snapshot, the retained checkpoints, the SSTs the retained checkpoints
// reference, the branch registrations, the follower-safe WAL horizon and the
// leadership generation. No later change can widen this set; per-phase fact
// checks can only shrink it (items that became protected move to retained).
//
// The checkpoint retention rules mirror checkpoint.Manager.GCCheckpoints
// exactly: the newest keep indexes survive, plus the manifest/latest
// checkpoint and every active branch pin, regardless of age.
func (r *Runner) buildPlan(ctx context.Context) (*Run, error) {
	keep := r.opts.Keep
	if keep < 1 {
		keep = 1
	}

	manifest, err := r.mgr.ReadManifest(ctx, r.store)
	if err != nil {
		return nil, fmt.Errorf("gc: read manifest: %w", err)
	}
	branches, err := r.mgr.ReadBranchEntries(ctx, r.store)
	if err != nil {
		return nil, fmt.Errorf("gc: read branch entries: %w", err)
	}

	keys, err := r.mgr.ListRemote(ctx, r.store)
	if err != nil {
		return nil, fmt.Errorf("gc: list checkpoints: %w", err)
	}

	id, err := newRunID(r.opts.Kind)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()
	run := &Run{
		FormatVersion: stateFormatVersion,
		ID:            id,
		Kind:          r.opts.Kind,
		Term:          r.opts.Term,
		Keep:          keep,
		CreatedNano:   now,
		UpdatedNano:   now,
		Status:        StatusRunning,
		BranchPins:    make(map[string]string),
	}
	if manifest != nil {
		run.Manifest = &ManifestSnapshot{
			CheckpointKey: manifest.CheckpointKey,
			Revision:      manifest.Revision,
			LastSequence:  manifest.LastSequence,
			Term:          manifest.Term,
			LastWALKey:    manifest.LastWALKey,
		}
	}
	for id, entry := range branches {
		if entry.AncestorCheckpointKey != "" {
			run.BranchPins[id] = entry.AncestorCheckpointKey
		}
	}

	pinned := make(map[string]struct{})
	if manifest != nil {
		pinned[manifest.CheckpointKey] = struct{}{}
	}
	for _, k := range run.BranchPins {
		pinned[k] = struct{}{}
	}
	if len(keys) > keep {
		for _, k := range keys[len(keys)-keep:] {
			pinned[k] = struct{}{}
		}
	}

	// Protect SSTs of every retained checkpoint before anything is deleted.
	// A kept checkpoint whose index is unreadable would leave its SSTs
	// unprotected, so planning aborts; an index already gone needs none.
	protectedSSTs := make(map[string]struct{})
	pinnedList := make([]string, 0, len(pinned))
	for k := range pinned {
		pinnedList = append(pinnedList, k)
	}
	sort.Strings(pinnedList)
	run.PinnedCheckpoints = pinnedList
	for _, k := range pinnedList {
		idx, err := r.mgr.ReadCheckpointIndex(ctx, r.store, k)
		if errors.Is(err, object.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("gc: read retained index %q: %w", k, err)
		}
		for _, s := range idx.SSTFiles {
			protectedSSTs[s] = struct{}{}
		}
	}

	// Expiring checkpoints become plan items. Before deleting each one its
	// index is harvested for SST candidates: SSTs no retained checkpoint
	// references, tagged with every expiring checkpoint that named them.
	if len(keys) > keep {
		sstSources := make(map[string][]string)
		for _, k := range keys[:len(keys)-keep] {
			if _, isPinned := pinned[k]; isPinned {
				continue // manifest/latest or a branch points here
			}
			run.Checkpoints = append(run.Checkpoints, &Item{Key: k, Phase: PhasePending, UpdatedNano: now})
			idx, err := r.mgr.ReadCheckpointIndex(ctx, r.store, k)
			if err != nil {
				// Same tolerance as GCCheckpoints: a checkpoint that can no
				// longer be read is deleted anyway; it contributes no
				// candidates.
				continue
			}
			for _, s := range idx.SSTFiles {
				if _, protected := protectedSSTs[s]; protected {
					continue
				}
				sstSources[s] = append(sstSources[s], k)
			}
		}
		sstKeys := make([]string, 0, len(sstSources))
		for s := range sstSources {
			sstKeys = append(sstKeys, s)
		}
		sort.Strings(sstKeys)
		for _, s := range sstKeys {
			sources := sstSources[s]
			sort.Strings(sources)
			run.SSTs = append(run.SSTs, &Item{
				Key:         s,
				Objects:     []string{s},
				Sources:     sources,
				Phase:       PhasePending,
				UpdatedNano: now,
			})
		}
	}

	// WAL phase: pin the follower-safe sequence horizon and the segments it
	// covers. The horizon callback re-evaluates at delete time too; only keys
	// on this pinned list can ever be deleted.
	if r.opts.WALHorizon != nil {
		horizon, ok, err := r.opts.WALHorizon(ctx)
		if err != nil {
			return nil, fmt.Errorf("gc: resolve wal horizon: %w", err)
		}
		if ok {
			run.WALHorizonSeq = horizon
			walKeys, err := wal.DeletableSegments(ctx, r.store, horizon)
			if err != nil {
				return nil, err
			}
			for _, k := range walKeys {
				run.WALs = append(run.WALs, &Item{
					Key:         k,
					Objects:     []string{k},
					Phase:       PhasePending,
					UpdatedNano: now,
				})
			}
		}
	}

	return run, nil
}

// currentPins re-reads the facts that can protect checkpoint directories at
// delete time: the manifest checkpoint and every active branch pin.
func (r *Runner) currentPins(ctx context.Context) (map[string]struct{}, error) {
	pins := make(map[string]struct{})
	manifest, err := r.mgr.ReadManifest(ctx, r.store)
	if err != nil {
		return nil, fmt.Errorf("gc: re-read manifest: %w", err)
	}
	if manifest != nil {
		pins[manifest.CheckpointKey] = struct{}{}
	}
	branches, err := r.mgr.ReadBranchEntries(ctx, r.store)
	if err != nil {
		return nil, fmt.Errorf("gc: re-read branch entries: %w", err)
	}
	for _, entry := range branches {
		if entry.AncestorCheckpointKey != "" {
			pins[entry.AncestorCheckpointKey] = struct{}{}
		}
	}
	return pins, nil
}

// currentReferencedSSTs returns the SST keys every checkpoint currently in
// the store references. It is the delete-time fact check for orphan-SST
// candidates: a new checkpoint (including one a newer leader wrote) keeps the
// SST even though the pinned plan had marked it orphan.
func (r *Runner) currentReferencedSSTs(ctx context.Context) (map[string]struct{}, error) {
	cps, err := r.mgr.ListRemote(ctx, r.store)
	if err != nil {
		return nil, fmt.Errorf("gc: re-list checkpoints: %w", err)
	}
	referenced := make(map[string]struct{})
	for _, k := range cps {
		idx, err := r.mgr.ReadCheckpointIndex(ctx, r.store, k)
		if errors.Is(err, object.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("gc: re-read index %q: %w", k, err)
		}
		for _, s := range idx.SSTFiles {
			referenced[s] = struct{}{}
		}
	}
	return referenced, nil
}

// checkpointPrefix returns the object prefix covering one checkpoint
// directory ("checkpoint/{term}/{rev}/"), matching deleteCheckpoint.
func checkpointPrefix(indexKey string) string {
	return indexKey[:strings.LastIndex(indexKey, "/")+1]
}
