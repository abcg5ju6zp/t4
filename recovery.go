package t4

// Automatic recovery from a lost/corrupt manifest/latest.
//
// When manifest/latest is missing, truncated, or points at an incomplete
// checkpoint, first boot cannot use the normal one-GET bootstrap path. Instead
// of requiring manual object-store surgery, the node scans every checkpoint
// index, verifies each candidate's index, SST objects, Pebble metadata and
// coverage, picks the highest-revision complete candidate, stages it as a
// single-checkpoint directory (never mixing files across checkpoints), then
// lets the normal remote-WAL replay extend it contiguously. manifest/latest is
// repaired with a conditional write only after the replay converges.
//
// Interruptions are safe: the recovery state file below persists the chosen
// candidate and every confirmed per-file hash. A retry is pinned to the same
// candidate and trusts only local files whose hashes still match, so a process
// restart never silently changes the recovery baseline. If the pinned
// candidate has lost objects, recovery fails explicitly — the node never
// publishes a half-recovered directory or a manifest pointing at one.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

// recoveryStateName is the file persisted under DataDir while an automatic
// recovery is in flight. Its existence is authoritative: while it is present,
// bootstrap is pinned to the recorded candidate.
const recoveryStateName = "recovery-state.json"

// recoveryStagingSuffix is the directory (alongside "db") holding the verified
// candidate files before the atomic rename into the canonical Pebble path.
const recoveryStagingSuffix = ".recover"

// recoveryBaselineTimeout bounds the scan/download phase on first boot. It is
// deliberately more generous than the normal single-checkpoint restore:
// recovery may validate and skip several candidates against a slow store.
const recoveryBaselineTimeout = 10 * time.Minute

type recoveryState struct {
	IndexKey      string                             `json:"index_key"`
	Term          uint64                             `json:"term"`
	Revision      int64                              `json:"revision"`
	LastSequence  int64                              `json:"last_sequence"`
	FormatVersion uint32                             `json:"format_version"`
	Files         map[string]checkpoint.VerifiedFile `json:"files"`
	Staged        bool                               `json:"staged"`
}

func recoveryStatePath(dataDir string) string {
	return filepath.Join(dataDir, recoveryStateName)
}

func loadRecoveryState(dataDir string) (*recoveryState, error) {
	b, err := os.ReadFile(recoveryStatePath(dataDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("t4: read recovery state: %w", err)
	}
	var s recoveryState
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("t4: decode recovery state %s: %w", recoveryStatePath(dataDir), err)
	}
	if s.IndexKey == "" {
		return nil, fmt.Errorf("t4: recovery state %s has no checkpoint key", recoveryStatePath(dataDir))
	}
	return &s, nil
}

// save atomically writes the state so a crash can never observe a truncated
// state file.
func (s *recoveryState) save(dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("t4: persist recovery state: %w", err)
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	path := recoveryStatePath(dataDir)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("t4: persist recovery state: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("t4: persist recovery state: %w", err)
	}
	return nil
}

func (s *recoveryState) remove(dataDir string) error {
	err := os.Remove(recoveryStatePath(dataDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *recoveryState) candidate() *checkpoint.RecoveryCandidate {
	idx := &checkpoint.CheckpointIndex{
		FormatVersion: s.FormatVersion,
		Term:          s.Term,
		Revision:      s.Revision,
		LastSequence:  s.LastSequence,
	}
	return &checkpoint.RecoveryCandidate{
		IndexKey: s.IndexKey,
		Index:    idx,
		Files:    s.Files,
	}
}

func stateFromCandidate(c *checkpoint.RecoveryCandidate) *recoveryState {
	return &recoveryState{
		IndexKey:      c.IndexKey,
		Term:          c.Index.Term,
		Revision:      c.Index.Revision,
		LastSequence:  c.Index.LastSequence,
		FormatVersion: c.Index.FormatVersion,
		Files:         c.Files,
	}
}

// verifyManifestTargetFully validates the complete evidence of the checkpoint
// a manifest points at (index, every SST/metadata object, Pebble coverage).
// Used to decide whether a concurrently published higher manifest is valid and
// must not be overwritten.
func verifyManifestTargetFully(ctx context.Context, cfg *Config, cp *checkpoint.Manager, m *checkpoint.Manifest) error {
	if err := cp.VerifyCheckpointComplete(ctx, cfg.ObjectStore, cfg.AncestorStore, m.CheckpointKey); err != nil {
		return fmt.Errorf("manifest target %q is not a complete checkpoint: %w", m.CheckpointKey, err)
	}
	return nil
}

// bootstrapCheckpointBaseline restores a checkpoint into pebbleDir before
// Pebble is opened. It returns:
//
//   - (nil, nil): normal bootstrap — either local Pebble already exists, or the
//     existing manifest-led restore succeeded, or the store is genuinely empty
//     (fresh cluster, no manifest and no checkpoints);
//   - (state, nil): an automatic recovery staged a verified candidate; callers
//     must open Pebble, replay remote WAL contiguously, and finish with
//     completeManifestRecovery before serving;
//   - (nil, err): evidence is incomplete or unusable — fail explicitly.
//
// RestorePoint/BranchPoint bootstraps bypass this function entirely and keep
// their pinned semantics.
func bootstrapCheckpointBaseline(ctx context.Context, cfg *Config, cp *checkpoint.Manager, pebbleDir string, log Logger) (*recoveryState, error) {
	store := cfg.ObjectStore

	state, err := loadRecoveryState(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	// Resume an interrupted recovery. The candidate is pinned: retrying with a
	// different checkpoint could splice two checkpoints' files together.
	if state != nil {
		log.Infof("t4: resuming interrupted checkpoint recovery for %s (term=%d rev=%d seq=%d)",
			state.IndexKey, state.Term, state.Revision, state.LastSequence)
		stagingDir := pebbleDir + recoveryStagingSuffix
		if state.Staged {
			if _, statErr := os.Stat(pebbleDir); statErr != nil {
				return nil, fmt.Errorf("t4: recovery state records a staged checkpoint but %q is gone: %w", pebbleDir, statErr)
			}
		} else {
			cand, _, verr := cp.SelectCandidate(ctx, store, cfg.AncestorStore, stagingDir, state.IndexKey, state.candidate())
			if verr != nil {
				return nil, fmt.Errorf("t4: resume checkpoint recovery: %w", verr)
			}
			state.Files = cand.Files
			if serr := state.save(cfg.DataDir); serr != nil {
				return nil, serr
			}
			if rerr := promoteStaging(stagingDir, pebbleDir); rerr != nil {
				return nil, rerr
			}
			state.Staged = true
			if serr := state.save(cfg.DataDir); serr != nil {
				return nil, serr
			}
		}
		return state, nil
	}

	status, err := cp.ReadManifestStatus(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("t4: read manifest: %w", err)
	}

	_, statErr := os.Stat(pebbleDir)
	pebbleExists := statErr == nil

	if status.Manifest != nil {
		if pebbleExists {
			return nil, nil // normal restart: local data is authoritative
		}
		// Preserve the existing manifest-led bootstrap, including the GC-race
		// retry loop for a checkpoint deleted between GET and download.
		manifest := status.Manifest
		var restoreErr error
		for attempt := 0; ; attempt++ {
			_, _, rerr := cp.Restore(ctx, store, manifest.CheckpointKey, pebbleDir)
			if rerr == nil {
				// Restore only downloads what the index names; a manifest that
				// parses can still point at a truncated or internally corrupt
				// checkpoint. Verify before trusting the directory.
				verr := cp.VerifyRestoredDirectory(pebbleDir)
				if verr == nil {
					log.Infof("t4: checkpoint restored (term=%d rev=%d)", manifest.Term, manifest.Revision)
					return nil, nil
				}
				restoreErr = verr
				_ = os.RemoveAll(pebbleDir)
				break
			}
			restoreErr = rerr
			if !errors.Is(rerr, object.ErrNotFound) || attempt >= 4 {
				break
			}
			log.Warnf("t4: checkpoint %q not found, re-reading manifest (attempt %d/5)",
				manifest.CheckpointKey, attempt+1)
			time.Sleep(500 * time.Millisecond)
			reread, merr := cp.ReadManifestStatus(ctx, store)
			if merr != nil {
				return nil, fmt.Errorf("t4: read manifest: %w", merr)
			}
			if reread.Missing || reread.Corrupt || reread.Manifest == nil {
				// Manifest changed shape mid-retry: fall into scan recovery.
				// Discard the partial directory from the failed restore.
				_ = os.RemoveAll(pebbleDir)
				return scanAndRecover(ctx, cfg, cp, pebbleDir, log,
					[]checkpoint.CandidateRejection{{IndexKey: manifest.CheckpointKey, Reason: rerr.Error()}})
			}
			manifest = reread.Manifest
		}
		// The manifest-led restore could not produce a usable directory:
		// scan all checkpoints for complete evidence instead of failing boot.
		log.Warnf("t4: manifest checkpoint %q is unusable (%v); scanning for a complete recovery candidate",
			manifest.CheckpointKey, restoreErr)
		_ = os.RemoveAll(pebbleDir)
		return scanAndRecover(ctx, cfg, cp, pebbleDir, log,
			[]checkpoint.CandidateRejection{{IndexKey: manifest.CheckpointKey, Reason: restoreErr.Error()}})
	}

	if pebbleExists {
		// Local data with no/corrupt manifest: keep prior behaviour. Missing
		// manifest simply means nothing to restore; corrupt manifest has
		// historically been a hard error and local data alone cannot prove
		// which object-store revision it belongs to.
		if status.Corrupt {
			return nil, fmt.Errorf("t4: read manifest: %s", status.Reason)
		}
		return nil, nil
	}

	// First boot with manifest missing or corrupt: look for complete
	// checkpoints. A genuinely empty store is a fresh cluster.
	var prior []checkpoint.CandidateRejection
	if status.Corrupt {
		prior = append(prior, checkpoint.CandidateRejection{IndexKey: checkpoint.ManifestKey, Reason: status.Reason})
	}
	return scanAndRecover(ctx, cfg, cp, pebbleDir, log, prior)
}

// scanAndRecover selects the highest-revision complete checkpoint, stages and
// verifies it, and persists the recovery state before atomically placing the
// directory at pebbleDir. With no prior failure and no candidate at all it
// returns (nil, nil) so a brand-new cluster still boots.
func scanAndRecover(ctx context.Context, cfg *Config, cp *checkpoint.Manager, pebbleDir string, log Logger, prior []checkpoint.CandidateRejection) (*recoveryState, error) {
	store := cfg.ObjectStore
	stagingDir := pebbleDir + recoveryStagingSuffix
	_ = os.RemoveAll(stagingDir)

	cand, rejections, err := cp.RecoverLatest(ctx, store, cfg.AncestorStore, stagingDir)
	if err != nil {
		return nil, fmt.Errorf("t4: scan checkpoints for recovery: %w", err)
	}
	if cand == nil {
		if len(prior) == 0 {
			log.Infof("t4: no manifest and no checkpoints in object storage — starting as a fresh cluster")
			return nil, nil
		}
		return nil, fmt.Errorf("t4: cannot recover: manifest/latest is unusable and no complete checkpoint exists (%s)",
			checkpoint.RejectionsString(append(prior, rejections...)))
	}
	for _, rj := range rejections {
		log.Warnf("t4: recovery skipped %s: %s", rj.IndexKey, rj.Reason)
	}
	log.Infof("t4: recovery selected checkpoint %s (term=%d rev=%d seq=%d) from %d candidate(s)",
		cand.IndexKey, cand.Index.Term, cand.Index.Revision, cand.Index.LastSequence, 1+len(rejections))

	// Persist the pinned candidate and all confirmed facts *before* the
	// directory becomes visible at pebbleDir: any crash past this point resumes
	// against the same evidence.
	state := stateFromCandidate(cand)
	if err := state.save(cfg.DataDir); err != nil {
		return nil, err
	}
	if err := promoteStaging(stagingDir, pebbleDir); err != nil {
		return nil, err
	}
	state.Staged = true
	if err := state.save(cfg.DataDir); err != nil {
		return nil, err
	}
	return state, nil
}

// promoteStaging atomically moves the verified staging directory into the
// canonical Pebble path, which must not already exist.
func promoteStaging(stagingDir, pebbleDir string) error {
	if _, err := os.Stat(pebbleDir); err == nil {
		return fmt.Errorf("t4: cannot promote recovered checkpoint: %q already exists", pebbleDir)
	}
	if err := os.Rename(stagingDir, pebbleDir); err != nil {
		return fmt.Errorf("t4: promote recovered checkpoint: %w", err)
	}
	return nil
}

// completeManifestRecovery publishes the repaired manifest/latest after the
// recovered checkpoint and contiguous WAL replay have converged. The write is
// conditional: if a newer valid manifest appeared meanwhile (for example a
// peer leader healed it), the repair leaves it untouched. The recovery state
// file is removed only after the manifest question is settled, so a crash
// before this point simply retries — idempotently — on the next boot.
func completeManifestRecovery(ctx context.Context, cfg *Config, cp *checkpoint.Manager, log Logger, state *recoveryState) error {
	store := cfg.ObjectStore
	cand := state.candidate()
	m := cand.Manifest()
	// A manifest only counts as "valid" for the don't-overwrite decision when
	// its checkpoint's full evidence verifies — an index that merely exists
	// may point at an incomplete checkpoint (the exact failure mode being
	// repaired). The strong check is reached only when a newer manifest was
	// actually observed; when manifest/latest is missing or corrupt the
	// position check fails first and nothing is downloaded.
	acceptable := func(cur *checkpoint.Manifest) error {
		return verifyManifestTargetFully(ctx, cfg, cp, cur)
	}

	// Fast convergence: a peer (or the GC-gap re-restore path) may already
	// have published a valid manifest at least as new as ours. Nothing to
	// repair — adopt it and drop the recovery state.
	atLeast, err := cp.ManifestAtLeast(ctx, store, m, acceptable)
	if err != nil {
		return err
	}
	if atLeast {
		log.Infof("t4: manifest/latest already points to a valid checkpoint at least as new as %s; left untouched",
			state.IndexKey)
		if err := state.remove(cfg.DataDir); err != nil {
			log.Warnf("t4: could not remove recovery state file: %v", err)
		}
		return nil
	}

	// We are about to publish: re-confirm the evidence is still complete in
	// object storage. A concurrent GC that evicted the pinned candidate means
	// there is no single-revision manifest to publish, so fail explicitly and
	// leave the state for a pinned retry rather than resurrect a broken
	// pointer.
	if err := cp.CheckCandidateObjectsPresent(ctx, store, cfg.AncestorStore, cand); err != nil {
		return fmt.Errorf("recovery candidate %s no longer complete: %w", state.IndexKey, err)
	}

	outcome, err := cp.RepairManifest(ctx, store, m, acceptable)
	if err != nil {
		return err
	}
	switch {
	case outcome.Written:
		log.Infof("t4: manifest/latest repaired to %s (term=%d rev=%d seq=%d)",
			state.IndexKey, state.Term, state.Revision, state.LastSequence)
	case outcome.AlreadyCurrent:
		log.Infof("t4: manifest/latest already points to a valid checkpoint at least as new as %s; left untouched",
			state.IndexKey)
	}
	if err := state.remove(cfg.DataDir); err != nil {
		// Cosmetic: the next boot would resume, find the manifest valid, and
		// remove the file then. Do not fail recovery over it.
		log.Warnf("t4: could not remove recovery state file: %v", err)
	}
	return nil
}
