package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/pkg/object"
)

// ── Manifest status ──────────────────────────────────────────────────────────

// ManifestStatus classifies the manifest/latest object at startup.
type ManifestStatus struct {
	// Manifest is the parsed manifest when the object exists and is decodable.
	// Nil when Missing or Corrupt.
	Manifest *Manifest
	// Missing is true when manifest/latest does not exist.
	Missing bool
	// Corrupt is true when manifest/latest exists but cannot be decoded or
	// uses a format this binary does not understand.
	Corrupt bool
	// Reason describes why the manifest is Corrupt, for diagnostics.
	Reason string
}

// ReadManifestStatus reads manifest/latest and classifies it as present,
// missing, or corrupt (truncated, invalid JSON, unknown future format).
// Infrastructure errors (transport, permissions) are returned as errors.
func (mgr *Manager) ReadManifestStatus(ctx context.Context, store object.Store) (*ManifestStatus, error) {
	rc, err := store.Get(ctx, ManifestKey)
	if errors.Is(err, object.ErrNotFound) {
		return &ManifestStatus{Missing: true}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read manifest: %w", err)
	}
	defer rc.Close()

	var m Manifest
	if decErr := json.NewDecoder(rc).Decode(&m); decErr != nil {
		return &ManifestStatus{
			Corrupt: true,
			Reason:  fmt.Sprintf("manifest/latest is not decodable: %v", decErr),
		}, nil
	}
	if m.FormatVersion > CheckpointFormatVersion {
		return &ManifestStatus{
			Corrupt: true,
			Reason: fmt.Sprintf("manifest/latest format_version=%d is newer than supported format_version=%d",
				m.FormatVersion, CheckpointFormatVersion),
		}, nil
	}
	if m.LastSequence == 0 {
		m.LastSequence = m.Revision
	}
	return &ManifestStatus{Manifest: &m}, nil
}

// ValidateManifestTarget reports whether m points at a checkpoint that can
// plausibly be booted from: its index exists, decodes, uses a supported
// format, and is consistent with its object key. It does not download the
// checkpoint's SST or metadata objects; callers that need full content
// verification use VerifyCandidate.
func (mgr *Manager) ValidateManifestTarget(ctx context.Context, store object.Store, m *Manifest) error {
	if m == nil {
		return errors.New("manifest is nil")
	}
	if m.FormatVersion > CheckpointFormatVersion {
		return fmt.Errorf("manifest format_version=%d is newer than supported %d", m.FormatVersion, CheckpointFormatVersion)
	}
	if !strings.HasSuffix(m.CheckpointKey, "/manifest.json") {
		return fmt.Errorf("manifest checkpoint_key %q is not a checkpoint index key", m.CheckpointKey)
	}
	idx, err := mgr.ReadCheckpointIndex(ctx, store, m.CheckpointKey)
	if err != nil {
		return fmt.Errorf("manifest points to an unreadable checkpoint %q: %w", m.CheckpointKey, err)
	}
	if CheckpointIndexKey(idx.Term, idx.Revision) != m.CheckpointKey {
		return fmt.Errorf("manifest points to checkpoint %q whose index records term=%d revision=%d",
			m.CheckpointKey, idx.Term, idx.Revision)
	}
	return nil
}

// ── Candidate verification ───────────────────────────────────────────────────

// VerifiedFile records one confirmed fact about a file staged as part of a
// recovery candidate: the object key it came from and the full SHA-256 of its
// (decrypted) content. Retries trust a local file only when it still matches.
type VerifiedFile struct {
	ObjectKey string `json:"object_key"`
	SHA256    string `json:"sha256"`
	Ancestor  bool   `json:"ancestor,omitempty"`
}

// RecoveryCandidate is a checkpoint whose complete evidence has been verified:
// index decodes, every referenced SST and Pebble metadata object exists and is
// intact, and Pebble can open and scan the merged directory. All files come
// from this one checkpoint — recovery never splices files across checkpoints.
type RecoveryCandidate struct {
	IndexKey string `json:"index_key"`
	// Index is the decoded checkpoint index.
	Index *CheckpointIndex `json:"-"`
	// Files maps local filename (as Pebble sees it) to its verified provenance.
	Files map[string]VerifiedFile `json:"files"`
}

// Manifest builds the manifest/latest body that publishes this candidate.
func (c *RecoveryCandidate) Manifest() *Manifest {
	return &Manifest{
		FormatVersion: c.Index.FormatVersion,
		CheckpointKey: c.IndexKey,
		Revision:      c.Index.Revision,
		LastSequence:  c.Index.LastSequence,
		Term:          c.Index.Term,
	}
}

// CandidateRejection records why a scanned checkpoint could not be used, so a
// failed recovery leaves a diagnosable trail instead of a single error.
type CandidateRejection struct {
	IndexKey string
	Reason   string
}

func (r CandidateRejection) String() string {
	return fmt.Sprintf("%s: %s", r.IndexKey, r.Reason)
}

// sstKeyPattern matches "sst/{16 lowercase hex}/{plain filename}".
func validateSSTObjectKey(key string) (name, hashPrefix string, err error) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 || parts[0] != "sst" || len(parts[1]) != 16 || parts[2] == "" {
		return "", "", fmt.Errorf("malformed sst object key %q", key)
	}
	for _, c := range parts[1] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", "", fmt.Errorf("malformed sst object key %q", key)
		}
	}
	if strings.Contains(parts[2], "/") {
		return "", "", fmt.Errorf("malformed sst object key %q", key)
	}
	return parts[2], parts[1], nil
}

// safeMetaName rejects metadata names that could escape the staging directory.
func safeMetaName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) {
		return false
	}
	if filepath.IsAbs(name) {
		return false
	}
	return true
}

// stageFile downloads key from src to stagingDir/name, verifying the full
// content hash against the SST hash prefix embedded in the object key. When a
// previous attempt already verified the file (expected non-nil, local file
// present and hash-matching), no download is performed.
func stageFile(ctx context.Context, src object.Store, key, stagingDir, name string, expected *VerifiedFile, isSST bool) (*VerifiedFile, error) {
	dest := filepath.Join(stagingDir, name)
	if expected != nil && expected.ObjectKey == key {
		if sum, ok := hashLocalFile(dest); ok && sum == expected.SHA256 {
			if isSST {
				if _, prefix, perr := validateSSTObjectKey(key); perr == nil && !strings.HasPrefix(sum, prefix) {
					return nil, fmt.Errorf("staged sst %q hash %s does not match key prefix %s", key, sum, prefix)
				}
			}
			v := *expected
			return &v, nil
		}
	}

	rc, err := src.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download %q: %w", key, err)
	}
	part := dest + ".part"
	f, ferr := os.Create(part)
	if ferr != nil {
		_ = rc.Close()
		return nil, ferr
	}
	h := sha256.New()
	if _, cerr := io.Copy(io.MultiWriter(f, h), rc); cerr != nil {
		_ = rc.Close()
		_ = f.Close()
		_ = os.Remove(part)
		return nil, fmt.Errorf("download %q: %w", key, cerr)
	}
	if cerr := rc.Close(); cerr != nil {
		_ = f.Close()
		_ = os.Remove(part)
		return nil, fmt.Errorf("download %q: %w", key, cerr)
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(part)
		return nil, fmt.Errorf("download %q: %w", key, cerr)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if isSST {
		_, prefix, perr := validateSSTObjectKey(key)
		if perr != nil {
			_ = os.Remove(part)
			return nil, perr
		}
		if !strings.HasPrefix(sum, prefix) {
			_ = os.Remove(part)
			return nil, fmt.Errorf("sst %q content hash %s does not match its object-key prefix %s (object truncated or corrupt)", key, sum, prefix)
		}
	}
	if rerr := os.Rename(part, dest); rerr != nil {
		_ = os.Remove(part)
		return nil, rerr
	}
	return &VerifiedFile{ObjectKey: key, SHA256: sum}, nil
}

func hashLocalFile(path string) (string, bool) {
	f, err := os.Open(path)
	if err != nil {
		return "", false
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", false
	}
	return hex.EncodeToString(h.Sum(nil)), true
}

// verifyPebble opens the staged directory read-only and scans the whole merged
// keyspace. Read-only open proves the Pebble metadata (CURRENT/MANIFEST) names
// exactly the files the index shipped and all referenced tables are present;
// the full scan forces every SST block through checksum verification, so a
// truncated or corrupt table cannot pass even if its key hash were ignored.
func verifyPebble(stagingDir string) error {
	db, err := pebble.Open(stagingDir, &pebble.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("open staged checkpoint with Pebble: %w", err)
	}
	iter, err := db.NewIter(&pebble.IterOptions{})
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("scan staged checkpoint: %w", err)
	}
	for iter.First(); iter.Valid(); iter.Next() {
	}
	ierr := iter.Error()
	if cerr := iter.Close(); cerr != nil && ierr == nil {
		ierr = cerr
	}
	if cerr := db.Close(); cerr != nil && ierr == nil {
		ierr = cerr
	}
	if ierr != nil {
		return fmt.Errorf("scan staged checkpoint: %w", ierr)
	}
	return nil
}

// VerifyCheckpointComplete fully validates the checkpoint at indexKey in a
// throwaway staging directory and discards the download afterwards. It is the
// "is this checkpoint usable as a manifest target" predicate for callers that
// do not keep the staged directory themselves (manifest repair decisions, the
// CLI).
func (mgr *Manager) VerifyCheckpointComplete(ctx context.Context, store, ancestorStore object.Store, indexKey string) error {
	dir, err := os.MkdirTemp("", "t4-verify-checkpoint-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if _, _, verr := mgr.SelectCandidate(ctx, store, ancestorStore, dir, indexKey, nil); verr != nil {
		return verr
	}
	return nil
}

// VerifyRestoredDirectory validates a checkpoint directory already on local
// disk (for example the result of Restore from manifest/latest): Pebble must
// open it read-only and a full key scan must succeed. It lets startup detect a
// manifest that parses but points at an incomplete/corrupt checkpoint before
// the directory is used as the live database.
func (mgr *Manager) VerifyRestoredDirectory(dir string) error {
	return verifyPebble(dir)
}

// VerifyCandidate fully validates the checkpoint at indexKey and stages every
// file it references into stagingDir (which is created if needed).
//
// Validation, in order:
//  1. The index object exists, decodes, uses a supported format, and its
//     term/revision match its object key.
//  2. Every referenced SST object exists and its full SHA-256 matches the hash
//     prefix embedded in its "sst/{hash16}/{name}" key; every referenced Pebble
//     metadata object exists. Ancestor SSTs come exclusively from ancestorStore
//     (or store when ancestorStore is nil, matching RestoreBranch semantics).
//  3. Pebble opens the staged directory read-only and scans every key.
//
// All files are taken from the single index — nothing is borrowed from other
// checkpoints.
//
// When expected is non-nil it must describe indexKey from an earlier,
// interrupted attempt: files already staged whose hashes still match are
// reused without any download, and every other confirmed fact is re-checked.
// If the pinned candidate has lost objects or fails validation, the error is
// returned to the caller, which must fail the recovery rather than silently
// switching candidates.
func (mgr *Manager) VerifyCandidate(ctx context.Context, store, ancestorStore object.Store, indexKey, stagingDir string, expected *RecoveryCandidate) (*RecoveryCandidate, error) {
	if expected != nil && expected.IndexKey != indexKey {
		return nil, fmt.Errorf("internal error: pinned candidate %q does not match requested %q", expected.IndexKey, indexKey)
	}
	if !strings.HasSuffix(indexKey, "/manifest.json") {
		return nil, fmt.Errorf("index key %q must end with \"manifest.json\"", indexKey)
	}

	idx, err := mgr.ReadCheckpointIndex(ctx, store, indexKey)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint index: %w", err)
	}
	if want := CheckpointIndexKey(idx.Term, idx.Revision); want != indexKey {
		return nil, fmt.Errorf("index object key %q does not match its term=%d revision=%d (want %q)", indexKey, idx.Term, idx.Revision, want)
	}
	if len(idx.PebbleMeta) == 0 {
		return nil, errors.New("index lists no Pebble metadata files")
	}
	hasCurrent := false
	for _, name := range idx.PebbleMeta {
		if !safeMetaName(name) {
			return nil, fmt.Errorf("index lists unsafe metadata name %q", name)
		}
		if name == "CURRENT" {
			hasCurrent = true
		}
	}
	if !hasCurrent {
		return nil, errors.New("index does not list the Pebble CURRENT file")
	}

	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		return nil, err
	}

	files := make(map[string]VerifiedFile, len(idx.SSTFiles)+len(idx.AncestorSSTFiles)+len(idx.PebbleMeta))
	claim := func(name string) error {
		if _, dup := files[name]; dup {
			return fmt.Errorf("index references the local filename %q more than once", name)
		}
		return nil
	}

	anc := store
	if ancestorStore != nil {
		anc = ancestorStore
	}

	seenSST := make(map[string]struct{})
	stageSST := func(key string, fromAncestor bool) error {
		if _, done := seenSST[key]; done {
			return nil
		}
		seenSST[key] = struct{}{}
		name, _, perr := validateSSTObjectKey(key)
		if perr != nil {
			return perr
		}
		if cerr := claim(name); cerr != nil {
			return cerr
		}
		var prior *VerifiedFile
		if expected != nil {
			if v, ok := expected.Files[name]; ok {
				vv := v
				prior = &vv
			}
		}
		src := store
		if fromAncestor {
			src = anc
		}
		v, gerr := stageFile(ctx, src, key, stagingDir, name, prior, true)
		if gerr != nil {
			return gerr
		}
		v.Ancestor = fromAncestor
		files[name] = *v
		return nil
	}
	for _, key := range idx.SSTFiles {
		if err := stageSST(key, false); err != nil {
			return nil, err
		}
	}
	for _, key := range idx.AncestorSSTFiles {
		if err := stageSST(key, true); err != nil {
			return nil, err
		}
	}

	metaPrefix := strings.TrimSuffix(indexKey, "manifest.json")
	for _, name := range idx.PebbleMeta {
		if err := claim(name); err != nil {
			return nil, err
		}
		key := metaPrefix + name
		var prior *VerifiedFile
		if expected != nil {
			if v, ok := expected.Files[name]; ok {
				vv := v
				prior = &vv
			}
		}
		v, gerr := stageFile(ctx, store, key, stagingDir, name, prior, false)
		if gerr != nil {
			return nil, gerr
		}
		files[name] = *v
	}

	if err := verifyPebble(stagingDir); err != nil {
		return nil, err
	}
	return &RecoveryCandidate{IndexKey: indexKey, Index: idx, Files: files}, nil
}

// SelectCandidate scans checkpoint indexes in descending revision order
// (newest first) and returns the first one whose complete evidence verifies.
//
// pinnedKey, when non-empty, forces a single-candidate attempt against that
// key (used when resuming an interrupted recovery): a pinned candidate that no
// longer verifies is an error — selection never falls back to a different
// checkpoint, because that would splice two checkpoints' files together.
// expected carries the previously confirmed facts for a pinned retry.
//
// Every skipped candidate contributes a CandidateRejection with a concrete,
// diagnosable reason (missing object, hash mismatch, unknown format, Pebble
// validation failure).
func (mgr *Manager) SelectCandidate(ctx context.Context, store, ancestorStore object.Store, stagingDir, pinnedKey string, expected *RecoveryCandidate) (*RecoveryCandidate, []CandidateRejection, error) {
	if pinnedKey != "" {
		c, err := mgr.VerifyCandidate(ctx, store, ancestorStore, pinnedKey, stagingDir, expected)
		if err != nil {
			return nil, nil, fmt.Errorf("pinned recovery candidate %q no longer verifies: %w", pinnedKey, err)
		}
		return c, nil, nil
	}

	keys, err := mgr.ListRemote(ctx, store)
	if err != nil {
		return nil, nil, fmt.Errorf("list checkpoints: %w", err)
	}
	var rejections []CandidateRejection
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		_ = os.RemoveAll(stagingDir)
		c, verr := mgr.VerifyCandidate(ctx, store, ancestorStore, key, stagingDir, nil)
		if verr != nil {
			mgr.log.Warnf("checkpoint: recovery skipping candidate %q: %v", key, verr)
			rejections = append(rejections, CandidateRejection{IndexKey: key, Reason: verr.Error()})
			continue
		}
		return c, rejections, nil
	}
	_ = os.RemoveAll(stagingDir)
	return nil, rejections, nil
}

// RecoverLatest scans for the highest-revision complete checkpoint and stages
// it into stagingDir. It is the one-shot entry point used by the CLI and by
// first-time auto-recovery; interrupted in-node recovery uses SelectCandidate
// with a pinned key instead. Returns (nil, rejections, nil) when no candidate
// is complete.
func (mgr *Manager) RecoverLatest(ctx context.Context, store, ancestorStore object.Store, stagingDir string) (*RecoveryCandidate, []CandidateRejection, error) {
	return mgr.SelectCandidate(ctx, store, ancestorStore, stagingDir, "", nil)
}

// RejectionsString formats candidate skip reasons for log messages and errors.
func RejectionsString(rs []CandidateRejection) string {
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		parts = append(parts, r.String())
	}
	return strings.Join(parts, "; ")
}

// ── Conditional manifest repair ──────────────────────────────────────────────

// CheckCandidateObjectsPresent re-confirms that every object a verified
// candidate references (index, Pebble metadata, SSTs) still exists in object
// storage. It is invoked right before publishing the repaired manifest: GC
// racing with a recovery could delete the candidate after verification, and
// pointing manifest/latest at an evicted checkpoint would re-publish exactly
// the incomplete state recovery exists to avoid. The check uses LIST and
// never downloads bodies; the content hashes were already confirmed during
// selection. Ancestor SSTs are looked up in ancestorStore (or in store when
// it is nil, matching RestoreBranch).
func (mgr *Manager) CheckCandidateObjectsPresent(ctx context.Context, store, ancestorStore object.Store, c *RecoveryCandidate) error {
	localSST, err := listSSTSet(ctx, store)
	if err != nil {
		return fmt.Errorf("list sst objects: %w", err)
	}
	ancSST := localSST
	if ancestorStore != nil {
		ancSST, err = listSSTSet(ctx, ancestorStore)
		if err != nil {
			return fmt.Errorf("list ancestor sst objects: %w", err)
		}
	}
	metaPrefix := strings.TrimSuffix(c.IndexKey, "manifest.json")
	metaKeys, err := store.List(ctx, metaPrefix)
	if err != nil {
		return fmt.Errorf("list checkpoint metadata: %w", err)
	}
	metaSet := make(map[string]struct{}, len(metaKeys))
	for _, k := range metaKeys {
		metaSet[k] = struct{}{}
	}
	if _, ok := metaSet[c.IndexKey]; !ok {
		return fmt.Errorf("checkpoint index %q is gone", c.IndexKey)
	}
	for name, vf := range c.Files {
		var present bool
		switch {
		case strings.HasPrefix(vf.ObjectKey, "sst/"):
			set := localSST
			if vf.Ancestor {
				set = ancSST
			}
			_, present = set[vf.ObjectKey]
		default:
			_, present = metaSet[vf.ObjectKey]
		}
		if !present {
			return fmt.Errorf("object %q referenced by checkpoint %q (file %q) is gone", vf.ObjectKey, c.IndexKey, name)
		}
	}
	return nil
}

// ManifestAtLeast reports whether manifest/latest currently exists, decodes,
// passes validate, and describes a checkpoint position at least as new as m.
// Recovery uses it to converge without writing when a peer healed the
// manifest concurrently: in that case validating and possibly republishing
// the older local candidate is pointless.
func (mgr *Manager) ManifestAtLeast(ctx context.Context, store object.Store, m *Manifest, validate func(*Manifest) error) (bool, error) {
	status, err := mgr.ReadManifestStatus(ctx, store)
	if err != nil {
		return false, err
	}
	if status == nil || status.Manifest == nil {
		return false, nil
	}
	if !isNewerOrEqual(status.Manifest, m) {
		return false, nil
	}
	if validate != nil {
		if err := validate(status.Manifest); err != nil {
			return false, nil
		}
	}
	return true, nil
}

// RepairOutcome reports what RepairManifest did.
type RepairOutcome struct {
	// Written is true when manifest/latest was (re)written by this call.
	Written bool
	// AlreadyCurrent is true when manifest/latest already pointed at a
	// checkpoint at least as new as the candidate and the caller's
	// acceptable check passed, so it was left untouched.
	AlreadyCurrent bool
}

// isNewerOrEqual reports whether old describes the same or a later checkpoint
// position than m.
func isNewerOrEqual(old, m *Manifest) bool {
	if old.Term != m.Term {
		return old.Term > m.Term
	}
	return old.LastSequence >= m.LastSequence
}

// RepairManifest conditionally publishes m at manifest/latest.
//
// It handles four observed states:
//   - absent: created with PutIfAbsent;
//   - corrupt (truncated/unknown format): replaced via compare-and-swap;
//   - parseable but older, or pointing at an unusable checkpoint: replaced via
//     compare-and-swap;
//   - parseable, at least as new, and acceptable: left untouched.
//
// acceptable decides whether an existing newer manifest counts as a valid one.
// It is typically bound to ValidateManifestTarget: a newer manifest whose
// checkpoint index cannot even be read is not "valid" and is repaired forward.
// Pass nil to accept any parseable manifest with a supported format.
//
// A lost compare-and-swap (manifest changed between read and write) is re-read
// and re-evaluated, so a valid manifest published concurrently — including a
// higher one — is never overwritten. Stores without conditional writes, and a
// single-writer Manager, fall back to the same read-check-write sequence the
// rest of the package uses on such stores.
func (mgr *Manager) RepairManifest(ctx context.Context, store object.Store, m *Manifest, acceptable func(*Manifest) error) (*RepairOutcome, error) {
	if m == nil {
		return nil, errors.New("cannot repair with a nil manifest")
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	ok := func(existing *Manifest) error {
		if acceptable == nil {
			return nil
		}
		return acceptable(existing)
	}

	cs, isConditional := store.(object.ConditionalStore)
	useCAS := isConditional && !mgr.singleWriter
	if !useCAS {
		return mgr.repairManifestPlain(ctx, store, b, m, ok)
	}

	const maxAttempts = 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		cur, gerr := cs.GetETag(ctx, ManifestKey)
		if errors.Is(gerr, object.ErrNotFound) {
			if perr := cs.PutIfAbsent(ctx, ManifestKey, bytes.NewReader(b)); perr == nil {
				return &RepairOutcome{Written: true}, nil
			} else if !errors.Is(perr, object.ErrPreconditionFailed) {
				return nil, fmt.Errorf("checkpoint: create manifest: %w", perr)
			}
			continue // someone published it first; re-evaluate
		}
		if gerr != nil {
			return nil, fmt.Errorf("checkpoint: read manifest for repair: %w", gerr)
		}

		var existing Manifest
		decErr := json.NewDecoder(cur.Body).Decode(&existing)
		_ = cur.Body.Close()
		if decErr != nil {
			// Corrupt object: replace unconditionally against its ETag.
			if perr := cs.PutIfMatch(ctx, ManifestKey, bytes.NewReader(b), cur.ETag); perr == nil {
				return &RepairOutcome{Written: true}, nil
			} else if !errors.Is(perr, object.ErrPreconditionFailed) {
				return nil, fmt.Errorf("checkpoint: replace corrupt manifest: %w", perr)
			}
			continue
		}
		if existing.FormatVersion > CheckpointFormatVersion {
			// Unknown format: only overwrite via CAS; treat like corrupt.
			if perr := cs.PutIfMatch(ctx, ManifestKey, bytes.NewReader(b), cur.ETag); perr == nil {
				return &RepairOutcome{Written: true}, nil
			} else if !errors.Is(perr, object.ErrPreconditionFailed) {
				return nil, fmt.Errorf("checkpoint: replace future-format manifest: %w", perr)
			}
			continue
		}
		if existing.LastSequence == 0 {
			existing.LastSequence = existing.Revision
		}
		if isNewerOrEqual(&existing, m) && ok(&existing) == nil {
			return &RepairOutcome{AlreadyCurrent: true}, nil
		}
		// Older/equal-but-broken: repair forward with CAS on the read ETag.
		if perr := cs.PutIfMatch(ctx, ManifestKey, bytes.NewReader(b), cur.ETag); perr == nil {
			return &RepairOutcome{Written: true}, nil
		} else if !errors.Is(perr, object.ErrPreconditionFailed) {
			return nil, fmt.Errorf("checkpoint: repair manifest: %w", perr)
		}
	}
	return nil, errors.New("checkpoint: repair manifest: lost too many conditional-write races")
}

// repairManifestPlain is the best-effort read-check-write path for stores that
// do not offer conditional writes and for single-writer Managers.
func (mgr *Manager) repairManifestPlain(ctx context.Context, store object.Store, b []byte, m *Manifest, ok func(*Manifest) error) (*RepairOutcome, error) {
	status, err := mgr.ReadManifestStatus(ctx, store)
	if err != nil {
		return nil, err
	}
	switch {
	case status.Missing || status.Corrupt:
		if err := store.Put(ctx, ManifestKey, bytes.NewReader(b)); err != nil {
			return nil, fmt.Errorf("checkpoint: repair manifest: %w", err)
		}
		return &RepairOutcome{Written: true}, nil
	case isNewerOrEqual(status.Manifest, m) && ok(status.Manifest) == nil:
		return &RepairOutcome{AlreadyCurrent: true}, nil
	default:
		if err := store.Put(ctx, ManifestKey, bytes.NewReader(b)); err != nil {
			return nil, fmt.Errorf("checkpoint: repair manifest: %w", err)
		}
		return &RepairOutcome{Written: true}, nil
	}
}
