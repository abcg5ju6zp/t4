package t4

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

// recoveryTestStore wraps Mem with:
//   - per-key GET counters;
//   - a scripted manifest view (garbage for the first corruptManifestUntil
//     GETs, then a fixed valid body) for the newer-manifest test;
//   - fail-next-N manifest Puts to interrupt recovery right before completion;
//   - a log of every successful manifest write (decoded checkpoint_key).
type recoveryTestStore struct {
	*object.Mem
	mu sync.Mutex

	getCount         map[string]int
	manifestWrites   []string
	failManifestPuts int
	corruptUntilGet  int
	thenManifestBody []byte

	injectedPutErr error
}

func newRecoveryTestStore() *recoveryTestStore {
	return &recoveryTestStore{Mem: object.NewMem(), getCount: map[string]int{}}
}

func (s *recoveryTestStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.getCount[key]++
	n := s.getCount[key]
	corruptUntil := s.corruptUntilGet
	then := s.thenManifestBody
	if key == checkpoint.ManifestKey && then != nil && n == corruptUntil+1 {
		// A peer heals the manifest for real just before the repair decision.
		_ = s.Mem.Put(ctx, key, bytes.NewReader(then))
	}
	s.mu.Unlock()
	if key == checkpoint.ManifestKey && then != nil {
		if n <= corruptUntil {
			return io.NopCloser(strings.NewReader("{manifest-truncated")), nil
		}
		return io.NopCloser(bytes.NewReader(append([]byte(nil), then...))), nil
	}
	return s.Mem.Get(ctx, key)
}

func (s *recoveryTestStore) Put(ctx context.Context, key string, r io.Reader) error {
	if key == checkpoint.ManifestKey {
		s.mu.Lock()
		if s.failManifestPuts > 0 {
			s.failManifestPuts--
			err := s.injectedPutErr
			s.mu.Unlock()
			if err == nil {
				err = errors.New("injected manifest put failure")
			}
			return err
		}
		s.mu.Unlock()
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		var m checkpoint.Manifest
		_ = json.Unmarshal(b, &m)
		s.mu.Lock()
		s.manifestWrites = append(s.manifestWrites, m.CheckpointKey)
		s.mu.Unlock()
		return s.Mem.Put(ctx, key, bytes.NewReader(b))
	}
	return s.Mem.Put(ctx, key, r)
}

func (s *recoveryTestStore) resetCounters() {
	s.mu.Lock()
	s.getCount = map[string]int{}
	s.manifestWrites = nil
	s.mu.Unlock()
}

func (s *recoveryTestStore) getsWithPrefix(p string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, v := range s.getCount {
		if strings.HasPrefix(k, p) {
			n += v
		}
	}
	return n
}

func (s *recoveryTestStore) firstManifestWrite() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.manifestWrites) == 0 {
		return "", false
	}
	return s.manifestWrites[0], true
}

func (s *recoveryTestStore) manifestWriteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.manifestWrites)
}

// seedRecoverableCluster runs a single node to 40 revisions, checkpointing at
// rev 20, with synchronous WAL uploads so wal/ covers sequences 1..40.
func seedRecoverableCluster(t *testing.T, store object.Store, dir string) {
	t.Helper()
	sync := true
	n, err := Open(Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &sync,
		CheckpointInterval: time.Hour,
		Logger:             NoopLogger,
	})
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	ctx := context.Background()
	put := func(i int) {
		if _, err := n.Put(ctx, fmt.Sprintf("/k%02d", i), []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for i := 0; i < 20; i++ {
		put(i)
	}
	n.maybeCheckpoint(ctx)
	for i := 20; i < 40; i++ {
		put(i)
	}
	if err := n.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
}

// listCheckpointKeys returns checkpoint index keys, sorted.
func listCheckpointKeys(t *testing.T, store object.Store) []string {
	t.Helper()
	keys, err := checkpoint.New(NoopLogger).ListRemote(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func readIdx(t *testing.T, store object.Store, key string) *checkpoint.CheckpointIndex {
	t.Helper()
	rc, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	var idx checkpoint.CheckpointIndex
	if err := json.Unmarshal(b, &idx); err != nil {
		t.Fatal(err)
	}
	return &idx
}

func wipeLocalDB(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(dir, "db")); err != nil {
		t.Fatal(err)
	}
}

func reopenRecovered(t *testing.T, store object.Store, dir string) *Node {
	t.Helper()
	if rs, ok := store.(*recoveryTestStore); ok {
		rs.resetCounters()
	}
	sync := true
	n, err := Open(Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &sync,
		CheckpointInterval: time.Hour,
		Logger:             NoopLogger,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func assertAllDataPresent(t *testing.T, n *Node) {
	t.Helper()
	for i := 0; i < 40; i++ {
		kv, err := n.Get(fmt.Sprintf("/k%02d", i))
		if err != nil || kv == nil || string(kv.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("Get /k%02d = %v err=%v", i, kv, err)
		}
	}
}

func assertRecoveryStateGone(t *testing.T, dir string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, recoveryStateName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery state file still present (err=%v)", err)
	}
}

func assertManifestUsable(t *testing.T, store object.Store) *checkpoint.Manifest {
	t.Helper()
	cp := checkpoint.New(NoopLogger)
	st, err := cp.ReadManifestStatus(context.Background(), store)
	if err != nil || st == nil || st.Manifest == nil {
		t.Fatalf("manifest status: %+v err=%v", st, err)
	}
	if err := cp.ValidateManifestTarget(context.Background(), store, st.Manifest); err != nil {
		t.Fatalf("repaired manifest target invalid: %v", err)
	}
	return st.Manifest
}

func TestRecoveryFromMissingManifest(t *testing.T) {
	store := newRecoveryTestStore()
	dir := filepath.Join(t.TempDir(), "data")
	seedRecoverableCluster(t, store, dir)
	keys := listCheckpointKeys(t, store)
	highKey := keys[len(keys)-1]

	if err := store.Delete(context.Background(), checkpoint.ManifestKey); err != nil {
		t.Fatal(err)
	}
	wipeLocalDB(t, dir)

	n := reopenRecovered(t, store, dir)
	assertAllDataPresent(t, n)

	written, ok := store.firstManifestWrite()
	if !ok || written != highKey {
		t.Fatalf("first repair wrote %q, want %q (all writes=%v)", written, highKey, store.manifestWrites)
	}
	assertManifestUsable(t, store)
	assertRecoveryStateGone(t, dir)
}

func TestRecoveryFromTruncatedManifest(t *testing.T) {
	store := newRecoveryTestStore()
	dir := filepath.Join(t.TempDir(), "data")
	seedRecoverableCluster(t, store, dir)
	keys := listCheckpointKeys(t, store)
	highKey := keys[len(keys)-1]

	if err := store.Mem.Put(context.Background(), checkpoint.ManifestKey,
		strings.NewReader(`{"checkpoint_key": "checkp`)); err != nil {
		t.Fatal(err)
	}
	wipeLocalDB(t, dir)

	n := reopenRecovered(t, store, dir)
	assertAllDataPresent(t, n)

	written, ok := store.firstManifestWrite()
	if !ok || written != highKey {
		t.Fatalf("repair wrote %q want %q (writes=%v)", written, highKey, store.manifestWrites)
	}
	assertManifestUsable(t, store)
	assertRecoveryStateGone(t, dir)
}

func TestRecoveryFallsBackWhenNewestIncomplete(t *testing.T) {
	store := newRecoveryTestStore()
	dir := filepath.Join(t.TempDir(), "data")
	// One good checkpoint@20; WAL segments 21..40 survive because no later
	// checkpoint successfully covered them.
	seedRecoverableCluster(t, store, dir)
	keys := listCheckpointKeys(t, store)
	lowKey := keys[len(keys)-1]

	// A checkpoint@40 upload crashed mid-flight: index exists, but its Pebble
	// metadata is incomplete (CURRENT missing, MANIFEST truncated). manifest/
	// latest already points at it.
	phantomKey := checkpoint.CheckpointIndexKey(1, 40)
	prefix := strings.TrimSuffix(phantomKey, "manifest.json")
	idx := &checkpoint.CheckpointIndex{
		FormatVersion: checkpoint.FormatVersionMeta,
		Term:          1,
		Revision:      40,
		LastSequence:  40,
		SSTFiles:      []string{},
		PebbleMeta:    []string{"MANIFEST-000001"},
	}
	ib, _ := json.Marshal(idx)
	ctx := context.Background()
	if err := store.Mem.Put(ctx, phantomKey, bytes.NewReader(ib)); err != nil {
		t.Fatal(err)
	}
	if err := store.Mem.Put(ctx, prefix+"MANIFEST-000001", bytes.NewReader([]byte("xxxx"))); err != nil {
		t.Fatal(err)
	}
	mb, _ := json.Marshal(&checkpoint.Manifest{
		CheckpointKey: phantomKey, Term: 1, Revision: 40, LastSequence: 40,
	})
	if err := store.Mem.Put(ctx, checkpoint.ManifestKey, bytes.NewReader(mb)); err != nil {
		t.Fatal(err)
	}
	wipeLocalDB(t, dir)

	store.resetCounters()
	sync := true
	n, err := Open(Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &sync,
		CheckpointInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	assertAllDataPresent(t, n)

	written, ok := store.firstManifestWrite()
	if !ok || written != lowKey {
		t.Fatalf("repair should pin the older complete %q, wrote %q (writes=%v)", lowKey, written, store.manifestWrites)
	}
	m := assertManifestUsable(t, store)
	if m.CheckpointKey != lowKey {
		t.Errorf("manifest points to %s, want %s", m.CheckpointKey, lowKey)
	}
	assertRecoveryStateGone(t, dir)
}

// TestRecoveryFailsWhenNewestIncompleteAndWALGCd: the highest complete
// checkpoint is behind a newer corrupt one whose WAL GC already removed the
// intervening segments — no single revision beyond the complete checkpoint is
// provable, so the node must fail explicitly rather than re-restore the broken
// checkpoint or serve a half-recovered directory.
func TestRecoveryFailsWhenNewestIncompleteAndWALGCd(t *testing.T) {
	store := newRecoveryTestStore()
	dir := filepath.Join(t.TempDir(), "data")
	sync := true
	n0, err := Open(Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &sync,
		CheckpointInterval: time.Hour,
		Logger:             NoopLogger,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, err := n0.Put(ctx, fmt.Sprintf("/k%02d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	n0.maybeCheckpoint(ctx)
	for i := 20; i < 40; i++ {
		if _, err := n0.Put(ctx, fmt.Sprintf("/k%02d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	n0.maybeCheckpoint(ctx) // successful cp@40; WAL 21..40 now GC-covered
	if err := n0.Close(); err != nil {
		t.Fatal(err)
	}
	keys := listCheckpointKeys(t, store)
	highKey := keys[len(keys)-1]

	// Corrupt cp@40 metadata after its WAL GC deleted the only path from
	// cp@20 to rev 40.
	hidx := readIdx(t, store, highKey)
	var manifestObj string
	for _, name := range hidx.PebbleMeta {
		if strings.HasPrefix(name, "MANIFEST") {
			manifestObj = strings.TrimSuffix(highKey, "manifest.json") + name
		}
	}
	if err := store.Mem.Put(ctx, manifestObj, bytes.NewReader([]byte("xxxx"))); err != nil {
		t.Fatal(err)
	}
	wipeLocalDB(t, dir)

	n, err := Open(Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &sync,
		CheckpointInterval: time.Hour,
		Logger:             NoopLogger,
	})
	if err == nil {
		_ = n.Close()
		t.Fatal("expected explicit failure when no single revision is recoverable")
	}
	if !strings.Contains(err.Error(), "WAL replay") && !strings.Contains(err.Error(), "candidate") && !strings.Contains(err.Error(), "sequence") {
		t.Fatalf("error should name the WAL/evidence gap: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, recoveryStateName)); statErr != nil {
		t.Fatalf("recovery state should remain for a pinned retry: %v", err)
	}
}

// TestRecoveryInterruptedBeforeRepairResumes: the first boot does all the
// scanning/download/replay work but fails at the single manifest write. The
// retry must reuse the same candidate and its confirmed local files — no SST
// or checkpoint-meta downloads at all.
func TestRecoveryInterruptedBeforeRepairResumes(t *testing.T) {
	store := newRecoveryTestStore()
	store.injectedPutErr = errors.New("simulated conditional write outage")
	dir := filepath.Join(t.TempDir(), "data")
	seedRecoverableCluster(t, store, dir)
	keys := listCheckpointKeys(t, store)
	highKey := keys[len(keys)-1]

	if err := store.Delete(context.Background(), checkpoint.ManifestKey); err != nil {
		t.Fatal(err)
	}
	wipeLocalDB(t, dir)

	sync := true
	cfg := Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &sync,
		CheckpointInterval: time.Hour,
	}

	// First attempt: fail the repair write exactly once.
	store.failManifestPuts = 1
	if n, err := Open(cfg); err == nil {
		_ = n.Close()
		t.Fatal("first open should fail when the manifest repair write fails")
	}
	statePath := filepath.Join(dir, recoveryStateName)
	b, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("recovery state should survive the failure: %v", err)
	}
	var st recoveryState
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	if !st.Staged || st.IndexKey != highKey {
		t.Fatalf("state staged=%v key=%s want pinned %s", st.Staged, st.IndexKey, highKey)
	}

	// Second attempt: everything is confirmed locally.
	store.resetCounters()
	n, err := Open(cfg)
	if err != nil {
		t.Fatalf("retry open: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	assertAllDataPresent(t, n)

	if got := store.getsWithPrefix("sst/"); got != 0 {
		t.Errorf("retry downloaded %d SST objects; confirmed files must be reused", got)
	}
	var metaGets int
	store.mu.Lock()
	for k, v := range store.getCount {
		if strings.HasPrefix(k, "checkpoint/") && strings.HasSuffix(k, "manifest.json") && k != checkpoint.ManifestKey && k != highKey {
			metaGets += v
		}
	}
	store.mu.Unlock()
	if metaGets != 0 {
		t.Errorf("retry inspected other checkpoint indexes: %d GETs", metaGets)
	}
	written, ok := store.firstManifestWrite()
	if !ok || written != highKey {
		t.Fatalf("retry repair wrote %q want %q", written, highKey)
	}
	assertRecoveryStateGone(t, dir)
}

// TestRecoveryPinnedCandidateThatVanishes: once a candidate is pinned, losing
// its objects on retry is an explicit failure, never a silent switch.
func TestRecoveryPinnedCandidateThatVanishes(t *testing.T) {
	store := newRecoveryTestStore()
	store.injectedPutErr = errors.New("simulated repair outage")
	dir := filepath.Join(t.TempDir(), "data")
	seedRecoverableCluster(t, store, dir)
	keys := listCheckpointKeys(t, store)
	highKey := keys[len(keys)-1]

	if err := store.Mem.Put(context.Background(), checkpoint.ManifestKey,
		strings.NewReader("garbage")); err != nil {
		t.Fatal(err)
	}
	wipeLocalDB(t, dir)

	sync := true
	cfg := Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &sync,
		CheckpointInterval: time.Hour,
	}
	store.failManifestPuts = 1
	if n, err := Open(cfg); err == nil {
		_ = n.Close()
		t.Fatal("first open should fail at repair")
	}

	// GC removes the pinned checkpoint while the node was down.
	idx := readIdx(t, store, highKey)
	var toDelete []string
	toDelete = append(toDelete, highKey)
	prefix := strings.TrimSuffix(highKey, "manifest.json")
	for _, name := range idx.PebbleMeta {
		toDelete = append(toDelete, prefix+name)
	}
	if err := store.DeleteMany(context.Background(), toDelete); err != nil {
		t.Fatal(err)
	}
	store.resetCounters()
	writesBefore := 0

	n, err := Open(cfg)
	if err == nil {
		_ = n.Close()
		t.Fatal("retry must fail explicitly when the pinned candidate vanished")
	}
	if !strings.Contains(err.Error(), highKey) {
		t.Fatalf("error must name the pinned candidate %s: %v", highKey, err)
	}
	if store.manifestWriteCount() != writesBefore {
		t.Fatal("no manifest may be repaired from a failed pinned recovery")
	}
}

// TestRecoveryKeepsNewerValidManifest: a valid higher-term manifest appearing
// before the repair write is never overwritten.
func TestRecoveryKeepsNewerValidManifest(t *testing.T) {
	store := newRecoveryTestStore()
	dir := filepath.Join(t.TempDir(), "data")
	seedRecoverableCluster(t, store, dir)
	keys := listCheckpointKeys(t, store)
	highKey := keys[len(keys)-1]

	// Script: first two GETs (baseline scan trigger, post-replay check) see a
	// corrupt manifest; the third GET (the repair decision) sees a newer valid
	// manifest pointing at a complete checkpoint.
	newer := checkpoint.Manifest{
		CheckpointKey: highKey, Term: 7, Revision: 20, LastSequence: 20,
	}
	nb, _ := json.Marshal(&newer)
	store.corruptUntilGet = 2
	store.thenManifestBody = nb
	store.resetCounters()
	wipeLocalDB(t, dir)

	n := reopenRecovered(t, store, dir)
	assertAllDataPresent(t, n)

	if store.manifestWriteCount() != 0 {
		t.Fatalf("newer valid manifest must not be overwritten: writes=%v", store.manifestWrites)
	}
	// Raw object must still be the injected higher-term manifest.
	rc, err := store.Mem.Get(context.Background(), checkpoint.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	var m checkpoint.Manifest
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("manifest body lost: %v (%q)", err, got)
	}
	if m.Term != 7 {
		t.Errorf("manifest term=%d want 7", m.Term)
	}
	assertRecoveryStateGone(t, dir)
}

// TestRecoveryWithoutEvidenceFailsExplicitly: broken manifest and no complete
// checkpoint anywhere — the node must not start or publish anything.
func TestRecoveryWithoutEvidenceFailsExplicitly(t *testing.T) {
	store := newRecoveryTestStore()
	dir := filepath.Join(t.TempDir(), "data")
	seedRecoverableCluster(t, store, dir)

	keys, err := store.List(context.Background(), "checkpoint/")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteMany(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	if err := store.Mem.Put(context.Background(), checkpoint.ManifestKey,
		strings.NewReader("garbage")); err != nil {
		t.Fatal(err)
	}
	wipeLocalDB(t, dir)
	writesBefore := store.manifestWriteCount()

	sync := true
	n, err := Open(Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &sync,
		CheckpointInterval: time.Hour,
		Logger:             NoopLogger,
	})
	if err == nil {
		_ = n.Close()
		t.Fatal("Open should fail when no complete checkpoint exists")
	}
	if !strings.Contains(err.Error(), "no complete checkpoint") {
		t.Fatalf("error should explain the lack of evidence: %v", err)
	}
	if store.manifestWriteCount() != writesBefore {
		t.Fatal("must not repair a manifest without complete evidence")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "db")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatal("no half-recovered db directory may be published")
	}
}

// TestRecoveryFreshEmptyStoreStartsNormally: missing manifest plus zero
// checkpoints is a brand-new cluster — unchanged behavior.
func TestRecoveryFreshEmptyStoreStartsNormally(t *testing.T) {
	store := newRecoveryTestStore()
	dir := filepath.Join(t.TempDir(), "data")
	n := reopenRecovered(t, store, dir)
	if _, err := n.Put(context.Background(), "/fresh", []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	assertRecoveryStateGone(t, dir)
}

// TestNormalManifestBootstrapUnchanged: an intact manifest still takes the
// ordinary one-GET restore path and leaves no recovery state.
func TestNormalManifestBootstrapUnchanged(t *testing.T) {
	store := newRecoveryTestStore()
	dir := filepath.Join(t.TempDir(), "data")
	seedRecoverableCluster(t, store, dir)
	wipeLocalDB(t, dir)

	n := reopenRecovered(t, store, dir)
	assertAllDataPresent(t, n)
	assertRecoveryStateGone(t, dir)
	assertManifestUsable(t, store)
}

// TestRecoveryWithEncryptedObjectStore: verification and repair operate on
// decrypted content through the encryption wrapper.
func TestRecoveryWithEncryptedObjectStore(t *testing.T) {
	mem := object.NewMem()
	kp, err := object.NewStaticKeyProvider(bytes.Repeat([]byte{0x2a}, 32))
	if err != nil {
		t.Fatal(err)
	}
	enc := object.NewEncryptedStore(mem, kp)
	dir := filepath.Join(t.TempDir(), "data")
	seedRecoverableCluster(t, enc, dir)

	if err := mem.Delete(context.Background(), checkpoint.ManifestKey); err != nil {
		t.Fatal(err)
	}
	wipeLocalDB(t, dir)

	n := reopenRecovered(t, enc, dir)
	assertAllDataPresent(t, n)
	assertManifestUsable(t, enc)
	assertRecoveryStateGone(t, dir)

	// The repaired manifest at rest is ciphertext, not plain JSON.
	rc, err := mem.Get(context.Background(), checkpoint.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(rc)
	rc.Close()
	if bytes.HasPrefix(raw, []byte("{")) || !bytes.HasPrefix(raw, []byte("T4E1")) {
		t.Fatalf("manifest at rest is not encrypted: %q", raw[:min(16, len(raw))])
	}
}
