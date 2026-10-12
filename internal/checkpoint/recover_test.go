package checkpoint_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

// seedTwoCheckpoints creates two real checkpoints (rev 10 and rev 20) with
// distinct data, returning the store and their index keys.
func seedTwoCheckpoints(t *testing.T, store object.Store) (lowKey, highKey string) {
	t.Helper()
	ctx := context.Background()
	db := openDB(t)

	b := db.NewBatch()
	for i := 0; i < 40; i++ {
		if err := b.Set([]byte(fmt.Sprintf("low-%03d", i)), bytes.Repeat([]byte("L"), 2048), nil); err != nil {
			t.Fatalf("set low: %v", err)
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatalf("commit low: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush low: %v", err)
	}
	if err := testCP.Write(ctx, db, store, 1, 10, "", nil); err != nil {
		t.Fatalf("write cp@10: %v", err)
	}
	low := testCurrentManifest(t, store)

	b2 := db.NewBatch()
	for i := 0; i < 40; i++ {
		if err := b2.Set([]byte(fmt.Sprintf("high-%03d", i)), bytes.Repeat([]byte("H"), 2048), nil); err != nil {
			t.Fatalf("set high: %v", err)
		}
	}
	if err := b2.Commit(pebble.Sync); err != nil {
		t.Fatalf("commit high: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush high: %v", err)
	}
	if err := testCP.Write(ctx, db, store, 1, 20, "", nil); err != nil {
		t.Fatalf("write cp@20: %v", err)
	}
	high := testCurrentManifest(t, store)
	return low.CheckpointKey, high.CheckpointKey
}

func testCurrentManifest(t *testing.T, store object.Store) *checkpoint.Manifest {
	t.Helper()
	m, err := testCP.ReadManifest(context.Background(), store)
	if err != nil || m == nil {
		t.Fatalf("read manifest: %v %v", m, err)
	}
	return m
}

// openAndCheck opens the staged directory read-only and verifies wantKey is
// readable, forcing Pebble through its own metadata/SST validation.
func openAndCheck(t *testing.T, dir, wantKey string) error {
	t.Helper()
	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("open staged: %w", err)
	}
	defer func() { _ = db.Close() }()
	v, closer, err := db.Get([]byte(wantKey))
	if err != nil {
		return fmt.Errorf("get %q from staged checkpoint: %w", wantKey, err)
	}
	_ = closer.Close()
	if len(v) == 0 {
		return fmt.Errorf("empty value for %q", wantKey)
	}
	return nil
}

// TestRecoverLatestPicksHighestComplete: with both checkpoints intact the
// highest-revision one wins.
func TestRecoverLatestPicksHighestComplete(t *testing.T) {
	store := object.NewMem()
	_, highKey := seedTwoCheckpoints(t, store)

	staging := filepath.Join(t.TempDir(), "stage")
	cand, skipped, err := testCP.RecoverLatest(context.Background(), store, nil, staging)
	if err != nil {
		t.Fatalf("RecoverLatest: %v", err)
	}
	if cand == nil {
		t.Fatalf("no candidate; skipped=%v", skipped)
	}
	if cand.IndexKey != highKey {
		t.Errorf("selected %s, want highest %s", cand.IndexKey, highKey)
	}
	if cand.Index.Revision != 20 {
		t.Errorf("selected revision %d, want 20", cand.Index.Revision)
	}
	if len(skipped) != 0 {
		t.Errorf("expected no skips, got %v", skipped)
	}
	// Pebble must be able to open and read the staged directory.
	if err := pebbleOpenRead(t, staging, "high-000"); err != nil {
		t.Error(err)
	}
}

// pebbleOpenRead opens dir read-only and checks that wantKey exists.
func pebbleOpenRead(t *testing.T, dir, wantKey string) error {
	t.Helper()
	return openAndCheck(t, dir, wantKey)
}

func TestRecoverSkipsTruncatedSST(t *testing.T) {
	store := object.NewMem()
	lowKey, highKey := seedTwoCheckpoints(t, store)

	// Truncate an SST referenced exclusively by the high checkpoint.
	highIdx := readTestIndex(t, store, highKey)
	lowIdx := readTestIndex(t, store, lowKey)
	victim := exclusiveSST(t, lowIdx, highIdx)
	truncateObject(t, store, victim)

	staging := filepath.Join(t.TempDir(), "stage")
	cand, skipped, err := testCP.RecoverLatest(context.Background(), store, nil, staging)
	if err != nil {
		t.Fatalf("RecoverLatest: %v", err)
	}
	if cand == nil {
		t.Fatalf("expected fallback to low checkpoint; skipped=%v", checkpoint.RejectionsString(skipped))
	}
	if cand.IndexKey != lowKey {
		t.Errorf("selected %s, want fallback %s", cand.IndexKey, lowKey)
	}
	if len(skipped) != 1 || skipped[0].IndexKey != highKey {
		t.Fatalf("expected one rejection for %s, got %v", highKey, skipped)
	}
	if !strings.Contains(skipped[0].Reason, "hash") {
		t.Errorf("skip reason should mention hash mismatch: %q", skipped[0].Reason)
	}
	if err := pebbleOpenRead(t, staging, "low-000"); err != nil {
		t.Error(err)
	}
}

func TestRecoverSkipsMissingCurrent(t *testing.T) {
	store := object.NewMem()
	lowKey, highKey := seedTwoCheckpoints(t, store)

	// Delete the high checkpoint's CURRENT (per-checkpoint metadata).
	if err := store.Delete(context.Background(), strings.TrimSuffix(highKey, "manifest.json")+"CURRENT"); err != nil {
		t.Fatal(err)
	}

	staging := filepath.Join(t.TempDir(), "stage")
	cand, skipped, err := testCP.RecoverLatest(context.Background(), store, nil, staging)
	if err != nil {
		t.Fatalf("RecoverLatest: %v", err)
	}
	if cand == nil || cand.IndexKey != lowKey {
		t.Fatalf("want fallback %s, got cand=%v skipped=%v", lowKey, cand, skipped)
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0].Reason, "CURRENT") {
		t.Fatalf("expected rejection naming the missing CURRENT object, got %v", skipped)
	}
}

// TestRecoverSkipsBrokenPebbleManifest: every object downloads, but the
// Pebble MANIFEST is internally corrupt (truncated) — read-only open must
// reject the candidate even though all referenced objects are present.
func TestRecoverSkipsBrokenPebbleManifest(t *testing.T) {
	store := object.NewMem()
	lowKey, highKey := seedTwoCheckpoints(t, store)
	idx := readTestIndex(t, store, highKey)
	var manifestObj string
	for _, name := range idx.PebbleMeta {
		if strings.HasPrefix(name, "MANIFEST") {
			manifestObj = strings.TrimSuffix(highKey, "manifest.json") + name
		}
	}
	if manifestObj == "" {
		t.Fatal("no MANIFEST object in high checkpoint")
	}
	// Truncate to a few bytes: Pebble tolerates a torn trailing record, but a
	// missing header makes opening the database fail.
	ctx := context.Background()
	rc, err := store.Get(ctx, manifestObj)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(rc)
	rc.Close()
	if err := store.Put(ctx, manifestObj, bytes.NewReader(raw[:4])); err != nil {
		t.Fatal(err)
	}

	staging := filepath.Join(t.TempDir(), "stage")
	cand, skipped, err := testCP.RecoverLatest(context.Background(), store, nil, staging)
	if err != nil {
		t.Fatalf("RecoverLatest: %v", err)
	}
	if cand == nil || cand.IndexKey != lowKey {
		t.Fatalf("want fallback %s, got cand=%v skipped=%v", lowKey, cand, skipped)
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0].Reason, "Pebble") {
		t.Fatalf("expected a Pebble validation rejection, got %v", skipped)
	}
}

func TestRecoverSkipsMissingSSTObject(t *testing.T) {
	store := object.NewMem()
	lowKey, highKey := seedTwoCheckpoints(t, store)
	highIdx := readTestIndex(t, store, highKey)
	lowIdx := readTestIndex(t, store, lowKey)
	victim := exclusiveSST(t, lowIdx, highIdx)
	if err := store.Delete(context.Background(), victim); err != nil {
		t.Fatal(err)
	}

	staging := filepath.Join(t.TempDir(), "stage")
	cand, skipped, err := testCP.RecoverLatest(context.Background(), store, nil, staging)
	if err != nil {
		t.Fatalf("RecoverLatest: %v", err)
	}
	if cand == nil || cand.IndexKey != lowKey {
		t.Fatalf("want fallback %s, got cand=%v skipped=%v", lowKey, cand, skipped)
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0].Reason, victim) {
		t.Fatalf("rejection should name the missing object %q, got %v", victim, skipped)
	}
}

func TestRecoverSkipsUnknownIndexFormat(t *testing.T) {
	store := object.NewMem()
	_, highKey := seedTwoCheckpoints(t, store)

	// Replace the newest index with a future-format one.
	body := `{"format_version":999,"term":1,"revision":20,"sst_files":[],"pebble_meta":[]}`
	if err := store.Put(context.Background(), highKey, strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}

	staging := filepath.Join(t.TempDir(), "stage")
	cand, skipped, err := testCP.RecoverLatest(context.Background(), store, nil, staging)
	if err != nil {
		t.Fatalf("RecoverLatest: %v", err)
	}
	if cand == nil || cand.Index.Revision != 10 {
		t.Fatalf("want rev=10 fallback, got cand=%v skipped=%v", cand, skipped)
	}
	found := false
	for _, r := range skipped {
		if r.IndexKey == highKey && strings.Contains(r.Reason, "format_version=999") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected unknown-format rejection for %s, got %v", highKey, skipped)
	}
}

func TestRecoverNoCandidates(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	key := checkpoint.CheckpointIndexKey(1, 1)
	idx := checkpoint.CheckpointIndex{Term: 1, Revision: 1, PebbleMeta: []string{"CURRENT"}}
	b, _ := json.Marshal(idx)
	if err := store.Put(ctx, key, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}

	cand, skipped, err := testCP.RecoverLatest(ctx, store, nil, filepath.Join(t.TempDir(), "stage"))
	if err != nil {
		t.Fatalf("RecoverLatest: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected no candidate, got %s", cand.IndexKey)
	}
	if len(skipped) != 1 {
		t.Fatalf("expected one rejection, got %v", skipped)
	}
}

// countingStore records Gets per key prefix.
type countingStore struct {
	object.Store
	mu       sync.Mutex
	getCount map[string]int
	deny     map[string]bool
}

func newCountingStore(inner object.Store) *countingStore {
	return &countingStore{Store: inner, getCount: map[string]int{}, deny: map[string]bool{}}
}

func (c *countingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	c.mu.Lock()
	c.getCount[key]++
	deny := c.deny[key]
	c.mu.Unlock()
	if deny {
		return nil, object.ErrNotFound
	}
	return c.Store.Get(ctx, key)
}

func (c *countingStore) getsForPrefix(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, v := range c.getCount {
		if strings.HasPrefix(k, prefix) {
			n += v
		}
	}
	return n
}

// TestVerifyCandidateReusesConfirmedFiles: a retry must not re-download files
// whose local hash matches the previously confirmed fact.
func TestVerifyCandidateReusesConfirmedFiles(t *testing.T) {
	raw := object.NewMem()
	_, highKey := seedTwoCheckpoints(t, raw)
	store := newCountingStore(raw)
	ctx := context.Background()
	staging := filepath.Join(t.TempDir(), "stage")

	cand, _, err := testCP.RecoverLatest(ctx, store, nil, staging)
	if err != nil {
		t.Fatalf("first verify: %v", err)
	}
	if cand.IndexKey != highKey {
		t.Fatalf("selected %s", cand.IndexKey)
	}

	// Second, pinned attempt against the same staging dir: deny every object
	// GET except the index itself. Confirmed files must make it succeed with
	// zero file downloads.
	idx := readTestIndex(t, raw, highKey)
	for _, k := range idx.SSTFiles {
		store.deny[k] = true
	}
	for _, name := range idx.PebbleMeta {
		store.deny[strings.TrimSuffix(highKey, "manifest.json")+name] = true
	}

	// Snapshot counters; the second attempt may GET the index but must not
	// re-download any confirmed file.
	sstBefore := store.getsForPrefix("sst/")
	metaBefore := 0
	for _, name := range idx.PebbleMeta {
		metaBefore += store.getCount[strings.TrimSuffix(highKey, "manifest.json")+name]
	}

	cand2, err := testCP.VerifyCandidate(ctx, store, nil, highKey, staging, cand)
	if err != nil {
		t.Fatalf("pinned retry over confirmed files: %v", err)
	}
	if len(cand2.Files) != len(cand.Files) {
		t.Fatalf("file count changed: %d vs %d", len(cand2.Files), len(cand.Files))
	}
	if n := store.getsForPrefix("sst/") - sstBefore; n != 0 {
		t.Errorf("expected zero SST downloads on retry, got %d", n)
	}
	metaAfter := 0
	for _, name := range idx.PebbleMeta {
		metaAfter += store.getCount[strings.TrimSuffix(highKey, "manifest.json")+name]
	}
	if metaAfter-metaBefore != 0 {
		t.Errorf("expected zero meta downloads on retry, got %d", metaAfter-metaBefore)
	}
}

// TestPinnedCandidateThatDisappearsFails: resume must fail explicitly instead
// of silently switching to another checkpoint.
func TestPinnedCandidateThatDisappearsFails(t *testing.T) {
	raw := object.NewMem()
	_, highKey := seedTwoCheckpoints(t, raw)
	staging := filepath.Join(t.TempDir(), "stage")
	cand, _, err := testCP.RecoverLatest(context.Background(), raw, nil, staging)
	if err != nil {
		t.Fatal(err)
	}

	// GC deletes the whole high checkpoint while the node was down.
	idx := readTestIndex(t, raw, highKey)
	toDelete := []string{highKey}
	for _, name := range idx.PebbleMeta {
		toDelete = append(toDelete, strings.TrimSuffix(highKey, "manifest.json")+name)
	}
	if err := raw.DeleteMany(context.Background(), toDelete); err != nil {
		t.Fatal(err)
	}

	_, _, err = testCP.SelectCandidate(context.Background(), raw, nil, staging, highKey, cand)
	if err == nil {
		t.Fatal("expected explicit failure when pinned candidate is gone")
	}
	if !strings.Contains(err.Error(), highKey) {
		t.Errorf("error should name pinned candidate %s: %v", highKey, err)
	}
}

// TestObjectDeletedMidScanFallsBack: a meta object lost between LIST and GET
// is reported as a skip, not a crash.
func TestObjectDeletedMidScanFallsBack(t *testing.T) {
	raw := object.NewMem()
	lowKey, highKey := seedTwoCheckpoints(t, raw)
	store := newCountingStore(raw)
	// Deny the high checkpoint's OPTIONS meta object: the candidate must be
	// skipped (and reported), and selection falls back to the low one.
	highIdx := readTestIndex(t, raw, highKey)
	for _, name := range highIdx.PebbleMeta {
		if strings.HasPrefix(name, "OPTIONS") {
			store.deny[strings.TrimSuffix(highKey, "manifest.json")+name] = true
		}
	}
	cand, skipped, err := testCP.RecoverLatest(context.Background(), store, nil, filepath.Join(t.TempDir(), "stage"))
	if err != nil {
		t.Fatalf("RecoverLatest: %v", err)
	}
	if cand == nil || cand.IndexKey != lowKey {
		t.Fatalf("want fallback %s, got %v skipped=%v", lowKey, cand, skipped)
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func readTestIndex(t *testing.T, store object.Store, key string) *checkpoint.CheckpointIndex {
	t.Helper()
	rc, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get index %s: %v", key, err)
	}
	defer rc.Close()
	var idx checkpoint.CheckpointIndex
	if err := json.NewDecoder(rc).Decode(&idx); err != nil {
		t.Fatal(err)
	}
	return &idx
}

func exclusiveSST(t *testing.T, lowIdx, highIdx *checkpoint.CheckpointIndex) string {
	t.Helper()
	low := map[string]bool{}
	for _, k := range lowIdx.SSTFiles {
		low[k] = true
	}
	for _, k := range highIdx.SSTFiles {
		if !low[k] {
			return k
		}
	}
	t.Fatal("high checkpoint has no SST exclusive of the low checkpoint")
	return ""
}

func truncateObject(t *testing.T, store object.Store, key string) {
	t.Helper()
	ctx := context.Background()
	rc, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 4 {
		t.Fatalf("object %s too small to truncate (%d bytes)", key, len(data))
	}
	if err := store.Put(ctx, key, bytes.NewReader(data[:len(data)/2])); err != nil {
		t.Fatal(err)
	}
}

// ── ManifestStatus / ValidateManifestTarget ──────────────────────────────────

func TestReadManifestStatus(t *testing.T) {
	ctx := context.Background()
	t.Run("missing", func(t *testing.T) {
		st, err := testCP.ReadManifestStatus(ctx, object.NewMem())
		if err != nil || st == nil || !st.Missing || st.Corrupt {
			t.Fatalf("want missing, got %+v err=%v", st, err)
		}
	})
	t.Run("corrupt", func(t *testing.T) {
		store := object.NewMem()
		if err := store.Put(ctx, checkpoint.ManifestKey, strings.NewReader("{not json")); err != nil {
			t.Fatal(err)
		}
		st, err := testCP.ReadManifestStatus(ctx, store)
		if err != nil || st == nil || !st.Corrupt || st.Manifest != nil {
			t.Fatalf("want corrupt, got %+v err=%v", st, err)
		}
		if !strings.Contains(st.Reason, "decodable") {
			t.Errorf("reason=%q", st.Reason)
		}
	})
	t.Run("future format", func(t *testing.T) {
		store := object.NewMem()
		body := `{"format_version":42,"checkpoint_key":"x/manifest.json","revision":1,"term":1}`
		if err := store.Put(ctx, checkpoint.ManifestKey, strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		st, _ := testCP.ReadManifestStatus(ctx, store)
		if st == nil || !st.Corrupt {
			t.Fatalf("want corrupt future format, got %+v", st)
		}
	})
	t.Run("valid", func(t *testing.T) {
		store := object.NewMem()
		if err := testCP.WriteManifest(ctx, store, &checkpoint.Manifest{
			CheckpointKey: checkpoint.CheckpointIndexKey(1, 1), Term: 1, Revision: 3,
		}); err != nil {
			t.Fatal(err)
		}
		st, err := testCP.ReadManifestStatus(ctx, store)
		if err != nil || st == nil || st.Manifest == nil || st.Manifest.Revision != 3 {
			t.Fatalf("got %+v err=%v", st, err)
		}
	})
}

func TestValidateManifestTarget(t *testing.T) {
	store := object.NewMem()
	seedTwoCheckpoints(t, store)
	ctx := context.Background()

	good, err := testCP.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := testCP.ValidateManifestTarget(ctx, store, good); err != nil {
		t.Errorf("good manifest: %v", err)
	}
	missing := *good
	missing.CheckpointKey = checkpoint.CheckpointIndexKey(7, 777)
	if err := testCP.ValidateManifestTarget(ctx, store, &missing); err == nil {
		t.Error("manifest pointing at missing index should fail")
	}
}

// ── RepairManifest ────────────────────────────────────────────────────────────

func repairCandidateManifest(t *testing.T, store object.Store) *checkpoint.Manifest {
	t.Helper()
	_, highKey := seedTwoCheckpoints(t, store)
	idx := readTestIndex(t, store, highKey)
	return &checkpoint.Manifest{
		FormatVersion: idx.FormatVersion,
		CheckpointKey: highKey,
		Revision:      idx.Revision,
		LastSequence:  idx.LastSequence,
		Term:          idx.Term,
	}
}

func manifestBody(t *testing.T, store object.Store) []byte {
	t.Helper()
	rc, err := store.Get(context.Background(), checkpoint.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	return b
}

func TestRepairManifestMissing(t *testing.T) {
	store := object.NewMem()
	m := repairCandidateManifest(t, store)
	if err := store.Delete(context.Background(), checkpoint.ManifestKey); err != nil {
		t.Fatal(err)
	}
	out, err := testCP.RepairManifest(context.Background(), store, m, nil)
	if err != nil || !out.Written || out.AlreadyCurrent {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	got, _ := testCP.ReadManifest(context.Background(), store)
	if got.CheckpointKey != m.CheckpointKey {
		t.Errorf("manifest points to %s, want %s", got.CheckpointKey, m.CheckpointKey)
	}
}

func TestRepairManifestCorruptReplaced(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	m := repairCandidateManifest(t, store)
	if err := store.Put(ctx, checkpoint.ManifestKey, strings.NewReader("<<truncated>>")); err != nil {
		t.Fatal(err)
	}
	out, err := testCP.RepairManifest(ctx, store, m, nil)
	if err != nil || !out.Written {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if !bytes.Contains(manifestBody(t, store), []byte(m.CheckpointKey)) {
		t.Error("corrupt manifest was not replaced")
	}
}

func TestRepairManifestKeepsNewerValid(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	m := repairCandidateManifest(t, store) // term=1 seq=20

	// A peer published a higher-term manifest pointing at a complete index.
	newer := &checkpoint.Manifest{
		CheckpointKey: m.CheckpointKey, Term: 5, Revision: 20, LastSequence: 20,
	}
	if err := testCP.WriteManifest(ctx, store, newer); err != nil {
		t.Fatal(err)
	}
	before := manifestBody(t, store)
	out, err := testCP.RepairManifest(ctx, store, m, func(cur *checkpoint.Manifest) error {
		return testCP.ValidateManifestTarget(ctx, store, cur)
	})
	if err != nil || !out.AlreadyCurrent || out.Written {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	after := manifestBody(t, store)
	if !bytes.Equal(before, after) {
		t.Error("newer valid manifest was overwritten")
	}
}

func TestRepairManifestReplacesNewerButBroken(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	m := repairCandidateManifest(t, store)

	newer := &checkpoint.Manifest{
		CheckpointKey: checkpoint.CheckpointIndexKey(9, 999),
		Term:          9, Revision: 999, LastSequence: 999,
	}
	if err := testCP.WriteManifest(ctx, store, newer); err != nil {
		t.Fatal(err)
	}
	out, err := testCP.RepairManifest(ctx, store, m, func(cur *checkpoint.Manifest) error {
		return testCP.ValidateManifestTarget(ctx, store, cur)
	})
	if err != nil || !out.Written {
		t.Fatalf("newer-but-broken manifest must be repaired forward: out=%+v err=%v", out, err)
	}
	got, _ := testCP.ReadManifest(ctx, store)
	if got.CheckpointKey != m.CheckpointKey {
		t.Errorf("manifest now points to %s, want %s", got.CheckpointKey, m.CheckpointKey)
	}
}

func TestRepairManifestEqualAlreadyCurrent(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	m := repairCandidateManifest(t, store)
	if err := testCP.WriteManifest(ctx, store, m); err != nil {
		t.Fatal(err)
	}
	before := manifestBody(t, store)
	out, err := testCP.RepairManifest(ctx, store, m, nil)
	if err != nil || !out.AlreadyCurrent {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if !bytes.Equal(before, manifestBody(t, store)) {
		t.Error("equal manifest was rewritten")
	}
}

func TestRepairManifestOlderOverwritten(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	m := repairCandidateManifest(t, store)
	// Seed an older manifest directly (WriteManifest would rightly refuse to
	// move the pointer backwards).
	older := &checkpoint.Manifest{
		CheckpointKey: checkpoint.CheckpointIndexKey(1, 10),
		Term:          1, Revision: 10, LastSequence: 10,
	}
	ob, _ := json.Marshal(older)
	if err := store.Put(ctx, checkpoint.ManifestKey, bytes.NewReader(ob)); err != nil {
		t.Fatal(err)
	}
	out, err := testCP.RepairManifest(ctx, store, m, nil)
	if err != nil || !out.Written {
		t.Fatalf("older manifest must be repaired forward: out=%+v err=%v", out, err)
	}
}

// flakyCASStore loses the first PutIfMatch; the repair loop must re-read and
// retry the conditional write.
type flakyCASStore struct {
	*object.Mem
	putIfMatchCalls int32
}

func (f *flakyCASStore) PutIfMatch(ctx context.Context, key string, r io.Reader, etag string) error {
	if atomic.AddInt32(&f.putIfMatchCalls, 1) == 1 {
		return object.ErrPreconditionFailed
	}
	return f.Mem.PutIfMatch(ctx, key, r, etag)
}

func TestRepairManifestCASConflictConverges(t *testing.T) {
	mem := object.NewMem()
	store := &flakyCASStore{Mem: mem}
	ctx := context.Background()
	m := repairCandidateManifest(t, mem)
	// Pre-seed a corrupt manifest so the first action is a PutIfMatch.
	if err := mem.Put(ctx, checkpoint.ManifestKey, strings.NewReader("garbage")); err != nil {
		t.Fatal(err)
	}

	out, err := testCP.RepairManifest(ctx, store, m, nil)
	if err != nil || !out.Written {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if calls := atomic.LoadInt32(&store.putIfMatchCalls); calls < 2 {
		t.Errorf("expected a CAS retry, calls=%d", calls)
	}
	got, _ := testCP.ReadManifest(ctx, store)
	if got.CheckpointKey != m.CheckpointKey {
		t.Errorf("manifest points to %s, want %s", got.CheckpointKey, m.CheckpointKey)
	}
}

// TestRepairManifestSingleWriterPlainReplace covers the no-CAS fallback.
func TestRepairManifestSingleWriterPlainReplace(t *testing.T) {
	store := object.NewMem()
	cp := checkpoint.New(nil)
	cp.SetSingleWriter()
	ctx := context.Background()
	m := repairCandidateManifest(t, store)
	if err := store.Put(ctx, checkpoint.ManifestKey, strings.NewReader("garbage")); err != nil {
		t.Fatal(err)
	}
	out, err := cp.RepairManifest(ctx, store, m, nil)
	if err != nil || !out.Written {
		t.Fatalf("out=%+v err=%v", out, err)
	}

	// A newer valid one is still left untouched.
	newer := &checkpoint.Manifest{
		CheckpointKey: m.CheckpointKey, Term: m.Term + 1,
		Revision: m.Revision, LastSequence: m.LastSequence,
	}
	if err := cp.WriteManifest(ctx, store, newer); err != nil {
		t.Fatal(err)
	}
	out, err = cp.RepairManifest(ctx, store, m, nil)
	if err != nil || !out.AlreadyCurrent {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}
