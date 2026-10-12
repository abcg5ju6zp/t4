package wal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/t4db/t4/pkg/object"
)

// GCSegments deletes WAL segments from object storage whose entire sequence
// range is covered by checkpointSeq.
//
// A segment starting at firstRev[i] covers entries [firstRev[i], firstRev[i+1]-1].
// It is safe to delete when firstRev[i+1]-1 <= checkpointSeq, i.e.
// firstRev[i+1] <= checkpointSeq+1.
//
// The most recent segment that starts at or before the checkpoint is always
// retained as it may contain entries after checkpointSeq.
//
// Returns the number of segments deleted.
func GCSegments(ctx context.Context, store object.Store, checkpointSeq int64, log walLogger) (int, error) {
	toDelete, err := DeletableSegments(ctx, store, checkpointSeq)
	if err != nil {
		return 0, err
	}
	if len(toDelete) == 0 {
		return 0, nil
	}
	if err := store.DeleteMany(ctx, toDelete); err != nil {
		log.Warnf("wal gc: delete: %v", err)
		return 0, nil
	}
	for _, k := range toDelete {
		log.Debugf("wal gc: deleted %q (covered by checkpoint seq=%d)", k, checkpointSeq)
	}
	return len(toDelete), nil
}

// DeletableSegments returns the WAL object keys whose entire sequence range
// is covered by checkpointSeq, without deleting anything. The boundary rules
// are identical to GCSegments; callers that persist a GC plan first (so a
// later retry only touches the pinned list) use this instead of deleting
// inline.
func DeletableSegments(ctx context.Context, store object.Store, checkpointSeq int64) ([]string, error) {
	keys, err := store.List(ctx, "wal/")
	if err != nil {
		return nil, fmt.Errorf("wal gc: list: %w", err)
	}
	if len(keys) < 2 {
		return nil, nil // nothing or only one segment — nothing to GC
	}

	// keys are returned sorted (lexicographic == chronological given our naming).
	firstRevs := make([]int64, len(keys))
	for i, k := range keys {
		firstRevs[i] = parseFirstRev(k)
	}

	var toDelete []string
	for i := 0; i < len(keys)-1; i++ {
		nextFirstRev := firstRevs[i+1]
		// Segment i ends at nextFirstRev-1; safe to delete if that is <= checkpointSeq.
		if nextFirstRev-1 <= checkpointSeq {
			toDelete = append(toDelete, keys[i])
		}
	}
	return toDelete, nil
}

// parseFirstRev extracts the firstRev integer from a WAL object key of the
// form "wal/{term:010d}/{firstRev:020d}".
func parseFirstRev(key string) int64 {
	parts := strings.Split(key, "/")
	if len(parts) < 3 {
		return 0
	}
	rev, _ := strconv.ParseInt(strings.TrimLeft(parts[2], "0"), 10, 64)
	return rev
}
