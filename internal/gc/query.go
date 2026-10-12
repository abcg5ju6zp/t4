package gc

import (
	"context"

	"github.com/t4db/t4/pkg/object"
)

// ReadActive returns the in-progress or failed-but-resumable run, or nil if
// there is none.
func ReadActive(ctx context.Context, store object.Store) (*Run, error) {
	run, _, err := loadRun(ctx, store, ActiveKey)
	if err != nil {
		return nil, err
	}
	return run, nil
}

// ReadLatest returns the most recent terminal run result (success, failure
// or supersession), or nil if no run has ever finished.
func ReadLatest(ctx context.Context, store object.Store) (*Run, error) {
	run, _, err := loadRun(ctx, store, LatestKey)
	if err != nil {
		return nil, err
	}
	return run, nil
}

// ReadResult loads one terminal run by ID (gc/results/{id}.json).
func ReadResult(ctx context.Context, store object.Store, id string) (*Run, error) {
	run, _, err := loadRun(ctx, store, ResultKeyPrefix+id+".json")
	if err != nil {
		return nil, err
	}
	return run, nil
}
