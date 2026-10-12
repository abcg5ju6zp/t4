package object

import (
	"context"
	"io"
	"time"

	"github.com/t4db/t4/internal/metrics"
)

// instrumentedStore wraps a Store and records Prometheus metrics for every
// operation: t4_object_store_ops_total{op, result} and
// t4_object_store_duration_seconds{op}.
type instrumentedStore struct{ inner Store }

// instrumentedConditionalStore additionally implements ConditionalStore.
type instrumentedConditionalStore struct {
	instrumentedStore
	inner ConditionalStore
}

type instrumentedVersionedStore struct {
	instrumentedStore
	inner VersionedStore
}

type instrumentedConditionalVersionedStore struct {
	instrumentedStore
	conditional ConditionalStore
	versioned   VersionedStore
}

// NewInstrumentedStore wraps s with metrics instrumentation. If s also
// implements optional Store extensions, the returned value implements them too,
// so callers that type-assert to ConditionalStore or VersionedStore continue to
// work.
func NewInstrumentedStore(s Store) Store {
	cs, hasConditional := s.(ConditionalStore)
	vs, hasVersioned := s.(VersionedStore)
	switch {
	case hasConditional && hasVersioned:
		return &instrumentedConditionalVersionedStore{
			instrumentedStore: instrumentedStore{inner: s},
			conditional:       cs,
			versioned:         vs,
		}
	case hasConditional:
		return &instrumentedConditionalStore{
			instrumentedStore: instrumentedStore{inner: s},
			inner:             cs,
		}
	case hasVersioned:
		return &instrumentedVersionedStore{
			instrumentedStore: instrumentedStore{inner: s},
			inner:             vs,
		}
	default:
		return &instrumentedStore{inner: s}
	}
}

func record(op string, start time.Time, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	metrics.ObjectStoreOpsTotal.WithLabelValues(op, result).Inc()
	metrics.ObjectStoreDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}

func (s *instrumentedStore) Put(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	err := s.inner.Put(ctx, key, r)
	record("put", start, err)
	return err
}

func (s *instrumentedStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.inner.Get(ctx, key)
	record("get", start, err)
	return rc, err
}

func (s *instrumentedStore) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := s.inner.Delete(ctx, key)
	record("delete", start, err)
	return err
}

func (s *instrumentedStore) Stat(ctx context.Context, key string) error {
	start := time.Now()
	var err error
	if st, ok := s.inner.(Stater); ok {
		err = st.Stat(ctx, key)
	} else {
		// Decorated store cannot HEAD: fall back to a short GET so the
		// Stater contract still holds for callers.
		var rc io.ReadCloser
		rc, err = s.inner.Get(ctx, key)
		if err == nil {
			_ = rc.Close()
		}
	}
	record("stat", start, err)
	return err
}

func (s *instrumentedStore) DeleteMany(ctx context.Context, keys []string) error {
	start := time.Now()
	err := s.inner.DeleteMany(ctx, keys)
	record("delete_many", start, err)
	return err
}

func (s *instrumentedStore) List(ctx context.Context, prefix string) ([]string, error) {
	start := time.Now()
	keys, err := s.inner.List(ctx, prefix)
	record("list", start, err)
	return keys, err
}

func (s *instrumentedConditionalStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	start := time.Now()
	res, err := s.inner.GetETag(ctx, key)
	record("get_etag", start, err)
	return res, err
}

func (s *instrumentedConditionalStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	err := s.inner.PutIfAbsent(ctx, key, r)
	record("put_if_absent", start, err)
	return err
}

func (s *instrumentedConditionalStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	start := time.Now()
	err := s.inner.PutIfMatch(ctx, key, r, matchETag)
	record("put_if_match", start, err)
	return err
}

func (s *instrumentedVersionedStore) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.inner.GetVersioned(ctx, key, versionID)
	record("get_versioned", start, err)
	return rc, err
}

func (s *instrumentedConditionalVersionedStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	start := time.Now()
	res, err := s.conditional.GetETag(ctx, key)
	record("get_etag", start, err)
	return res, err
}

func (s *instrumentedConditionalVersionedStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	err := s.conditional.PutIfAbsent(ctx, key, r)
	record("put_if_absent", start, err)
	return err
}

func (s *instrumentedConditionalVersionedStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	start := time.Now()
	err := s.conditional.PutIfMatch(ctx, key, r, matchETag)
	record("put_if_match", start, err)
	return err
}

func (s *instrumentedConditionalVersionedStore) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.versioned.GetVersioned(ctx, key, versionID)
	record("get_versioned", start, err)
	return rc, err
}
