// Package kafkautil holds the batching and retry helpers shared by the consumer services.
package kafkautil

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// FetchBatch blocks for the first message, then keeps collecting until max messages
// are held or wait has elapsed since the first one. Offsets are not committed.
// It returns ctx's error only when ctx itself is done.
func FetchBatch(ctx context.Context, r *kafka.Reader, max int, wait time.Duration) ([]kafka.Message, error) {
	first, err := r.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	batch := append(make([]kafka.Message, 0, max), first)
	fctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for len(batch) < max {
		m, err := r.FetchMessage(fctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || fctx.Err() != nil {
				break
			}
			return batch, err
		}
		batch = append(batch, m)
	}
	return batch, nil
}

// Retry runs f until it succeeds or ctx is done, backing off exponentially up to
// maxBackoff. Used for dependency outages (broker restart, OpenSearch down), where
// the right behavior is to stop consuming and not commit, not to drop data.
func Retry(ctx context.Context, what string, maxBackoff time.Duration, onRetry func(), f func() error) error {
	backoff := 100 * time.Millisecond
	for {
		err := f()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if onRetry != nil {
			onRetry()
		}
		log.Printf("%s failed, retrying in %s: %v", what, backoff, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// LagGauge periodically reports reader.Stats().Lag via set.
func LagGauge(ctx context.Context, r *kafka.Reader, set func(float64)) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			set(float64(r.Stats().Lag))
		}
	}
}
