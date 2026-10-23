package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jeronperey/issueiq/shared/kafkautil"
	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/segmentio/kafka-go"
)

const (
	batchSize = 500
	batchWait = 20 * time.Millisecond
)

// ConsumerLoop reads batches from reader until ctx is cancelled or the reader fails.
// Each batch is written to OpenSearch with one _bulk request. Documents OpenSearch
// permanently rejects, and messages that cannot be parsed, go to the DLQ. Transient
// failures (OpenSearch down or pushing back) are retried with backoff and never
// dead-lettered. Offsets are committed only after every message in the batch has
// been indexed or dead-lettered.
func ConsumerLoop(
	ctx context.Context,
	reader *kafka.Reader,
	osc *OpenSearchClient,
	dlq *DLQPublisher,
) error {
	for {
		batch, err := kafkautil.FetchBatch(ctx, reader, batchSize, batchWait)
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown via context cancellation
			}
			return fmt.Errorf("consumer: fetch: %w", err)
		}

		start := time.Now()
		if err := handleBatch(ctx, batch, osc, dlq); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err // leave offsets uncommitted; messages replay on restart
		}

		if err := reader.CommitMessages(ctx, batch...); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Not fatal: uncommitted offsets are redelivered and the upsert is idempotent.
			commitErrors.Inc()
			log.Printf("consumer: commit batch of %d: %v", len(batch), err)
		}
		batchSeconds.Observe(time.Since(start).Seconds())
		batchSizeHist.Observe(float64(len(batch)))
	}
}

// handleBatch indexes or dead-letters every message in batch. It returns an error
// only if ctx ends or the DLQ cannot be written.
func handleBatch(ctx context.Context, batch []kafka.Message, osc *OpenSearchClient, dlq *DLQPublisher) error {
	var dead []kafka.Message
	deadLetter := func(msg kafka.Message, reason string) {
		m, err := dlq.Message(msg, reason)
		if err != nil {
			log.Printf("consumer: build dlq record: %v", err)
			return
		}
		dead = append(dead, m)
	}

	type pendingDoc struct {
		msg   kafka.Message
		issue schema.NormalizedIssue
	}
	pending := make([]pendingDoc, 0, len(batch))
	for _, msg := range batch {
		issue, err := parseNormalizedIssueFromEnvelope(msg.Value)
		if err != nil {
			deadLetter(msg, err.Error())
			continue
		}
		pending = append(pending, pendingDoc{msg, issue})
	}

	backoff := 100 * time.Millisecond
	for len(pending) > 0 {
		issues := make([]schema.NormalizedIssue, len(pending))
		for i, p := range pending {
			issues[i] = p.issue
		}
		bulkStart := time.Now()
		results, err := osc.BulkUpsert(ctx, issues)
		bulkSeconds.Observe(time.Since(bulkStart).Seconds())

		var retry []pendingDoc
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			retries.Inc()
			log.Printf("consumer: bulk of %d failed, retrying in %s: %v", len(pending), backoff, err)
			retry = pending
		default:
			for i, r := range results {
				switch {
				case r.ok():
					docsTotal.WithLabelValues("indexed").Inc()
					if p := pending[i].issue; !p.IngestedAt.IsZero() {
						latency.Observe(time.Since(p.IngestedAt).Seconds())
					}
				case r.transient():
					retry = append(retry, pending[i])
				default:
					deadLetter(pending[i].msg, fmt.Sprintf("opensearch rejected (status %d): %s", r.Status, r.Err))
				}
			}
			if len(retry) > 0 {
				retries.Add(float64(len(retry)))
				log.Printf("consumer: %d/%d docs throttled or failed transiently, retrying in %s", len(retry), len(pending), backoff)
			}
		}

		pending = retry
		if len(pending) > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 5*time.Second)
		}
	}

	if len(dead) > 0 {
		err := kafkautil.Retry(ctx, "publish dlq batch", 5*time.Second, func() { retries.Inc() }, func() error {
			return dlq.producer.PublishBatch(ctx, dead)
		})
		if err != nil {
			return fmt.Errorf("consumer: dlq: %w", err)
		}
		docsTotal.WithLabelValues("dlq").Add(float64(len(dead)))
	}
	return nil
}

// parseNormalizedIssueFromEnvelope decodes an EventEnvelope from raw bytes and
// extracts its NormalizedIssue payload. Separated from handleMessage so it can
// be unit-tested without a Kafka reader.
func parseNormalizedIssueFromEnvelope(data []byte) (schema.NormalizedIssue, error) {
	var envelope schema.EventEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return schema.NormalizedIssue{}, fmt.Errorf("unmarshal envelope: %v", err)
	}

	if envelope.EventType != "issue_normalized" {
		return schema.NormalizedIssue{}, fmt.Errorf("unexpected event_type %q", envelope.EventType)
	}

	// Payload is typed as `any`, so JSON decode produces map[string]any — not NormalizedIssue.
	payloadBytes, err := json.Marshal(envelope.Payload)
	if err != nil {
		return schema.NormalizedIssue{}, fmt.Errorf("marshal payload: %v", err)
	}

	var issue schema.NormalizedIssue
	if err := json.Unmarshal(payloadBytes, &issue); err != nil {
		return schema.NormalizedIssue{}, fmt.Errorf("unmarshal normalized issue: %v", err)
	}

	return issue, nil
}
