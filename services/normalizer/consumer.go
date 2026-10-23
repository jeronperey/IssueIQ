package main

import (
	"context"
	"crypto/rand"
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
// Each batch is normalized and published to dstTopic (failures go to dlq) in a single
// Kafka write, and offsets are committed only after that write is acknowledged.
// A crash between publish and commit redelivers the batch; the downstream upsert is
// keyed by a stable ID, so redelivery is harmless.
func ConsumerLoop(
	ctx context.Context,
	reader *kafka.Reader,
	normalize func(schema.RawIssue) (schema.NormalizedIssue, error),
	publisher *KafkaProducer,
	dstTopic string,
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
		out, nOK, nDLQ := processBatch(batch, normalize, dstTopic, dlq)

		// Broker unavailable (restart, leader election) is retried, never dropped:
		// we simply do not commit until the write is acknowledged.
		err = kafkautil.Retry(ctx, "publish batch", 5*time.Second, func() { publishRetries.Inc() }, func() error {
			return publisher.PublishBatch(ctx, out)
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer: publish: %w", err)
		}

		if err := reader.CommitMessages(ctx, batch...); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Not fatal: uncommitted offsets are redelivered after the next rebalance
			// and re-processing is idempotent.
			commitErrors.Inc()
			log.Printf("consumer: commit batch of %d: %v", len(batch), err)
		}

		messagesTotal.WithLabelValues("ok").Add(float64(nOK))
		messagesTotal.WithLabelValues("dlq").Add(float64(nDLQ))
		batchSeconds.Observe(time.Since(start).Seconds())
		batchSizeHist.Observe(float64(len(batch)))
	}
}

// processBatch normalizes every message and returns the Kafka records to write:
// normalized events for good input, dead-letter records for bad input.
func processBatch(
	batch []kafka.Message,
	normalize func(schema.RawIssue) (schema.NormalizedIssue, error),
	dstTopic string,
	dlq *DLQPublisher,
) (out []kafka.Message, nOK, nDLQ int) {
	out = make([]kafka.Message, 0, len(batch))
	for _, msg := range batch {
		normalized, err := normalizeMessage(msg, normalize)
		if err != nil {
			m, merr := dlq.Message(msg, err.Error())
			if merr != nil {
				log.Printf("consumer: build dlq record: %v", merr)
				continue
			}
			out = append(out, m)
			nDLQ++
			continue
		}
		outBytes, err := json.Marshal(schema.EventEnvelope{
			EventID:    newEventID(),
			EventType:  "issue_normalized",
			Source:     normalized.Source,
			OccurredAt: time.Now().UTC(),
			Payload:    normalized,
		})
		if err != nil {
			log.Printf("consumer: marshal output envelope: %v", err)
			continue
		}
		out = append(out, kafka.Message{Topic: dstTopic, Key: []byte(normalized.ID), Value: outBytes})
		nOK++
	}
	return out, nOK, nDLQ
}

func normalizeMessage(msg kafka.Message, normalize func(schema.RawIssue) (schema.NormalizedIssue, error)) (schema.NormalizedIssue, error) {
	var envelope schema.EventEnvelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		return schema.NormalizedIssue{}, fmt.Errorf("unmarshal envelope: %v", err)
	}
	// Payload is typed as `any`, so JSON decode produces map[string]any — not RawIssue.
	payloadBytes, err := json.Marshal(envelope.Payload)
	if err != nil {
		return schema.NormalizedIssue{}, fmt.Errorf("marshal payload: %v", err)
	}
	var raw schema.RawIssue
	if err := json.Unmarshal(payloadBytes, &raw); err != nil {
		return schema.NormalizedIssue{}, fmt.Errorf("unmarshal raw issue: %v", err)
	}
	n, err := normalize(raw)
	if err != nil {
		return schema.NormalizedIssue{}, fmt.Errorf("normalize: %v", err)
	}
	return n, nil
}

func newEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
