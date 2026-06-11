package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/segmentio/kafka-go"
)

// ConsumerLoop reads from reader until ctx is cancelled or a fatal error occurs.
// Each message is normalized and published to dstTopic, or sent to dlq on failure.
// Offsets are committed only after a successful publish (to either destination).
func ConsumerLoop(
	ctx context.Context,
	reader *kafka.Reader,
	normalize func(schema.RawIssue) (schema.NormalizedIssue, error),
	publisher *KafkaProducer,
	dstTopic string,
	dlq *DLQPublisher,
) error {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown via context cancellation
			}
			return fmt.Errorf("consumer: fetch: %w", err)
		}

		if err := handleMessage(ctx, msg, normalize, publisher, dstTopic, dlq); err != nil {
			return err // publish or DLQ failed — leave offset uncommitted
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer: commit offset %d: %w", msg.Offset, err)
		}
	}
}

// handleMessage processes a single Kafka message.
// Returns nil if the message was handled (either published or sent to DLQ).
// Returns an error only if DLQ or normalized publish itself failed — in that case
// the caller must not commit the offset.
func handleMessage(
	ctx context.Context,
	msg kafka.Message,
	normalize func(schema.RawIssue) (schema.NormalizedIssue, error),
	publisher *KafkaProducer,
	dstTopic string,
	dlq *DLQPublisher,
) error {
	// Step 1: unmarshal outer EventEnvelope
	var envelope schema.EventEnvelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		return dlq.Publish(ctx, msg, fmt.Sprintf("unmarshal envelope: %v", err))
	}

	// Step 2: re-marshal Payload back to bytes, then unmarshal into RawIssue.
	// Payload is typed as `any`, so JSON decode produces map[string]any — not RawIssue.
	payloadBytes, err := json.Marshal(envelope.Payload)
	if err != nil {
		return dlq.Publish(ctx, msg, fmt.Sprintf("marshal payload: %v", err))
	}
	var raw schema.RawIssue
	if err := json.Unmarshal(payloadBytes, &raw); err != nil {
		return dlq.Publish(ctx, msg, fmt.Sprintf("unmarshal raw issue: %v", err))
	}

	// Step 3: normalize
	normalized, err := normalize(raw)
	if err != nil {
		return dlq.Publish(ctx, msg, fmt.Sprintf("normalize: %v", err))
	}

	// Step 4: wrap in a new EventEnvelope and publish to normalized-issues
	outEnvelope := schema.EventEnvelope{
		EventID:    newEventID(),
		EventType:  "issue_normalized",
		Source:     normalized.Source,
		OccurredAt: time.Now().UTC(),
		Payload:    normalized,
	}
	outBytes, err := json.Marshal(outEnvelope)
	if err != nil {
		return fmt.Errorf("consumer: marshal output envelope: %w", err)
	}
	if err := publisher.Publish(ctx, dstTopic, normalized.ID, outBytes); err != nil {
		return fmt.Errorf("consumer: publish to %s: %w", dstTopic, err)
	}

	return nil
}

func newEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
