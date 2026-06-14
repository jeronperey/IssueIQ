package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/segmentio/kafka-go"
)

// ConsumerLoop reads from reader until ctx is cancelled or a fatal error occurs.
// Each message is upserted into OpenSearch, or sent to dlq on failure.
// Offsets are committed only after a successful upsert or successful DLQ publish.
func ConsumerLoop(
	ctx context.Context,
	reader *kafka.Reader,
	osc *OpenSearchClient,
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

		if err := handleMessage(ctx, msg, osc, dlq); err != nil {
			return err // DLQ publish failed — leave offset uncommitted
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
// Returns nil if the message was handled (upserted or sent to DLQ).
// Returns an error only if the DLQ publish itself failed — the caller must not
// commit the offset so the message replays on restart.
func handleMessage(
	ctx context.Context,
	msg kafka.Message,
	osc *OpenSearchClient,
	dlq *DLQPublisher,
) error {
	issue, err := parseNormalizedIssueFromEnvelope(msg.Value)
	if err != nil {
		return dlq.Publish(ctx, msg, err.Error())
	}

	if err := osc.UpsertDocument(ctx, issue); err != nil {
		return dlq.Publish(ctx, msg, fmt.Sprintf("opensearch upsert: %v", err))
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
