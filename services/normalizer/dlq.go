package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/segmentio/kafka-go"
)

type DLQPublisher struct {
	producer *KafkaProducer
	topic    string
}

func NewDLQPublisher(producer *KafkaProducer, topic string) *DLQPublisher {
	return &DLQPublisher{producer: producer, topic: topic}
}

// Publish wraps the original Kafka message in a DLQEnvelope and sends it to
// the dead-letter topic. The original message key is preserved so the failed
// event can be traced back to its source issue.
func (d *DLQPublisher) Publish(ctx context.Context, originalMsg kafka.Message, reason string) error {
	envelope := schema.DLQEnvelope{
		Reason:          reason,
		OriginalPayload: json.RawMessage(originalMsg.Value),
		FailedAt:        time.Now().UTC(),
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("dlq: marshal envelope: %w", err)
	}

	if err := d.producer.Publish(ctx, d.topic, string(originalMsg.Key), data); err != nil {
		return fmt.Errorf("dlq: publish to %s: %w", d.topic, err)
	}

	return nil
}
