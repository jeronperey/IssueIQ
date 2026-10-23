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
// If the original bytes are not valid JSON, they are encoded as a JSON string
// so the DLQEnvelope itself always marshals successfully.
func (d *DLQPublisher) Publish(ctx context.Context, originalMsg kafka.Message, reason string) error {
	m, err := d.Message(originalMsg, reason)
	if err != nil {
		return err
	}
	if err := d.producer.PublishBatch(ctx, []kafka.Message{m}); err != nil {
		return fmt.Errorf("dlq: publish to %s: %w", d.topic, err)
	}
	return nil
}

// Message builds the dead-letter record for originalMsg without sending it, so
// callers can publish many in one batch.
func (d *DLQPublisher) Message(originalMsg kafka.Message, reason string) (kafka.Message, error) {
	var payload json.RawMessage
	if json.Valid(originalMsg.Value) {
		payload = json.RawMessage(originalMsg.Value)
	} else {
		encoded, _ := json.Marshal(string(originalMsg.Value))
		payload = json.RawMessage(encoded)
	}

	envelope := schema.DLQEnvelope{
		SourceTopic:     originalMsg.Topic,
		Reason:          reason,
		OriginalPayload: payload,
		FailedAt:        time.Now().UTC(),
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("dlq: marshal envelope: %w", err)
	}
	return kafka.Message{Topic: d.topic, Key: originalMsg.Key, Value: data}, nil
}
