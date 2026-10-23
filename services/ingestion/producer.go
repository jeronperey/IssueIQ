package main

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer builds a writer that hash-partitions by key (so one issue always
// lands on one partition) and waits for all in-sync replicas. BatchTimeout is short:
// callers hand over whole batches, so the writer should not sit on them.
func NewKafkaProducer(addr string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchSize:    500,
			BatchTimeout: 5 * time.Millisecond,
			MaxAttempts:  5,
		},
	}
}

// Publish sends value to topic using key for partition routing.
// Topic is set per-message so the producer stays reusable across topics.
func (p *KafkaProducer) Publish(ctx context.Context, topic, key string, value []byte) error {
	return p.PublishBatch(ctx, []kafka.Message{{Topic: topic, Key: []byte(key), Value: value}})
}

// PublishBatch sends msgs in one WriteMessages call. Each message must set Topic.
func (p *KafkaProducer) PublishBatch(ctx context.Context, msgs []kafka.Message) error {
	return p.writer.WriteMessages(ctx, msgs...)
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
