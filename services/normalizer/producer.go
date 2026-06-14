package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(addr string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
		},
	}
}

// Publish sends value to topic using key for partition routing.
// Topic is set per-message so the producer stays reusable across topics.
func (p *KafkaProducer) Publish(ctx context.Context, topic, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	})
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
