package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeronperey/issueiq/shared/kafkautil"
	"github.com/jeronperey/issueiq/shared/metrics"
	"github.com/segmentio/kafka-go"
)

func main() {
	kafkaAddr := getenv("KAFKA_BROKERS", "localhost:9092")
	srcTopic := getenv("RAW_ISSUES_TOPIC", "raw-issues")
	dstTopic := getenv("NORMALIZED_ISSUES_TOPIC", "normalized-issues")
	dlqTopic := getenv("DLQ_TOPIC", "dead-letter")
	group := getenv("CONSUMER_GROUP", "normalizer-group")

	log.Printf("normalizer service starting")
	log.Printf("  broker:    %s", kafkaAddr)
	log.Printf("  src:       %s", srcTopic)
	log.Printf("  dst:       %s", dstTopic)
	log.Printf("  dlq:       %s", dlqTopic)
	log.Printf("  group:     %s", group)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaAddr},
		Topic:   srcTopic,
		GroupID: group,
		// Big enough to let a batch fill from the fetch buffer instead of one round trip per message.
		MinBytes: 1,
		MaxBytes: 10 << 20,
		MaxWait:  50 * time.Millisecond,
	})
	defer reader.Close()

	producer := NewKafkaProducer(kafkaAddr)
	defer producer.Close()

	dlq := NewDLQPublisher(producer, dlqTopic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics.Serve(getenv("METRICS_ADDR", ":9101"))
	go kafkautil.LagGauge(ctx, reader, consumerLag.Set)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		log.Println("shutting down...")
		cancel()
	}()

	if err := ConsumerLoop(ctx, reader, NormalizeIssue, producer, dstTopic, dlq); err != nil {
		log.Printf("consumer loop error: %v", err)
		os.Exit(1)
	}

	log.Println("normalizer stopped cleanly")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
