package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	kafkaAddr := getenv("KAFKA_BROKERS", "localhost:9092")
	srcTopic := getenv("NORMALIZED_ISSUES_TOPIC", "normalized-issues")
	dlqTopic := getenv("DLQ_TOPIC", "dead-letter")
	group := getenv("CONSUMER_GROUP", "indexer-group")
	osURL := getenv("OPENSEARCH_URL", "http://localhost:9200")
	osIndex := getenv("OPENSEARCH_INDEX", "issues")

	log.Printf("indexer service starting")
	log.Printf("  broker:     %s", kafkaAddr)
	log.Printf("  src:        %s", srcTopic)
	log.Printf("  dlq:        %s", dlqTopic)
	log.Printf("  group:      %s", group)
	log.Printf("  opensearch: %s  index=%s", osURL, osIndex)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaAddr},
		Topic:   srcTopic,
		GroupID: group,
	})
	defer reader.Close()

	producer := NewKafkaProducer(kafkaAddr)
	defer producer.Close()

	dlq := NewDLQPublisher(producer, dlqTopic)
	osc := NewOpenSearchClient(osURL, osIndex)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		log.Println("shutting down...")
		cancel()
	}()

	if err := osc.EnsureIndex(ctx); err != nil {
		log.Fatalf("indexer: ensure index: %v", err)
	}
	log.Printf("index %q ready", osIndex)

	if err := ConsumerLoop(ctx, reader, osc, dlq); err != nil {
		log.Printf("consumer loop error: %v", err)
		os.Exit(1)
	}

	log.Println("indexer stopped cleanly")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
