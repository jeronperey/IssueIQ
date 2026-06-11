package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	kafkaAddr := getenv("KAFKA_ADDR", "localhost:9092")
	kafkaTopic := getenv("KAFKA_TOPIC", "raw-issues")

	producer := NewKafkaProducer(kafkaAddr)
	defer producer.Close()

	mux := http.NewServeMux()
	mux.Handle("POST /ingest", &IngestHandler{producer: producer, topic: kafkaTopic})
	mux.Handle("POST /ingest/batch", &BatchIngestHandler{producer: producer, topic: kafkaTopic})
	mux.HandleFunc("GET /health", healthHandler)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		log.Println("shutting down...")
		srv.Shutdown(context.Background())
	}()

	log.Printf("ingestion service starting on :8080 (kafka=%s topic=%s)", kafkaAddr, kafkaTopic)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
