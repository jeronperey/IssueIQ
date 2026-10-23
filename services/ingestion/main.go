package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeronperey/issueiq/shared/metrics"
)

func main() {
	kafkaAddr := getenv("KAFKA_ADDR", "localhost:9092")
	kafkaTopic := getenv("KAFKA_TOPIC", "raw-issues")

	metrics.Serve(getenv("METRICS_ADDR", ":9100"))
	httpAddr := getenv("HTTP_ADDR", ":8080")

	producer := NewKafkaProducer(kafkaAddr)
	defer producer.Close()

	mux := http.NewServeMux()
	mux.Handle("POST /ingest", &IngestHandler{producer: producer, topic: kafkaTopic})
	mux.Handle("POST /ingest/batch", &BatchIngestHandler{producer: producer, topic: kafkaTopic})
	mux.HandleFunc("GET /health", healthHandler)

	srv := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		log.Println("shutting down...")
		srv.Shutdown(context.Background())
	}()

	log.Printf("ingestion service starting on %s (kafka=%s topic=%s)", httpAddr, kafkaAddr, kafkaTopic)
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
