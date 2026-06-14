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
	osURL := getenv("OPENSEARCH_URL", "http://localhost:9200")
	osIndex := getenv("OPENSEARCH_INDEX", "issues")

	log.Printf("query service starting on :8082")
	log.Printf("  opensearch: %s  index=%s", osURL, osIndex)

	osc := NewOpenSearchClient(osURL, osIndex)
	h := &handlers{osc: osc}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("GET /issues/search", h.searchIssues)
	mux.HandleFunc("GET /issues/stats", h.statsHandler)
	mux.HandleFunc("GET /issues/aggregate", h.aggregateHandler)
	mux.HandleFunc("GET /issues", h.listIssues)
	mux.HandleFunc("GET /issues/{id}", h.issueByID)

	srv := &http.Server{
		Addr:    ":8082",
		Handler: mux,
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		log.Println("shutting down...")
		srv.Shutdown(context.Background())
	}()

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
