package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
)

type IngestHandler struct {
	producer *KafkaProducer
	topic    string
}

func (h *IngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var issue schema.RawIssue
	if err := json.NewDecoder(r.Body).Decode(&issue); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := ValidateRawIssue(issue); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	issue.ReceivedAt = time.Now().UTC()

	envelope := schema.EventEnvelope{
		EventID:    newEventID(),
		EventType:  "issue_created",
		Source:     issue.Source,
		OccurredAt: time.Now().UTC(),
		Payload:    issue,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("marshal envelope: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := h.producer.Publish(r.Context(), h.topic, issue.SourceID, data); err != nil {
		log.Printf("publish to kafka: %v", err)
		http.Error(w, "failed to queue event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "queued",
		"event_id": envelope.EventID,
	})
}

type BatchIngestHandler struct {
	producer *KafkaProducer
	topic    string
}

func (h *BatchIngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var issues []schema.RawIssue
	if err := json.NewDecoder(r.Body).Decode(&issues); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	var queued, failed int
	for _, issue := range issues {
		if err := ValidateRawIssue(issue); err != nil {
			log.Printf("validation failed for source_id=%q: %v", issue.SourceID, err)
			failed++
			continue
		}

		issue.ReceivedAt = time.Now().UTC()

		envelope := schema.EventEnvelope{
			EventID:    newEventID(),
			EventType:  "issue_created",
			Source:     issue.Source,
			OccurredAt: time.Now().UTC(),
			Payload:    issue,
		}

		data, err := json.Marshal(envelope)
		if err != nil {
			log.Printf("marshal envelope for source_id=%q: %v", issue.SourceID, err)
			failed++
			continue
		}

		if err := h.producer.Publish(r.Context(), h.topic, issue.SourceID, data); err != nil {
			log.Printf("publish source_id=%q: %v", issue.SourceID, err)
			failed++
			continue
		}

		queued++
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]int{
		"queued": queued,
		"failed": failed,
	})
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// newEventID returns a random UUID v4 string using crypto/rand.
func newEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
