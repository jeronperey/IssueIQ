package schema

import "time"

// RawIssue is the unprocessed event published to the raw-issues Kafka topic by the ingestion service.
type RawIssue struct {
	SourceID   string         `json:"source_id"`
	Source     string         `json:"source"`
	RawPayload map[string]any `json:"raw_payload"`
	ReceivedAt time.Time      `json:"received_at"`
}

// NormalizedIssue is the canonical form written to OpenSearch by the indexer after normalization.
type NormalizedIssue struct {
	SchemaVersion string    `json:"schema_version"`
	ID            string    `json:"id"`
	Source        string    `json:"source"`
	Title         string    `json:"title"`
	Description   string    `json:"description"`
	Status        string    `json:"status"`
	Priority      string    `json:"priority"`
	Tags          []string  `json:"tags"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// EventEnvelope wraps every Kafka message with routing metadata so consumers
// can route and trace events without inspecting the payload.
type EventEnvelope struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	Source     string    `json:"source"`
	OccurredAt time.Time `json:"occurred_at"`
	Payload    any       `json:"payload"`
}
