package schema

import "time"

type RawIssue struct {
	SourceID   string         `json:"source_id"`
	Source     string         `json:"source"`
	RawPayload map[string]any `json:"raw_payload"`
	ReceivedAt time.Time      `json:"received_at"`
}
