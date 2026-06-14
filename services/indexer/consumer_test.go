package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
)

func makeEnvelopeJSON(t *testing.T, eventType string, payload any) []byte {
	t.Helper()
	env := schema.EventEnvelope{
		EventID:    "test-event-id",
		EventType:  eventType,
		Source:     "github",
		OccurredAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Payload:    payload,
	}
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal test envelope: %v", err)
	}
	return data
}

func testNormalizedIssue() schema.NormalizedIssue {
	return schema.NormalizedIssue{
		SchemaVersion: "1.0",
		ID:            "deadbeef1234",
		Source:        "github",
		Title:         "fix login bug",
		Status:        "open",
		Priority:      "high",
		Tags:          []string{"auth", "bug"},
		CreatedAt:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestParseNormalizedIssueFromEnvelope_Valid(t *testing.T) {
	want := testNormalizedIssue()
	data := makeEnvelopeJSON(t, "issue_normalized", want)

	got, err := parseNormalizedIssueFromEnvelope(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("ID: got %q, want %q", got.ID, want.ID)
	}
	if got.Title != want.Title {
		t.Errorf("Title: got %q, want %q", got.Title, want.Title)
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %q, want %q", got.Status, want.Status)
	}
	if got.Priority != want.Priority {
		t.Errorf("Priority: got %q, want %q", got.Priority, want.Priority)
	}
	if got.Source != want.Source {
		t.Errorf("Source: got %q, want %q", got.Source, want.Source)
	}
}

func TestParseNormalizedIssueFromEnvelope_MalformedJSON(t *testing.T) {
	_, err := parseNormalizedIssueFromEnvelope([]byte("not json at all"))
	if err == nil {
		t.Error("expected error for malformed JSON, got nil")
	}
}

func TestParseNormalizedIssueFromEnvelope_WrongEventType(t *testing.T) {
	data := makeEnvelopeJSON(t, "some_other_event", testNormalizedIssue())
	_, err := parseNormalizedIssueFromEnvelope(data)
	if err == nil {
		t.Error("expected error for wrong event type, got nil")
	}
}
