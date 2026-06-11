package schema

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRawIssueRoundTrip(t *testing.T) {
	original := GenerateMockRawIssue()

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got RawIssue
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.SourceID != original.SourceID {
		t.Errorf("SourceID: got %q, want %q", got.SourceID, original.SourceID)
	}
	if got.Source != original.Source {
		t.Errorf("Source: got %q, want %q", got.Source, original.Source)
	}
	if got.ReceivedAt.IsZero() {
		t.Error("ReceivedAt: got zero time after round trip")
	}
	if len(got.RawPayload) == 0 {
		t.Error("RawPayload: empty after round trip")
	}
}

func TestNormalizedIssueRoundTrip(t *testing.T) {
	original := NormalizedIssue{
		SchemaVersion: "1.0",
		ID:            "abc123",
		Source:        "github",
		Title:         "fix login redirect loop",
		Description:   "Reproducible on staging.",
		Status:        "open",
		Priority:      "high",
		Tags:          []string{"auth", "bug"},
		CreatedAt:     time.Now().UTC().Truncate(time.Second),
		UpdatedAt:     time.Now().UTC().Truncate(time.Second),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got NormalizedIssue
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.SchemaVersion != original.SchemaVersion {
		t.Errorf("SchemaVersion: got %q, want %q", got.SchemaVersion, original.SchemaVersion)
	}
	if got.ID != original.ID {
		t.Errorf("ID: got %q, want %q", got.ID, original.ID)
	}
	if got.Source != original.Source {
		t.Errorf("Source: got %q, want %q", got.Source, original.Source)
	}
	if got.Title != original.Title {
		t.Errorf("Title: got %q, want %q", got.Title, original.Title)
	}
	if got.Status != original.Status {
		t.Errorf("Status: got %q, want %q", got.Status, original.Status)
	}
	if got.Priority != original.Priority {
		t.Errorf("Priority: got %q, want %q", got.Priority, original.Priority)
	}
	if len(got.Tags) != len(original.Tags) {
		t.Errorf("Tags length: got %d, want %d", len(got.Tags), len(original.Tags))
	}
	for i := range original.Tags {
		if got.Tags[i] != original.Tags[i] {
			t.Errorf("Tags[%d]: got %q, want %q", i, got.Tags[i], original.Tags[i])
		}
	}
}
