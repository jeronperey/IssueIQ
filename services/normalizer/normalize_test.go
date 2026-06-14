package main

import (
	"testing"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
)

func rawIssue(payload map[string]any) schema.RawIssue {
	return schema.RawIssue{
		SourceID:   "gh-101",
		Source:     "github",
		RawPayload: payload,
		ReceivedAt: time.Now(),
	}
}

func TestNormalizeIssue_HappyPath(t *testing.T) {
	got, err := NormalizeIssue(rawIssue(map[string]any{
		"title":       "fix login bug",
		"description": "Reproducible on staging.",
		"status":      "OPEN",
		"priority":    "P1",
		"tags":        []string{"auth", "bug"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.SchemaVersion != "1.0" {
		t.Errorf("SchemaVersion: got %q, want %q", got.SchemaVersion, "1.0")
	}
	if got.Source != "github" {
		t.Errorf("Source: got %q, want %q", got.Source, "github")
	}
	if got.Title != "fix login bug" {
		t.Errorf("Title: got %q, want %q", got.Title, "fix login bug")
	}
	if got.Description != "Reproducible on staging." {
		t.Errorf("Description: got %q", got.Description)
	}
	if got.Status != "open" {
		t.Errorf("Status: got %q, want %q", got.Status, "open")
	}
	if got.Priority != "high" {
		t.Errorf("Priority: got %q, want %q", got.Priority, "high")
	}
	if got.ID == "" {
		t.Error("ID must not be empty")
	}
}

func TestNormalizeIssue_StatusVariants(t *testing.T) {
	cases := []struct{ raw, want string }{
		{"OPEN", "open"},
		{"Open", "open"},
		{"open", "open"},
		{"CLOSED", "closed"},
		{"closed", "closed"},
		{"IN_PROGRESS", "in_progress"},
		{"in-progress", "in_progress"},
		{"In Progress", "in_progress"},
		{"in progress", "in_progress"},
		{"unknown", "open"},
		{"", "open"},
	}
	for _, c := range cases {
		got := normalizeStatus(c.raw)
		if got != c.want {
			t.Errorf("normalizeStatus(%q) = %q, want %q", c.raw, got, c.want)
		}
	}
}

func TestNormalizeIssue_PriorityVariants(t *testing.T) {
	cases := []struct{ raw, want string }{
		{"P0", "critical"},
		{"urgent", "critical"},
		{"blocker", "critical"},
		{"P1", "high"},
		{"high", "high"},
		{"P2", "medium"},
		{"medium", "medium"},
		{"P3", "low"},
		{"low", "low"},
		{"unknown", "medium"},
		{"", "medium"},
	}
	for _, c := range cases {
		got := normalizePriority(c.raw)
		if got != c.want {
			t.Errorf("normalizePriority(%q) = %q, want %q", c.raw, got, c.want)
		}
	}
}

func TestNormalizeTags_DedupeAndLowercase(t *testing.T) {
	got := normalizeTags([]string{"Go", "  go  ", "GO"})
	if len(got) != 1 || got[0] != "go" {
		t.Errorf("normalizeTags([Go, go, GO]) = %v, want [go]", got)
	}
}

func TestNormalizeTags_SortedAndTrimmed(t *testing.T) {
	got := normalizeTags([]string{"  security  ", "auth", "Bug"})
	want := []string{"auth", "bug", "security"}
	if len(got) != len(want) {
		t.Fatalf("length: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("tags[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestNormalizeTags_NilReturnsEmptySlice(t *testing.T) {
	got := normalizeTags(nil)
	if got == nil {
		t.Error("normalizeTags(nil) returned nil, want empty slice")
	}
	if len(got) != 0 {
		t.Errorf("normalizeTags(nil) length = %d, want 0", len(got))
	}
}

func TestNormalizeIssue_MissingTitle(t *testing.T) {
	_, err := NormalizeIssue(rawIssue(map[string]any{
		"status": "OPEN",
	}))
	if err == nil {
		t.Error("expected error for missing title, got nil")
	}
}

func TestNormalizeIssue_WhitespaceTitle(t *testing.T) {
	_, err := NormalizeIssue(rawIssue(map[string]any{
		"title": "   ",
	}))
	if err == nil {
		t.Error("expected error for whitespace-only title, got nil")
	}
}

func TestNormalizeIssue_StableID(t *testing.T) {
	raw := rawIssue(map[string]any{"title": "some issue"})
	a, err := NormalizeIssue(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, err := NormalizeIssue(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.ID != b.ID {
		t.Errorf("IDs not stable: %q vs %q", a.ID, b.ID)
	}
	if a.ID == "" {
		t.Error("ID must not be empty")
	}
}

func TestNormalizeIssue_DifferentSourcesProduceDifferentIDs(t *testing.T) {
	payload := map[string]any{"title": "login bug"}
	github := schema.RawIssue{SourceID: "101", Source: "github", RawPayload: payload}
	zendesk := schema.RawIssue{SourceID: "101", Source: "zendesk", RawPayload: payload}

	a, _ := NormalizeIssue(github)
	b, _ := NormalizeIssue(zendesk)
	if a.ID == b.ID {
		t.Error("same source_id with different source should produce different IDs")
	}
}
