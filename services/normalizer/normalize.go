package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
)

// canonicalKey lowercases, trims, and collapses spaces/hyphens to underscores
// so that "In Progress", "in-progress", "IN_PROGRESS" all map to "in_progress".
func canonicalKey(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return s
}

// Keys are pre-canonicalized; normalizeStatus/normalizePriority apply canonicalKey
// before lookup so case and separator variants are handled automatically.
var statusMap = map[string]string{
	"open":        "open",
	"new":         "open",
	"pending":     "open",
	"closed":      "closed",
	"in_progress": "in_progress",
}

var priorityMap = map[string]string{
	"p0":      "critical",
	"urgent":  "critical",
	"blocker": "critical",
	"p1":      "high",
	"high":    "high",
	"p2":      "medium",
	"medium":  "medium",
	"p3":      "low",
	"low":     "low",
}

// NormalizeIssue converts a RawIssue into a canonical NormalizedIssue.
// No I/O or external calls; timestamps default to time.Now() when absent from payload.
func NormalizeIssue(raw schema.RawIssue) (schema.NormalizedIssue, error) {
	title := strings.TrimSpace(getString(raw.RawPayload, "title"))
	if title == "" {
		return schema.NormalizedIssue{}, fmt.Errorf("title is missing or empty")
	}

	updatedAt := time.Now().UTC()
	if t, ok := getTime(raw.RawPayload, "updated_at"); ok {
		updatedAt = t
	}

	createdAt := time.Now().UTC()
	if t, ok := getTime(raw.RawPayload, "created_at"); ok {
		createdAt = t
	}

	return schema.NormalizedIssue{
		SchemaVersion: "1.0",
		ID:            generateID(raw.Source, raw.SourceID),
		Source:        raw.Source,
		Title:         title,
		Description:   getString(raw.RawPayload, "description"),
		Status:        normalizeStatus(getString(raw.RawPayload, "status")),
		Priority:      normalizePriority(getString(raw.RawPayload, "priority")),
		Tags:          normalizeTags(getStringSlice(raw.RawPayload, "tags")),
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
		IngestedAt:    raw.ReceivedAt,
	}, nil
}

func normalizeStatus(raw string) string {
	if canonical, ok := statusMap[canonicalKey(raw)]; ok {
		return canonical
	}
	return "open"
}

func normalizePriority(raw string) string {
	if canonical, ok := priorityMap[canonicalKey(raw)]; ok {
		return canonical
	}
	return "medium"
}

func normalizeTags(raw []string) []string {
	if len(raw) == 0 {
		return []string{}
	}
	seen := make(map[string]bool)
	result := []string{}
	for _, tag := range raw {
		t := strings.ToLower(strings.TrimSpace(tag))
		if t != "" && !seen[t] {
			seen[t] = true
			result = append(result, t)
		}
	}
	sort.Strings(result)
	return result
}

func generateID(source, sourceID string) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s", source, sourceID)))
	return hex.EncodeToString(h[:])
}

// getString safely extracts a string value from a map[string]any.
func getString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// getStringSlice handles both []string (from Go) and []any (from JSON unmarshal).
func getStringSlice(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	switch val := v.(type) {
	case []string:
		return val
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// getTime parses an RFC3339 timestamp string from a map[string]any.
func getTime(m map[string]any, key string) (time.Time, bool) {
	s := getString(m, key)
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
