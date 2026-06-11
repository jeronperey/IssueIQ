package schema

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	mockSources    = []string{"github", "zendesk", "mock"}
	mockStatuses   = []string{"OPEN", "Open", "open", "new", "pending", "closed"}
	mockPriorities = []string{"P0", "urgent", "high", "medium", "low"}
	mockTags       = []string{"bug", "auth", "ui", "backend", "billing", "performance", "security"}
	mockTitles     = []string{
		"login redirect loop after OAuth",
		"nil pointer in user service",
		"dashboard loads slowly for enterprise accounts",
		"cannot export CSV report",
		"billing page crashes on mobile",
		"search returns stale results",
		"session token not invalidated on logout",
		"UI misaligned on Safari 17",
	}
	mockDescriptions = []string{
		"Reproducible on staging. Affects all users.",
		"First reported by enterprise customer. Priority escalated.",
		"Intermittent. Appears under high load.",
		"No workaround available. Blocking release.",
		"Low severity. Nice to have before next sprint.",
	}
)

// GenerateMockRawIssue returns a realistic RawIssue with randomized fields
// for use in local testing and seed scripts.
func GenerateMockRawIssue() RawIssue {
	source := mockSources[rand.Intn(len(mockSources))]
	sourceID := fmt.Sprintf("%s-%d", source[:2], rand.Intn(9000)+1000)

	numTags := 2 + rand.Intn(3) // 2–4 tags
	tags := make([]string, 0, numTags)
	for _, i := range rand.Perm(len(mockTags))[:numTags] {
		tags = append(tags, mockTags[i])
	}

	now := time.Now().UTC()
	createdAt := now.Add(-time.Duration(rand.Intn(30*24)) * time.Hour)

	return RawIssue{
		SourceID: sourceID,
		Source:   source,
		RawPayload: map[string]any{
			"title":       mockTitles[rand.Intn(len(mockTitles))],
			"description": mockDescriptions[rand.Intn(len(mockDescriptions))],
			"status":      mockStatuses[rand.Intn(len(mockStatuses))],
			"priority":    mockPriorities[rand.Intn(len(mockPriorities))],
			"tags":        tags,
			"created_at":  createdAt.Format(time.RFC3339),
			"updated_at":  now.Format(time.RFC3339),
		},
		ReceivedAt: now,
	}
}
