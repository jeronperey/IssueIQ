package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "raw-issues",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	mocks := []schema.RawIssue{
		{
			SourceID: "gh-101",
			Source:   "github",
			RawPayload: map[string]any{
				"title":    "fix login redirect loop",
				"status":   "OPEN",
				"priority": "P1",
				"tags":     []string{"auth", "bug"},
			},
			ReceivedAt: time.Now(),
		},
		{
			SourceID: "gh-102",
			Source:   "github",
			RawPayload: map[string]any{
				"title":    "nil pointer in user service",
				"status":   "OPEN",
				"priority": "P0",
				"tags":     []string{"crash", "urgent"},
			},
			ReceivedAt: time.Now(),
		},
		{
			SourceID: "zd-55",
			Source:   "zendesk",
			RawPayload: map[string]any{
				"title":    "cannot export CSV report",
				"status":   "open",
				"priority": "high",
				"tags":     []string{"reporting"},
			},
			ReceivedAt: time.Now(),
		},
		{
			SourceID: "zd-56",
			Source:   "zendesk",
			RawPayload: map[string]any{
				"title":    "dashboard loads slowly for enterprise accounts",
				"status":   "In Progress",
				"priority": "medium",
				"tags":     []string{"performance", "enterprise"},
			},
			ReceivedAt: time.Now(),
		},
		{
			SourceID: "mock-1",
			Source:   "mock",
			RawPayload: map[string]any{
				"title":    "test issue for pipeline validation",
				"status":   "closed",
				"priority": "low",
				"tags":     []string{"test"},
			},
			ReceivedAt: time.Now(),
		},
	}

	ctx := context.Background()

	for i, issue := range mocks {
		data, err := json.Marshal(issue)
		if err != nil {
			log.Fatalf("marshal issue %d: %v", i, err)
		}

		if err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(issue.SourceID),
			Value: data,
		}); err != nil {
			log.Fatalf("publish issue %d: %v", i, err)
		}

		fmt.Printf("[%d/5] published key=%-10s source=%-8s title=%q\n",
			i+1, issue.SourceID, issue.Source, issue.RawPayload["title"])
	}

	fmt.Println("done — 5 messages published to raw-issues")
}
