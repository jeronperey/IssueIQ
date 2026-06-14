//go:build ignore

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "raw-issues",
		GroupID:     "learn-group",
		StartOffset: kafka.FirstOffset, // new groups start from beginning, not latest
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nshutting down...")
		cancel()
	}()

	fmt.Println("consuming raw-issues (group: learn-group) — Ctrl+C to stop\n")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		var issue schema.RawIssue
		if err := json.Unmarshal(msg.Value, &issue); err != nil {
			log.Printf("unmarshal error at offset %d: %v", msg.Offset, err)
		} else {
			fmt.Printf("partition=%-2d offset=%-4d key=%-10s source=%-8s title=%q\n",
				msg.Partition, msg.Offset, string(msg.Key), issue.Source, issue.RawPayload["title"])
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("commit error: %v", err)
		}
	}

	fmt.Println("consumer stopped cleanly")
}
