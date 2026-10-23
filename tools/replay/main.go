// replay re-publishes dead-lettered messages to the topic they originally failed on.
//
//	go run ./tools/replay -reason-contains opensearch      # replay only outage-related failures
//	go run ./tools/replay -dry-run                          # show what would be replayed
//
// It reads the dead-letter topic through a committed consumer group, so running it
// twice does not replay the same record twice. Records whose original bytes were not
// valid JSON are skipped: replaying them could only fail the same way.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/segmentio/kafka-go"
)

func main() {
	var (
		brokers  = flag.String("brokers", "localhost:9092", "kafka broker")
		dlqTopic = flag.String("dlq-topic", "dead-letter", "dead-letter topic")
		group    = flag.String("group", "dlq-replayer", "consumer group (offsets make replay at-most-once per record)")
		contains = flag.String("reason-contains", "", "only replay records whose reason contains this substring")
		fallback = flag.String("default-topic", "", "destination for legacy records without source_topic")
		dryRun   = flag.Bool("dry-run", false, "print without publishing or committing")
		idle     = flag.Duration("idle", 3*time.Second, "stop after the topic is quiet this long")
	)
	flag.Parse()

	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{*brokers}, Topic: *dlqTopic, GroupID: *group, MinBytes: 1, MaxBytes: 10 << 20, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	if *dryRun {
		// a dry run must not move the group, so read without one
		r.Close()
		r = kafka.NewReader(kafka.ReaderConfig{Brokers: []string{*brokers}, Topic: *dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10 << 20})
	}
	w := &kafka.Writer{Addr: kafka.TCP(*brokers), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchTimeout: 5 * time.Millisecond}
	defer w.Close()

	var replayed, skipped, filtered int
	wait := 20 * time.Second // first read includes the consumer-group join
	for {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		wait = *idle
		var m kafka.Message
		var err error
		if *dryRun {
			m, err = r.ReadMessage(ctx)
		} else {
			m, err = r.FetchMessage(ctx)
		}
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			log.Fatalf("read: %v", err)
		}

		var env schema.DLQEnvelope
		if err := json.Unmarshal(m.Value, &env); err != nil {
			skipped++
		} else if *contains != "" && !strings.Contains(env.Reason, *contains) {
			filtered++
		} else if dest := firstNonEmpty(env.SourceTopic, *fallback); dest == "" || len(env.OriginalPayload) == 0 || env.OriginalPayload[0] == '"' {
			skipped++
		} else if *dryRun {
			log.Printf("would replay key=%s -> %s (%s)", m.Key, dest, env.Reason)
			replayed++
		} else {
			if err := w.WriteMessages(context.Background(), kafka.Message{Topic: dest, Key: m.Key, Value: env.OriginalPayload}); err != nil {
				log.Fatalf("republish: %v", err)
			}
			replayed++
		}
		if !*dryRun {
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Fatalf("commit: %v", err)
			}
		}
	}
	log.Printf("replayed=%d skipped(unreplayable)=%d filtered(out by reason)=%d", replayed, skipped, filtered)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
