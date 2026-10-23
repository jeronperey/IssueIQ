// loadgen drives synthetic issue events through IssueIQ and reports sustained
// throughput, ingest-to-searchable latency and a correctness check against OpenSearch.
//
//	go run ./tools/loadgen -mode kafka -count 200000
//	go run ./tools/loadgen -mode kafka -rate 5000 -duration 60s -probe 200ms
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/jeronperey/issueiq/tools/loadtest"
)

type summary struct {
	Label         string  `json:"label"`
	Mode          string  `json:"mode"`
	RunID         string  `json:"run_id"`
	TargetRate    int     `json:"target_rate"`
	Partitions    string  `json:"partitions,omitempty"`
	Acked         int64   `json:"acked"`
	Indexed       int     `json:"indexed"`
	Missing       int     `json:"missing"`
	Unexpected    int     `json:"unexpected"`
	ProduceSecs   float64 `json:"produce_secs"`
	TotalSecs     float64 `json:"total_secs"`
	OverallEPS    float64 `json:"overall_events_per_sec"`
	SteadyEPS     float64 `json:"steady_state_events_per_sec"`
	ServerP50Ms   float64 `json:"ingest_to_indexed_p50_ms"`
	ServerP99Ms   float64 `json:"ingest_to_indexed_p99_ms"`
	ProbeN        int     `json:"probe_samples"`
	ProbeP50Ms    float64 `json:"searchable_p50_ms"`
	ProbeP99Ms    float64 `json:"searchable_p99_ms"`
	ProbeMaxMs    float64 `json:"searchable_max_ms"`
	DLQDelta      int64   `json:"dlq_delta"`
	MalformedSent int64   `json:"malformed_sent"`
	Redelivered   int64   `json:"redelivered_writes"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func main() {
	var (
		mode     = flag.String("mode", "kafka", "kafka (write to raw-issues) or http (POST /ingest)")
		brokers  = flag.String("brokers", "localhost:9092", "kafka broker")
		topic    = flag.String("topic", "raw-issues", "raw topic")
		dlqTopic = flag.String("dlq-topic", "dead-letter", "dead-letter topic (for malformed accounting)")
		ingest   = flag.String("ingest-url", "http://localhost:8080", "ingestion service base URL")
		osURL    = flag.String("os-url", "http://localhost:9200", "OpenSearch URL")
		index    = flag.String("index", "issues", "OpenSearch index")
		count    = flag.Int("count", 0, "number of events (0 = use -duration)")
		duration = flag.Duration("duration", 30*time.Second, "produce duration when -count is 0")
		rate     = flag.Int("rate", 0, "target events/sec (0 = as fast as possible)")
		workers  = flag.Int("workers", 64, "concurrent senders")
		descSize = flag.Int("desc-bytes", 400, "description size per event")
		probe    = flag.Duration("probe", 0, "emit a searchable-latency probe event this often (0 = off)")
		badPct   = flag.Float64("malformed", 0, "fraction of malformed events to inject (e.g. 0.01)")
		label    = flag.String("label", "", "free-form label stored in the summary")
		parts    = flag.String("partitions", "", "partition count, recorded in the summary only")
		out      = flag.String("out", "", "append the JSON summary to this file")
		runID    = flag.String("run-id", fmt.Sprintf("r%d", time.Now().Unix()), "run id (lowercase alnum)")
		stall    = flag.Duration("stall", 60*time.Second, "give up draining if the count stops moving this long")
	)
	flag.Parse()

	ctx := context.Background()
	osc := loadtest.NewOS(*osURL, *index)

	var sender loadtest.Sender
	if *mode == "http" {
		sender = loadtest.NewHTTPSender(*ingest, *descSize)
	} else {
		sender = loadtest.NewKafkaSender(*brokers, *topic, *descSize)
	}
	defer sender.Close()

	dlqBefore, _ := loadtest.TopicEndOffset(*brokers, *dlqTopic)

	// per-second searchable-count sampler
	type sample struct {
		t time.Time
		n int64
	}
	var samples []sample
	sctx, stopSampler := context.WithCancel(ctx)
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
				if n, err := osc.CountRun(sctx, *runID); err == nil {
					samples = append(samples, sample{time.Now(), n})
				}
			}
		}
	}()

	start := time.Now()
	var pr *loadtest.Probe
	pctx, stopProbe := context.WithCancel(ctx)
	probeDone := make(chan struct{})
	if *probe > 0 {
		pr = &loadtest.Probe{Sender: sender, OS: osc, RunID: *runID}
		go func() { defer close(probeDone); pr.Run(pctx, *probe, 1_000_000_000) }()
	} else {
		close(probeDone)
	}

	log.Printf("run %s: mode=%s count=%d duration=%s rate=%d workers=%d", *runID, *mode, *count, *duration, *rate, *workers)
	res := loadtest.Produce(ctx, sender, loadtest.Params{
		RunID: *runID, Count: *count, Duration: *duration, Rate: *rate, Workers: *workers,
		MalformedPct: *badPct, RetrySend: false,
		OnProgress: func(n int64) { log.Printf("  sent %d", n) },
	})
	stopProbe()
	<-probeDone
	log.Printf("produced %d events in %s (%.0f/s), %d send errors", res.Sent, res.SendElapsed.Round(time.Millisecond), float64(res.Sent)/res.SendElapsed.Seconds(), res.SendErrors)

	expected := res.Acked
	if pr != nil {
		expected = append(expected, pr.Events...)
	}
	doneAt, _, err := osc.WaitDrained(ctx, *runID, int64(len(expected)), *stall)
	stopSampler()
	<-samplerDone
	if err != nil {
		log.Printf("drain: %v", err)
	}

	verdict, docs, verr := osc.Verify(ctx, *runID, expected)
	if verr != nil {
		log.Fatalf("verify: %v", verr)
	}

	s := summary{
		Label: *label, Mode: *mode, RunID: *runID, TargetRate: *rate, Partitions: *parts,
		Acked: res.Sent, Indexed: verdict.Found, Missing: len(verdict.Missing), Unexpected: len(verdict.Unexpected),
		ProduceSecs: res.SendElapsed.Seconds(), TotalSecs: doneAt.Sub(start).Seconds(),
		MalformedSent: res.Malformed, Redelivered: verdict.Redelivered,
	}
	s.OverallEPS = float64(len(expected)) / doneAt.Sub(start).Seconds()

	// steady state = median per-second delta over the middle 60% of the ramp
	var deltas []float64
	for i := 1; i < len(samples); i++ {
		if samples[i].n > 0 && samples[i-1].n < int64(len(expected)) {
			deltas = append(deltas, float64(samples[i].n-samples[i-1].n)/samples[i].t.Sub(samples[i-1].t).Seconds())
		}
	}
	if k := len(deltas); k >= 5 {
		mid := deltas[k/5 : k-k/5]
		sort.Float64s(mid)
		s.SteadyEPS = mid[len(mid)/2]
	}

	var server []time.Duration
	for _, d := range docs {
		if !d.IngestedAt.IsZero() && !d.IndexedAt.IsZero() {
			server = append(server, d.IndexedAt.Sub(d.IngestedAt))
		}
	}
	s.ServerP50Ms, s.ServerP99Ms = ms(loadtest.Percentile(server, .50)), ms(loadtest.Percentile(server, .99))
	if pr != nil {
		lat := pr.Latencies()
		s.ProbeN = len(lat)
		s.ProbeP50Ms, s.ProbeP99Ms, s.ProbeMaxMs = ms(loadtest.Percentile(lat, .50)), ms(loadtest.Percentile(lat, .99)), ms(loadtest.Percentile(lat, 1))
	}
	if after, err := loadtest.TopicEndOffset(*brokers, *dlqTopic); err == nil {
		s.DLQDelta = after - dlqBefore
	}

	b, _ := json.MarshalIndent(s, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		line, _ := json.Marshal(s)
		f, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			f.Write(append(line, '\n'))
			f.Close()
		}
	}
	if !verdict.OK() || err != nil {
		os.Exit(1)
	}
}
