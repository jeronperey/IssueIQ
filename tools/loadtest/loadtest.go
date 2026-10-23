// Package loadtest holds the producer, OpenSearch verifier and latency probe
// shared by tools/loadgen (benchmarks) and tools/chaos (fault injection).
package loadtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/segmentio/kafka-go"
)

// Event is one synthetic issue. Tag ties it to a run so the verifier can find it.
type Event struct {
	SourceID string
	ID       string // expected OpenSearch _id
}

// RunTag is the keyword tag attached to every event of a run.
func RunTag(runID string) string { return "run_" + strings.ToLower(runID) }

// ExpectedID mirrors the normalizer's stable ID: sha256("source:source_id").
func ExpectedID(source, sourceID string) string {
	h := sha256.Sum256([]byte(source + ":" + sourceID))
	return hex.EncodeToString(h[:])
}

func NewEvent(runID string, n int) Event {
	sid := fmt.Sprintf("%s-%08d", runID, n)
	return Event{SourceID: sid, ID: ExpectedID("mock", sid)}
}

func rawIssue(runID string, e Event, descBytes int) schema.RawIssue {
	desc := strings.Repeat("lorem ipsum dolor sit amet ", descBytes/27+1)[:descBytes]
	return schema.RawIssue{
		SourceID: e.SourceID,
		Source:   "mock",
		RawPayload: map[string]any{
			"title":       "synthetic issue " + e.SourceID,
			"description": desc,
			"status":      []string{"open", "new", "in progress", "closed"}[len(e.SourceID)%4],
			"priority":    []string{"p0", "p1", "p2", "p3"}[len(e.SourceID)%4],
			"tags":        []string{RunTag(runID), "synthetic"},
		},
		ReceivedAt: time.Now().UTC(),
	}
}

func envelope(r schema.RawIssue) []byte {
	b, _ := json.Marshal(schema.EventEnvelope{
		EventID:    fmt.Sprintf("lt-%s", r.SourceID),
		EventType:  "issue_created",
		Source:     r.Source,
		OccurredAt: time.Now().UTC(),
		Payload:    r,
	})
	return b
}

// Sender delivers one event to the pipeline. Implementations must be safe for concurrent use.
type Sender interface {
	// Send returns nil only once the pipeline has durably accepted the event
	// (Kafka ack, or HTTP 202).
	Send(ctx context.Context, runID string, e Event) error
	// SendMalformed injects an event that must end up in the dead-letter topic.
	SendMalformed(ctx context.Context, runID string, n int) error
	Close() error
}

// ---- Kafka-direct sender ----

type KafkaSender struct {
	w         *kafka.Writer
	descBytes int
}

func NewKafkaSender(broker, topic string, descBytes int) *KafkaSender {
	return &KafkaSender{
		descBytes: descBytes,
		w: &kafka.Writer{
			Addr:         kafka.TCP(broker),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchSize:    500,
			BatchTimeout: 5 * time.Millisecond,
			MaxAttempts:  20,
		},
	}
}

func (k *KafkaSender) Send(ctx context.Context, runID string, e Event) error {
	return k.w.WriteMessages(ctx, kafka.Message{Key: []byte(e.SourceID), Value: envelope(rawIssue(runID, e, k.descBytes))})
}

func (k *KafkaSender) SendMalformed(ctx context.Context, runID string, n int) error {
	var v []byte
	switch n % 3 {
	case 0:
		v = []byte(`{not json at all`)
	case 1: // valid envelope, title missing -> normalizer rejects
		r := rawIssue(runID, NewEvent(runID, -n), 10)
		delete(r.RawPayload, "title")
		v = envelope(r)
	default: // wrong shape payload
		v = []byte(`{"event_id":"x","event_type":"issue_created","payload":"oops"}`)
	}
	return k.w.WriteMessages(ctx, kafka.Message{Key: []byte(fmt.Sprintf("bad-%s-%d", runID, n)), Value: v})
}

func (k *KafkaSender) Close() error { return k.w.Close() }

// ---- HTTP sender (goes through the ingestion service) ----

type HTTPSender struct {
	url       string
	c         *http.Client
	descBytes int
}

func NewHTTPSender(baseURL string, descBytes int) *HTTPSender {
	return &HTTPSender{url: baseURL, descBytes: descBytes, c: &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 256},
	}}
}

func (h *HTTPSender) post(ctx context.Context, body []byte) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.url+"/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("ingest: status %d", resp.StatusCode)
	}
	return nil
}

func (h *HTTPSender) Send(ctx context.Context, runID string, e Event) error {
	b, _ := json.Marshal(rawIssue(runID, e, h.descBytes))
	return h.post(ctx, b)
}

// SendMalformed over HTTP: ingestion validates up front and rejects (400), so the
// only malformed events that reach the DLQ are ones that pass validation but fail
// normalization (empty title).
func (h *HTTPSender) SendMalformed(ctx context.Context, runID string, n int) error {
	r := rawIssue(runID, NewEvent(runID, -n), 10)
	r.RawPayload["title"] = "   "
	b, _ := json.Marshal(r)
	return h.post(ctx, b)
}

func (h *HTTPSender) Close() error { return nil }

// ---- Run driver ----

type Result struct {
	Sent        int64         // events acked by the pipeline
	SendErrors  int64         // Send calls that failed after retries
	Malformed   int64         // malformed events acked
	SendElapsed time.Duration // wall time spent producing
	Acked       []Event       // events the pipeline acked (the expected set)
}

type Params struct {
	RunID        string
	Count        int           // number of valid events (0 = until Duration)
	Duration     time.Duration // used when Count == 0
	Rate         int           // events/sec target, 0 = unbounded
	Workers      int
	MalformedPct float64 // fraction of extra malformed events relative to valid
	RetrySend    bool    // retry failed sends (a client behaving correctly); otherwise count the error
	OnProgress   func(sent int64)
}

// Produce drives the sender and records exactly which events were acked.
func Produce(ctx context.Context, s Sender, p Params) Result {
	if p.Workers <= 0 {
		p.Workers = 32
	}
	var (
		sent, errs, bad atomic.Int64
		mu              sync.Mutex
		acked           []Event
	)
	jobs := make(chan int, p.Workers*2)
	var wg sync.WaitGroup
	start := time.Now()

	sendWithRetry := func(f func() error) error {
		var err error
		for attempt := 0; attempt < 600; attempt++ { // ~ up to 5 minutes of 500ms retries
			if err = f(); err == nil {
				return nil
			}
			if !p.RetrySend || ctx.Err() != nil {
				return err
			}
			time.Sleep(500 * time.Millisecond)
		}
		return err
	}

	for i := 0; i < p.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				if n < 0 { // malformed marker
					if sendWithRetry(func() error { return s.SendMalformed(ctx, p.RunID, -n) }) == nil {
						bad.Add(1)
					}
					continue
				}
				e := NewEvent(p.RunID, n)
				if err := sendWithRetry(func() error { return s.Send(ctx, p.RunID, e) }); err != nil {
					errs.Add(1)
					continue
				}
				sent.Add(1)
				mu.Lock()
				acked = append(acked, e)
				mu.Unlock()
				if p.OnProgress != nil && sent.Load()%5000 == 0 {
					p.OnProgress(sent.Load())
				}
			}
		}()
	}

	var tick <-chan time.Time
	if p.Rate > 0 {
		t := time.NewTicker(time.Second / time.Duration(min(p.Rate, 1000)))
		defer t.Stop()
		tick = t.C
	}
	perTick := 1
	if p.Rate > 1000 {
		perTick = p.Rate / 1000
	}
	badEvery := 0
	if p.MalformedPct > 0 {
		badEvery = int(1 / p.MalformedPct)
	}

	deadline := time.Time{}
	if p.Count == 0 {
		deadline = start.Add(p.Duration)
	}
	n, nb := 0, 0
loop:
	for {
		if p.Count > 0 && n >= p.Count {
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
		if tick != nil {
			select {
			case <-tick:
			case <-ctx.Done():
				break loop
			}
		}
		for j := 0; j < perTick; j++ {
			if p.Count > 0 && n >= p.Count {
				break
			}
			select {
			case jobs <- n:
			case <-ctx.Done():
				break loop
			}
			n++
			if badEvery > 0 && n%badEvery == 0 {
				nb++
				select {
				case jobs <- -nb:
				case <-ctx.Done():
					break loop
				}
			}
		}
	}
	close(jobs)
	wg.Wait()
	return Result{Sent: sent.Load(), SendErrors: errs.Load(), Malformed: bad.Load(), SendElapsed: time.Since(start), Acked: acked}
}

// ---- OpenSearch verification ----

type OS struct {
	URL, Index string
	C          *http.Client
}

func NewOS(url, index string) *OS {
	return &OS{URL: url, Index: index, C: &http.Client{Timeout: 60 * time.Second}}
}

func (o *OS) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, o.URL+path, rd)
	req.Header.Set("Content-Type", "application/json")
	resp, err := o.C.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("opensearch %s %s: %d %s", method, path, resp.StatusCode, truncate(string(data), 200))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// Reset deletes the index so each run starts clean; the indexer recreates it on
// start. Callers that keep the indexer running should call EnsureIndexLike instead.
func (o *OS) DeleteIndex(ctx context.Context) error {
	return o.do(ctx, http.MethodDelete, "/"+o.Index+"?ignore_unavailable=true", nil, nil)
}

func (o *OS) Refresh(ctx context.Context) error {
	return o.do(ctx, http.MethodPost, "/"+o.Index+"/_refresh", nil, nil)
}

// CountRun counts searchable docs carrying the run tag.
func (o *OS) CountRun(ctx context.Context, runID string) (int64, error) {
	var out struct {
		Count int64 `json:"count"`
	}
	err := o.do(ctx, http.MethodPost, "/"+o.Index+"/_count",
		map[string]any{"query": map[string]any{"term": map[string]any{"tags": RunTag(runID)}}}, &out)
	return out.Count, err
}

// RunDoc is the subset of a document the verifier needs.
type RunDoc struct {
	ID         string
	Version    int64
	IngestedAt time.Time
	IndexedAt  time.Time
}

// ScanRun pages through every doc of the run with search_after on _id-equivalent
// keyword field `id`, so the result is exact regardless of size.
func (o *OS) ScanRun(ctx context.Context, runID string) ([]RunDoc, error) {
	var docs []RunDoc
	var after any
	for {
		q := map[string]any{
			"size":    5000,
			"version": true,
			"sort":    []any{map[string]any{"id": "asc"}},
			"_source": []string{"id", "ingested_at", "indexed_at"},
			"query":   map[string]any{"term": map[string]any{"tags": RunTag(runID)}},
		}
		if after != nil {
			q["search_after"] = after
		}
		var out struct {
			Hits struct {
				Hits []struct {
					Version int64 `json:"_version"`
					Source  struct {
						ID         string    `json:"id"`
						IngestedAt time.Time `json:"ingested_at"`
						IndexedAt  time.Time `json:"indexed_at"`
					} `json:"_source"`
					Sort []any `json:"sort"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := o.do(ctx, http.MethodPost, "/"+o.Index+"/_search", q, &out); err != nil {
			return nil, err
		}
		if len(out.Hits.Hits) == 0 {
			return docs, nil
		}
		for _, h := range out.Hits.Hits {
			docs = append(docs, RunDoc{ID: h.Source.ID, Version: h.Version, IngestedAt: h.Source.IngestedAt, IndexedAt: h.Source.IndexedAt})
		}
		after = out.Hits.Hits[len(out.Hits.Hits)-1].Sort
	}
}

// WaitDrained polls until the run's searchable count reaches want or timeout/stall.
// It returns the time the final doc became searchable (observed) and the final count.
func (o *OS) WaitDrained(ctx context.Context, runID string, want int64, stall time.Duration) (time.Time, int64, error) {
	var last int64 = -1
	lastChange := time.Now()
	for {
		n, err := o.CountRun(ctx, runID)
		if err == nil {
			if n != last {
				last, lastChange = n, time.Now()
			}
			if n >= want {
				return time.Now(), n, nil
			}
		}
		if time.Since(lastChange) > stall {
			return time.Now(), last, fmt.Errorf("stalled at %d/%d for %s", last, want, stall)
		}
		select {
		case <-ctx.Done():
			return time.Now(), last, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Verdict compares the expected set against what OpenSearch holds.
type Verdict struct {
	Expected     int
	Found        int
	Missing      []string
	Unexpected   []string
	Redelivered  int64 // sum(_version-1): extra writes caused by at-least-once redelivery
	MaxVersion   int64
	DuplicateIDs int // ids appearing more than once in the scan (must be 0)
}

func (v Verdict) OK() bool {
	return len(v.Missing) == 0 && len(v.Unexpected) == 0 && v.DuplicateIDs == 0
}

func (o *OS) Verify(ctx context.Context, runID string, expected []Event) (Verdict, []RunDoc, error) {
	if err := o.Refresh(ctx); err != nil {
		return Verdict{}, nil, err
	}
	docs, err := o.ScanRun(ctx, runID)
	if err != nil {
		return Verdict{}, nil, err
	}
	want := make(map[string]bool, len(expected))
	for _, e := range expected {
		want[e.ID] = true
	}
	v := Verdict{Expected: len(expected), Found: len(docs)}
	seen := make(map[string]bool, len(docs))
	for _, d := range docs {
		if seen[d.ID] {
			v.DuplicateIDs++
		}
		seen[d.ID] = true
		if !want[d.ID] {
			v.Unexpected = append(v.Unexpected, d.ID)
		}
		if d.Version > 1 {
			v.Redelivered += d.Version - 1
		}
		v.MaxVersion = max(v.MaxVersion, d.Version)
	}
	for id := range want {
		if !seen[id] {
			v.Missing = append(v.Missing, id)
		}
	}
	return v, docs, nil
}

// ---- searchable-latency probe ----

// Probe sends single events through the real ingest path and measures send -> first
// _search hit. Unlike ingested_at/indexed_at it includes OpenSearch's refresh delay.
type Probe struct {
	Sender Sender
	OS     *OS
	RunID  string

	mu        sync.Mutex
	lat       []time.Duration
	Events    []Event
	Timeouts  int
	nextIndex int
}

// Run emits one probe every interval until ctx is done. Probe n uses ids in the
// range [base, base+...) so they never collide with the main stream.
func (p *Probe) Run(ctx context.Context, interval time.Duration, base int) {
	t := time.NewTicker(interval)
	defer t.Stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		p.mu.Lock()
		n := base + p.nextIndex
		p.nextIndex++
		p.mu.Unlock()
		e := NewEvent(p.RunID, n)
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			if err := p.Sender.Send(ctx, p.RunID, e); err != nil {
				return
			}
			pctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for pctx.Err() == nil {
				var out struct {
					Hits struct{ Total struct{ Value int } } `json:"hits"`
				}
				err := p.OS.do(pctx, http.MethodPost, "/"+p.OS.Index+"/_search?size=0&terminate_after=1",
					map[string]any{"query": map[string]any{"ids": map[string]any{"values": []string{e.ID}}}}, &out)
				if err == nil && out.Hits.Total.Value > 0 {
					p.mu.Lock()
					p.lat = append(p.lat, time.Since(start))
					p.Events = append(p.Events, e)
					p.mu.Unlock()
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			p.mu.Lock()
			p.Timeouts++
			p.Events = append(p.Events, e) // still expected eventually
			p.mu.Unlock()
		}()
	}
}

func (p *Probe) Latencies() []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]time.Duration(nil), p.lat...)
}

// Percentile returns the q-quantile (0..1) of ds using nearest-rank.
func Percentile(ds []time.Duration, q float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(q*float64(len(s))+0.9999999) - 1
	i = max(0, min(len(s)-1, i))
	return s[i]
}

// ---- Kafka helpers ----

// TopicEndOffset sums the high-water marks of all partitions of topic.
func TopicEndOffset(broker, topic string) (int64, error) {
	c, err := kafka.Dial("tcp", broker)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	parts, err := c.ReadPartitions(topic)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, p := range parts {
		pc, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, p.ID)
		if err != nil {
			return 0, err
		}
		_, last, err := pc.ReadOffsets()
		pc.Close()
		if err != nil {
			return 0, err
		}
		total += last
	}
	return total, nil
}

// GroupLag returns total lag (end offset - committed offset) for group on topic.
func GroupLag(broker, topic, group string) (int64, error) {
	cl := &kafka.Client{Addr: kafka.TCP(broker), Timeout: 10 * time.Second}
	md, err := cl.Metadata(context.Background(), &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return 0, err
	}
	var parts []int
	for _, t := range md.Topics {
		for _, p := range t.Partitions {
			parts = append(parts, p.ID)
		}
	}
	of, err := cl.OffsetFetch(context.Background(), &kafka.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{topic: parts}})
	if err != nil {
		return 0, err
	}
	var lag int64
	for _, p := range of.Topics[topic] {
		off, err := leaderEnd(broker, topic, p.Partition)
		if err != nil {
			return 0, err
		}
		committed := p.CommittedOffset
		if committed < 0 {
			committed = 0
		}
		lag += off - committed
	}
	return lag, nil
}

func leaderEnd(broker, topic string, part int) (int64, error) {
	pc, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, part)
	if err != nil {
		return 0, err
	}
	defer pc.Close()
	_, last, err := pc.ReadOffsets()
	return last, err
}
