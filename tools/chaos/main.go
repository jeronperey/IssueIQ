// chaos runs the pipeline under injected faults and checks, against OpenSearch, that
// every event the system acknowledged is searchable exactly once.
//
//	make chaos                      # all scenarios, 5 runs each
//	go run ./tools/chaos -scenario kill-consumers -runs 3 -events 20000
//
// Requires: docker compose up -d kafka opensearch (containers issueiq-kafka and
// issueiq-opensearch), and bin/{ingestion,normalizer,indexer} built (make build).
// Pipeline services run as host processes so they can be SIGKILLed; a supervisor
// restarts them after a second, the way a container restart policy would.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
	"github.com/jeronperey/issueiq/tools/loadtest"
	"github.com/segmentio/kafka-go"
)

const (
	broker    = "localhost:9092"
	osURL     = "http://localhost:9200"
	ingestURL = "http://localhost:18080"
	partCount = 4
)

// ---------- process supervision ----------

type proc struct {
	name  string
	env   []string
	mu    sync.Mutex
	cmd   *exec.Cmd
	stop  bool
	log   *os.File
	wg    sync.WaitGroup
	kills int
}

func startProc(name string, env []string, logDir string) *proc {
	lf, _ := os.Create(fmt.Sprintf("%s/%s.log", logDir, name))
	p := &proc{name: name, env: env, log: lf}
	p.wg.Add(1)
	go p.supervise()
	return p
}

func (p *proc) supervise() {
	defer p.wg.Done()
	bin := "bin/" + strings.TrimRight(strings.SplitN(p.name, "-", 2)[0], "0123456789")
	for {
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(), p.env...)
		cmd.Stdout, cmd.Stderr = p.log, p.log
		if err := cmd.Start(); err != nil {
			log.Fatalf("start %s: %v", p.name, err)
		}
		p.mu.Lock()
		p.cmd = cmd
		p.mu.Unlock()
		cmd.Wait()
		p.mu.Lock()
		stopped := p.stop
		p.mu.Unlock()
		if stopped {
			return
		}
		time.Sleep(time.Second) // restart delay
	}
}

func (p *proc) kill9() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil && p.cmd.Process != nil {
		p.cmd.Process.Signal(syscall.SIGKILL)
		p.kills++
	}
}

func (p *proc) shutdown() {
	p.mu.Lock()
	p.stop = true
	if p.cmd != nil && p.cmd.Process != nil {
		p.cmd.Process.Signal(syscall.SIGKILL)
	}
	p.mu.Unlock()
	p.wg.Wait()
	p.log.Close()
}

// ---------- environment helpers ----------

func docker(args ...string) error {
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %v: %v: %s", args, err, out)
	}
	return nil
}

func kafkaCLI(args ...string) error {
	return docker(append([]string{"exec", "issueiq-kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092"}, args...)...)
}

func waitUntil(what string, d time.Duration, f func() bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Fatalf("timed out waiting for %s", what)
}

func osHealthy() bool {
	c := http.Client{Timeout: 2 * time.Second}
	r, err := c.Get(osURL + "/_cluster/health?wait_for_status=yellow&timeout=1s")
	if err != nil {
		return false
	}
	r.Body.Close()
	return r.StatusCode == 200
}

func kafkaHealthy() bool {
	_, err := loadtest.TopicEndOffset(broker, "dead-letter")
	return err == nil
}

func resetTopics() {
	waitUntil("kafka", 120*time.Second, kafkaHealthy)
	for _, t := range []string{"raw-issues", "normalized-issues"} {
		kafkaCLI("--delete", "--topic", t)
	}
	for _, t := range []string{"raw-issues", "normalized-issues"} {
		waitUntil("topic "+t, 60*time.Second, func() bool {
			return kafkaCLI("--create", "--topic", t, "--partitions", fmt.Sprint(partCount), "--replication-factor", "1") == nil
		})
	}
	kafkaCLI("--create", "--if-not-exists", "--topic", "dead-letter", "--partitions", "1", "--replication-factor", "1")
}

// ---------- scenario machinery ----------

type env struct {
	runID  string
	procs  []*proc
	normal []*proc
	index  []*proc
}

func (e *env) killRandomConsumer() {
	all := append(append([]*proc{}, e.normal...), e.index...)
	all[rand.Intn(len(all))].kill9()
}

func (e *env) up(logDir string) {
	grp := e.runID
	e.procs = append(e.procs, startProc("ingestion", []string{"HTTP_ADDR=:18080", "METRICS_ADDR=:19100", "KAFKA_ADDR=" + broker}, logDir))
	for i := 1; i <= 2; i++ {
		p := startProc(fmt.Sprintf("normalizer-%d", i), []string{fmt.Sprintf("METRICS_ADDR=:%d", 19100+i), "KAFKA_BROKERS=" + broker, "CONSUMER_GROUP=n-" + grp}, logDir)
		e.normal, e.procs = append(e.normal, p), append(e.procs, p)
		q := startProc(fmt.Sprintf("indexer-%d", i), []string{fmt.Sprintf("METRICS_ADDR=:%d", 19200+i), "KAFKA_BROKERS=" + broker, "CONSUMER_GROUP=i-" + grp, "OPENSEARCH_URL=" + osURL}, logDir)
		e.index, e.procs = append(e.index, q), append(e.procs, q)
	}
	waitUntil("ingestion", 20*time.Second, func() bool {
		r, err := http.Get(ingestURL + "/health")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})
	time.Sleep(10 * time.Second) // consumer group join + partition assignment
}

func (e *env) down() {
	for _, p := range e.procs {
		p.shutdown()
	}
}

type hybridSender struct {
	loadtest.Sender                       // valid events over HTTP
	bad             *loadtest.KafkaSender // malformed events straight onto the topic
}

func (h hybridSender) SendMalformed(ctx context.Context, runID string, n int) error {
	return h.bad.SendMalformed(ctx, runID, n)
}

type scenario struct {
	name    string
	faults  func(ctx context.Context, e *env) // runs concurrently with the producer
	badPct  float64
	preload func(e *env) int // optional: returns how many extra valid events are dead-lettered up front
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

var scenarios = []scenario{
	{name: "kill-consumers", faults: func(ctx context.Context, e *env) {
		for i := 0; i < 5; i++ {
			if !sleepCtx(ctx, 2*time.Second) {
				return
			}
			e.killRandomConsumer()
		}
	}},
	{name: "kafka-restart", faults: func(ctx context.Context, e *env) {
		if sleepCtx(ctx, 4*time.Second) {
			docker("restart", "issueiq-kafka")
		}
	}},
	{name: "opensearch-restart", faults: func(ctx context.Context, e *env) {
		if sleepCtx(ctx, 4*time.Second) {
			docker("restart", "issueiq-opensearch")
		}
	}},
	{name: "malformed-2pct", badPct: 0.02},
	{name: "combined", badPct: 0.02, faults: func(ctx context.Context, e *env) {
		if !sleepCtx(ctx, 2*time.Second) {
			return
		}
		e.killRandomConsumer()
		if !sleepCtx(ctx, 2*time.Second) {
			return
		}
		docker("restart", "issueiq-kafka")
		if !sleepCtx(ctx, 20*time.Second) {
			return
		}
		e.killRandomConsumer()
		docker("restart", "issueiq-opensearch")
	}},
}

type runResult struct {
	Scenario    string  `json:"scenario"`
	Run         int     `json:"run"`
	RunID       string  `json:"run_id"`
	Acked       int     `json:"acked"`
	SendErrors  int64   `json:"send_errors"`
	Indexed     int     `json:"indexed"`
	Missing     int     `json:"missing"`
	Unexpected  int     `json:"unexpected"`
	DupDocs     int     `json:"duplicate_docs"`
	Redelivered int64   `json:"redelivered_writes"`
	Malformed   int64   `json:"malformed_injected"`
	DLQDelta    int64   `json:"dlq_delta"`
	Kills       int     `json:"process_kills"`
	Secs        float64 `json:"secs"`
	Pass        bool    `json:"pass"`
	Note        string  `json:"note,omitempty"`
}

func runOnce(sc scenario, run, events, rate int, logRoot string) runResult {
	runID := fmt.Sprintf("%s%d%02d", strings.NewReplacer("-", "", "_", "").Replace(sc.name)[:3], time.Now().Unix()%100000, run)
	logDir := fmt.Sprintf("%s/%s-%d", logRoot, sc.name, run)
	os.MkdirAll(logDir, 0o755)

	waitUntil("opensearch", 180*time.Second, osHealthy)
	resetTopics()
	osc := loadtest.NewOS(osURL, "issues")
	dlqBefore, _ := loadtest.TopicEndOffset(broker, "dead-letter")

	e := &env{runID: runID}
	e.up(logDir)
	defer e.down()

	start := time.Now()
	ks := loadtest.NewKafkaSender(broker, "raw-issues", 400)
	sender := hybridSender{loadtest.NewHTTPSender(ingestURL, 400), ks}
	defer sender.Close()
	defer ks.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var fwg sync.WaitGroup
	if sc.faults != nil {
		fwg.Add(1)
		go func() { defer fwg.Done(); sc.faults(ctx, e) }()
	}
	res := loadtest.Produce(ctx, sender, loadtest.Params{
		RunID: runID, Count: events, Rate: rate, Workers: 64, MalformedPct: sc.badPct, RetrySend: true,
	})
	fwg.Wait() // finish scripted faults even if the producer was faster than the script
	cancel()

	// Everything the system acknowledged must show up. Wait for lag to settle.
	waitUntil("opensearch", 240*time.Second, osHealthy)
	_, _, derr := osc.WaitDrained(context.Background(), runID, int64(len(res.Acked)), 150*time.Second)
	v, _, verr := osc.Verify(context.Background(), runID, res.Acked)
	rr := runResult{Scenario: sc.name, Run: run, RunID: runID, Acked: len(res.Acked), SendErrors: res.SendErrors, Malformed: res.Malformed, Secs: time.Since(start).Seconds()}
	for _, p := range e.procs {
		rr.Kills += p.kills
	}
	if verr != nil {
		rr.Note = "verify: " + verr.Error()
		return rr
	}
	rr.Indexed, rr.Missing, rr.Unexpected, rr.DupDocs, rr.Redelivered = v.Found, len(v.Missing), len(v.Unexpected), v.DuplicateIDs, v.Redelivered
	if after, err := loadtest.TopicEndOffset(broker, "dead-letter"); err == nil {
		rr.DLQDelta = after - dlqBefore
	}
	rr.Pass = v.OK() && res.SendErrors == 0 && derr == nil && rr.DLQDelta >= res.Malformed
	if derr != nil {
		rr.Note = derr.Error()
	}
	return rr
}

// replayRun dead-letters N valid events (as an outage-era failure would), replays them
// with tools/replay, and checks each lands in the index exactly once.
func replayRun(run, events int, logRoot string) runResult {
	runID := fmt.Sprintf("rpl%d%02d", time.Now().Unix()%100000, run)
	logDir := fmt.Sprintf("%s/dlq-replay-%d", logRoot, run)
	os.MkdirAll(logDir, 0o755)
	waitUntil("opensearch", 180*time.Second, osHealthy)
	resetTopics()
	osc := loadtest.NewOS(osURL, "issues")
	start := time.Now()

	// fresh consumer group for the replayer so earlier runs' offsets don't hide records
	grp := "replayer-" + runID
	dlqBefore, _ := loadtest.TopicEndOffset(broker, "dead-letter")
	w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: "dead-letter", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchTimeout: 5 * time.Millisecond}
	var expected []loadtest.Event
	var msgs []kafka.Message
	for n := 0; n < events; n++ {
		ev := loadtest.NewEvent(runID, n)
		raw := schema.RawIssue{SourceID: ev.SourceID, Source: "mock", ReceivedAt: time.Now().UTC(), RawPayload: map[string]any{"title": "replayed " + ev.SourceID, "tags": []string{loadtest.RunTag(runID)}}}
		payload, _ := json.Marshal(schema.EventEnvelope{EventID: "r-" + ev.SourceID, EventType: "issue_created", Source: "mock", OccurredAt: time.Now().UTC(), Payload: raw})
		d, _ := json.Marshal(schema.DLQEnvelope{SourceTopic: "raw-issues", Reason: "opensearch upsert: connection refused", OriginalPayload: payload, FailedAt: time.Now().UTC()})
		msgs = append(msgs, kafka.Message{Key: []byte(ev.SourceID), Value: d})
		expected = append(expected, ev)
	}
	if err := w.WriteMessages(context.Background(), msgs...); err != nil {
		log.Fatalf("seed dlq: %v", err)
	}
	w.Close()

	e := &env{runID: runID}
	e.up(logDir)
	defer e.down()

	for i := 0; i < 2; i++ { // second pass must be a no-op
		out, err := exec.Command("go", "run", "./tools/replay", "-group", grp, "-reason-contains", "opensearch").CombinedOutput()
		if err != nil {
			log.Fatalf("replay: %v: %s", err, out)
		}
		log.Printf("    replay pass %d: %s", i+1, strings.TrimSpace(string(out)))
	}
	_, _, derr := osc.WaitDrained(context.Background(), runID, int64(events), 60*time.Second)
	time.Sleep(3 * time.Second) // let any (incorrect) double replay land before checking versions
	v, _, verr := osc.Verify(context.Background(), runID, expected)
	rr := runResult{Scenario: "dlq-replay", Run: run, RunID: runID, Acked: events, Secs: time.Since(start).Seconds()}
	if verr != nil {
		rr.Note = verr.Error()
		return rr
	}
	rr.Indexed, rr.Missing, rr.Unexpected, rr.DupDocs, rr.Redelivered = v.Found, len(v.Missing), len(v.Unexpected), v.DuplicateIDs, v.Redelivered
	after, _ := loadtest.TopicEndOffset(broker, "dead-letter")
	rr.DLQDelta = after - dlqBefore - int64(events) // records added beyond the seeded ones
	rr.Pass = v.OK() && derr == nil && v.Redelivered == 0
	return rr
}

func main() {
	var (
		only   = flag.String("scenario", "all", "scenario name, dlq-replay, or all")
		runs   = flag.Int("runs", 5, "runs per scenario")
		events = flag.Int("events", 30000, "valid events per run")
		rate   = flag.Int("rate", 2500, "events/sec during the run")
		out    = flag.String("out", "docs/results/chaos.jsonl", "JSONL results file")
		logs   = flag.String("logs", "/tmp/issueiq-chaos", "directory for service logs")
	)
	flag.Parse()
	os.MkdirAll(*logs, 0o755)
	of, _ := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	defer of.Close()

	var all []runResult
	record := func(r runResult) {
		all = append(all, r)
		b, _ := json.Marshal(r)
		of.Write(append(b, '\n'))
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
		}
		log.Printf("%s %-20s run %d: acked=%d indexed=%d missing=%d unexpected=%d dup_docs=%d redelivered=%d dlq+%d (injected %d) kills=%d %.0fs %s",
			status, r.Scenario, r.Run, r.Acked, r.Indexed, r.Missing, r.Unexpected, r.DupDocs, r.Redelivered, r.DLQDelta, r.Malformed, r.Kills, r.Secs, r.Note)
	}
	for _, sc := range scenarios {
		if *only != "all" && *only != sc.name {
			continue
		}
		for i := 1; i <= *runs; i++ {
			record(runOnce(sc, i, *events, *rate, *logs))
		}
	}
	if *only == "all" || *only == "dlq-replay" {
		for i := 1; i <= *runs; i++ {
			record(replayRun(i, 5000, *logs))
		}
	}

	fail := 0
	var acked int
	for _, r := range all {
		acked += r.Acked
		if !r.Pass {
			fail++
		}
	}
	log.Printf("SUMMARY: %d runs, %d acked events verified, %d failed runs", len(all), acked, fail)
	if fail > 0 {
		os.Exit(1)
	}
}
