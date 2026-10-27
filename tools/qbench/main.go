// qbench measures the query API's read path with a closed-loop load: N workers each
// issue the next request as soon as the previous one returns, per scenario, for a fixed
// duration. It first checks cursor paging for duplicates/skips, then reports QPS and
// p50/p95/p99 per scenario.
//
//	go run ./tools/qbench -url http://localhost:8082 -workers 16 -duration 15s
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type page struct {
	Issues []struct {
		ID        string    `json:"id"`
		CreatedAt time.Time `json:"created_at"`
	} `json:"issues"`
	Total         int    `json:"total"`
	TotalRelation string `json:"total_relation"`
	NextCursor    string `json:"next_cursor"`
}

var client = &http.Client{Timeout: 30 * time.Second}

func get(url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s -> %d: %s", url, resp.StatusCode, b)
	}
	return b, nil
}

// verifyPaging walks `pages` pages and fails on any repeated ID or out-of-order created_at.
func verifyPaging(base string, pages, limit int) (int, []string) {
	seen := map[string]bool{}
	var ids []string
	var prev time.Time
	cursor := ""
	for i := 0; i < pages; i++ {
		u := fmt.Sprintf("%s/issues?limit=%d", base, limit)
		if cursor != "" {
			u += "&cursor=" + cursor
		}
		b, err := get(u)
		if err != nil {
			log.Fatal(err)
		}
		var p page
		json.Unmarshal(b, &p)
		for _, is := range p.Issues {
			if seen[is.ID] {
				log.Fatalf("paging FAILED: duplicate id %s on page %d", is.ID, i+1)
			}
			// OpenSearch sorts dates at millisecond precision; ties are ordered by id.
			ca := is.CreatedAt.Truncate(time.Millisecond)
			if !prev.IsZero() && ca.After(prev) {
				log.Fatalf("paging FAILED: created_at went backwards on page %d", i+1)
			}
			seen[is.ID], prev = true, ca
			if len(ids) < 500 {
				ids = append(ids, is.ID)
			}
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	return len(seen), ids
}

type scenario struct {
	name string
	req  func(r *rand.Rand) string // returns the URL for one request
}

type result struct {
	Name     string  `json:"scenario"`
	Workers  int     `json:"workers"`
	Requests int     `json:"requests"`
	Errors   int     `json:"errors"`
	QPS      float64 `json:"qps"`
	P50Ms    float64 `json:"p50_ms"`
	P95Ms    float64 `json:"p95_ms"`
	P99Ms    float64 `json:"p99_ms"`
}

func pct(s []time.Duration, q float64) float64 {
	if len(s) == 0 {
		return 0
	}
	return float64(s[int(float64(len(s)-1)*q)].Microseconds()) / 1000
}

func run(sc scenario, workers int, d time.Duration) result {
	var (
		mu   sync.Mutex
		lats []time.Duration
		errs int
		wg   sync.WaitGroup
	)
	end := time.Now().Add(d)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			var local []time.Duration
			e := 0
			for time.Now().Before(end) {
				u := sc.req(r)
				t0 := time.Now()
				if _, err := get(u); err != nil {
					e++
					continue
				}
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			lats, errs = append(lats, local...), errs+e
			mu.Unlock()
		}(int64(w) + 1)
	}
	wg.Wait()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	return result{sc.name, workers, len(lats), errs, float64(len(lats)) / d.Seconds(), pct(lats, .5), pct(lats, .95), pct(lats, .99)}
}

func main() {
	base := flag.String("url", "http://localhost:8082", "query API base URL")
	workers := flag.Int("workers", 16, "concurrent clients")
	dur := flag.Duration("duration", 15*time.Second, "per-scenario duration")
	out := flag.String("out", "", "append JSON-lines results to this file")
	flag.Parse()

	b, err := get(*base + "/issues?limit=1")
	if err != nil {
		log.Fatal(err)
	}
	var first page
	json.Unmarshal(b, &first)
	fmt.Printf("corpus: total=%d relation=%s (count is capped; see /issues/stats for exact bucket counts)\n", first.Total, first.TotalRelation)

	n, ids := verifyPaging(*base, 200, 100)
	fmt.Printf("paging check: walked 200 pages x 100 via cursor -> %d unique docs, 0 duplicates, created_at monotonic\n\n", n)

	scs := []scenario{
		{"get by id", func(r *rand.Rand) string { return *base + "/issues/" + ids[r.Intn(len(ids))] }},
		{"list, 2 filters", func(*rand.Rand) string { return *base + "/issues?status=open&priority=high&limit=20" }},
		{"list, selective filter (840 docs)", func(*rand.Rand) string { return *base + "/issues?priority=critical&limit=20" }},
		{"text search (rare token)", func(r *rand.Rand) string {
			return fmt.Sprintf("%s/issues/search?q=%08d&limit=20", *base, r.Intn(200000))
		}},
		{"filtered aggregate", func(*rand.Rand) string { return *base + "/issues/aggregate?status=open&priority=high" }},
		{"global stats aggregate", func(*rand.Rand) string { return *base + "/issues/stats" }},
	}
	fmt.Printf("%-36s %9s %9s %9s %9s %7s\n", "scenario", "req/s", "p50 ms", "p95 ms", "p99 ms", "errors")
	var results []result
	for _, sc := range scs {
		run(sc, *workers, 2*time.Second) // warm-up
		r := run(sc, *workers, *dur)
		results = append(results, r)
		fmt.Printf("%-36s %9.0f %9.1f %9.1f %9.1f %7d\n", r.Name, r.QPS, r.P50Ms, r.P95Ms, r.P99Ms, r.Errors)
	}

	// Cursor paging cost vs depth: latency of page k while walking 1..40 (limit 100).
	const depth = 40
	perPage := make([][]time.Duration, depth)
	var mu sync.Mutex
	var wg sync.WaitGroup
	end := time.Now().Add(*dur)
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(end) {
				cursor := ""
				for k := 0; k < depth; k++ {
					u := *base + "/issues?limit=100"
					if cursor != "" {
						u += "&cursor=" + cursor
					}
					t0 := time.Now()
					b, err := get(u)
					if err != nil {
						break
					}
					l := time.Since(t0)
					var p page
					json.Unmarshal(b, &p)
					mu.Lock()
					perPage[k] = append(perPage[k], l)
					mu.Unlock()
					if cursor = p.NextCursor; cursor == "" {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	med := func(a, b int) float64 {
		var all []time.Duration
		for k := a; k < b; k++ {
			all = append(all, perPage[k]...)
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		return pct(all, .5)
	}
	fmt.Printf("\ncursor paging by depth (limit 100, %d workers): median ms  pages 1-5: %.1f   pages 36-40: %.1f  (flat = depth-independent)\n",
		*workers, med(0, 5), med(depth-5, depth))

	if *out != "" {
		f, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			defer f.Close()
			for _, r := range results {
				json.NewEncoder(f).Encode(r)
			}
		}
	}
}
