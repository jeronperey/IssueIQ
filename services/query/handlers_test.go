package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeOpenSearch records the last _search body and answers with a canned response.
type fakeOpenSearch struct {
	srv    *httptest.Server
	body   map[string]any
	status int
	resp   string
}

func newFake(t *testing.T, status int, resp string) *fakeOpenSearch {
	f := &fakeOpenSearch{status: status, resp: resp}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &f.body)
		w.WriteHeader(f.status)
		io.WriteString(w, f.resp)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeOpenSearch) handlers() *handlers {
	return &handlers{osc: NewOpenSearchClient(f.srv.URL, "issues", 2*time.Second)}
}

const twoHits = `{"hits":{"total":{"value":10000,"relation":"gte"},"hits":[
 {"_source":{"id":"a","title":"t1"},"sort":[1700000000000,"a"]},
 {"_source":{"id":"b","title":"t2"},"sort":[1699999999000,"b"]}]}}`

func get(h http.HandlerFunc, url string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, url, nil))
	return rec
}

func TestPagingRequestAndCursor(t *testing.T) {
	f := newFake(t, 200, twoHits)
	h := f.handlers()

	rec := get(h.listIssues, "/issues?status=open&limit=2")
	if rec.Code != 200 {
		t.Fatalf("code %d: %s", rec.Code, rec.Body)
	}
	if f.body["size"] != float64(2) {
		t.Errorf("size = %v, want 2", f.body["size"])
	}
	if f.body["track_total_hits"] != float64(trackTotalHitsUpTo) {
		t.Errorf("track_total_hits = %v", f.body["track_total_hits"])
	}
	if _, ok := f.body["search_after"]; ok {
		t.Error("first page must not send search_after")
	}
	sort := f.body["sort"].([]any)
	if len(sort) != 2 || sort[0].(map[string]any)["created_at"] != "desc" || sort[1].(map[string]any)["id"] != "asc" {
		t.Errorf("sort = %v, want created_at desc then id asc tiebreak", sort)
	}

	var resp IssuesResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Total != 10000 || resp.TotalRelation != "gte" {
		t.Errorf("total = %d %q: the capped count must be reported as a lower bound", resp.Total, resp.TotalRelation)
	}
	if resp.NextCursor == "" {
		t.Fatal("full page must return a next_cursor")
	}

	// Following the cursor must send the last hit's sort values as search_after.
	get(h.listIssues, "/issues?limit=2&cursor="+resp.NextCursor)
	after := f.body["search_after"].([]any)
	if after[0] != float64(1699999999000) || after[1] != "b" {
		t.Errorf("search_after = %v, want last hit's sort values", after)
	}
}

func TestLastPageHasNoCursor(t *testing.T) {
	f := newFake(t, 200, twoHits)
	rec := get(f.handlers().listIssues, "/issues?limit=5") // 2 hits < limit 5
	var resp IssuesResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.NextCursor != "" {
		t.Errorf("short page returned cursor %q", resp.NextCursor)
	}
}

func TestTextSearchSortsByRelevance(t *testing.T) {
	f := newFake(t, 200, twoHits)
	get(f.handlers().searchIssues, "/issues/search?q=login")
	first := f.body["sort"].([]any)[0].(map[string]any)
	if first["_score"] != "desc" {
		t.Errorf("text search sort = %v, want _score desc", f.body["sort"])
	}
}

func TestBadParamsAre400(t *testing.T) {
	f := newFake(t, 200, twoHits)
	h := f.handlers()
	for _, url := range []string{
		"/issues?limit=0", "/issues?limit=101", "/issues?limit=abc",
		"/issues?cursor=!!notbase64", "/issues?cursor=" + encodeCursor([]any{1}), // wrong shape
	} {
		if rec := get(h.listIssues, url); rec.Code != 400 {
			t.Errorf("%s -> %d, want 400", url, rec.Code)
		}
	}
	if f.body != nil {
		t.Error("invalid requests must not reach OpenSearch")
	}
}

func TestUpstreamFailureDoesNotLeakInternals(t *testing.T) {
	f := newFake(t, 500, `{"error":"index [issues] shard [0] secret-internal-detail"}`)
	rec := get(f.handlers().listIssues, "/issues")
	if rec.Code != http.StatusBadGateway {
		t.Errorf("code = %d, want 502", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "secret-internal-detail") || strings.Contains(rec.Body.String(), "shard") {
		t.Errorf("response leaks upstream detail: %s", rec.Body)
	}
}

func TestSlowUpstreamTimesOut(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer slow.Close()
	h := &handlers{osc: NewOpenSearchClient(slow.URL, "issues", 50*time.Millisecond)}
	start := time.Now()
	rec := get(h.listIssues, "/issues")
	if rec.Code < 500 {
		t.Errorf("code = %d, want 5xx", rec.Code)
	}
	if time.Since(start) > 250*time.Millisecond {
		t.Error("client timeout was not enforced")
	}
}

func TestByIDNotFound(t *testing.T) {
	f := newFake(t, 404, `{"found":false}`)
	h := f.handlers()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/issues/zzz", nil)
	req.SetPathValue("id", "zzz")
	h.issueByID(rec, req)
	if rec.Code != 404 {
		t.Errorf("code = %d, want 404", rec.Code)
	}
}

func TestBuildSearchQueryFilters(t *testing.T) {
	q := buildSearchQuery("x", map[string]string{"status": "open"})
	b, _ := json.Marshal(q)
	if !strings.Contains(string(b), `"multi_match"`) || !strings.Contains(string(b), `"term":{"status":"open"}`) {
		t.Errorf("query = %s", b)
	}
	if b, _ := json.Marshal(buildSearchQuery("", nil)); string(b) != `{"match_all":{}}` {
		t.Errorf("empty query = %s", b)
	}
}
