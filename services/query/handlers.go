package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/jeronperey/issueiq/shared/schema"
)

const (
	defaultLimit = 20
	maxLimit     = 100
)

// IssuesResponse is one page of results. Total is exact when TotalRelation is "eq";
// "gte" means at least Total (the count stops at 10,000 to keep queries cheap).
// Pass NextCursor back as ?cursor= to get the following page; it is absent on the last page.
type IssuesResponse struct {
	Issues        []schema.NormalizedIssue `json:"issues"`
	Total         int                      `json:"total"`
	TotalRelation string                   `json:"total_relation"`
	NextCursor    string                   `json:"next_cursor,omitempty"`
}

type StatsResponse struct {
	ByStatus   map[string]int `json:"by_status"`
	ByPriority map[string]int `json:"by_priority"`
	BySource   map[string]int `json:"by_source"`
}

type AggregateResponse struct {
	Total      int               `json:"total"`
	Filters    map[string]string `json:"filters,omitempty"`
	ByStatus   map[string]int    `json:"by_status"`
	ByPriority map[string]int    `json:"by_priority"`
	BySource   map[string]int    `json:"by_source"`
}

type handlers struct {
	osc *OpenSearchClient
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

// issueByID handles GET /issues/{id} — returns the issue or 404.
func (h *handlers) issueByID(w http.ResponseWriter, r *http.Request) {
	issue, err := h.osc.GetByID(r.Context(), r.PathValue("id"))
	if err != nil {
		upstreamError(w, r, err)
		return
	}
	if issue == nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, issue)
}

// listIssues handles GET /issues with optional status, priority, source filters.
func (h *handlers) listIssues(w http.ResponseWriter, r *http.Request) { h.page(w, r, "") }

// searchIssues handles GET /issues/search?q=<text> with optional filters.
func (h *handlers) searchIssues(w http.ResponseWriter, r *http.Request) {
	h.page(w, r, r.URL.Query().Get("q"))
}

func (h *handlers) page(w http.ResponseWriter, r *http.Request, q string) {
	limit, after, err := readPaging(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := h.osc.Search(r.Context(), SearchParams{Q: q, Filters: readFilters(r), Limit: limit, After: after})
	if err != nil {
		upstreamError(w, r, err)
		return
	}
	out := IssuesResponse{Issues: res.Issues, Total: res.Total, TotalRelation: res.Relation}
	if res.Next != nil {
		out.NextCursor = encodeCursor(res.Next)
	}
	writeJSON(w, out)
}

// readPaging parses ?limit= (1..maxLimit, default 20) and ?cursor=.
func readPaging(r *http.Request) (int, []any, error) {
	limit := defaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			return 0, nil, fmt.Errorf("limit must be an integer between 1 and %d", maxLimit)
		}
		limit = n
	}
	var after []any
	if c := r.URL.Query().Get("cursor"); c != "" {
		a, err := decodeCursor(c)
		if err != nil {
			return 0, nil, errors.New("invalid cursor")
		}
		after = a
	}
	return limit, after, nil
}

// The cursor is the last hit's sort values, base64url(JSON). It is opaque to clients.
func encodeCursor(sort []any) string {
	b, _ := json.Marshal(sort)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(c string) ([]any, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return nil, err
	}
	var v []any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	if len(v) != 2 { // (created_at | _score, id)
		return nil, errors.New("bad cursor shape")
	}
	return v, nil
}

// aggregateHandler handles GET /issues/aggregate with optional status, priority, source filters.
// Aggregations run only over the matching subset, so bucket counts reflect the filter.
func (h *handlers) aggregateHandler(w http.ResponseWriter, r *http.Request) {
	filters := readFilters(r)
	result, err := h.osc.GetAggregate(r.Context(), filters)
	if err != nil {
		upstreamError(w, r, err)
		return
	}
	writeJSON(w, result)
}

// statsHandler handles GET /issues/stats — term-count aggregations by status, priority, source.
func (h *handlers) statsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := h.osc.Aggregate(r.Context())
	if err != nil {
		upstreamError(w, r, err)
		return
	}
	writeJSON(w, stats)
}

// readFilters extracts supported filter params; only non-empty values are included.
func readFilters(r *http.Request) map[string]string {
	filters := make(map[string]string)
	for _, field := range []string{"status", "priority", "source"} {
		if v := r.URL.Query().Get(field); v != "" {
			filters[field] = v
		}
	}
	return filters
}

// upstreamError logs the real cause and returns a generic body, so OpenSearch internals
// (index names, query bodies, stack hints) never reach API clients.
func upstreamError(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("upstream error: %s %s: %v", r.Method, r.URL.Path, err)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		writeError(w, http.StatusGatewayTimeout, "search backend timed out")
		return
	}
	writeError(w, http.StatusBadGateway, "search backend unavailable")
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
