package main

import (
	"encoding/json"
	"net/http"

	"github.com/jeronperey/issueiq/shared/schema"
)

type IssuesResponse struct {
	Issues []schema.NormalizedIssue `json:"issues"`
	Total  int                      `json:"total"`
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
	id := r.PathValue("id")
	issue, err := h.osc.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if issue == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, issue)
}

// listIssues handles GET /issues with optional status, priority, source filters.
func (h *handlers) listIssues(w http.ResponseWriter, r *http.Request) {
	filters := readFilters(r)
	issues, total, err := h.osc.Search(r.Context(), "", filters)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, IssuesResponse{Issues: issues, Total: total})
}

// searchIssues handles GET /issues/search?q=<text> with optional filters.
func (h *handlers) searchIssues(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	filters := readFilters(r)
	issues, total, err := h.osc.Search(r.Context(), q, filters)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, IssuesResponse{Issues: issues, Total: total})
}

// aggregateHandler handles GET /issues/aggregate with optional status, priority, source filters.
// Aggregations run only over the matching subset, so bucket counts reflect the filter.
func (h *handlers) aggregateHandler(w http.ResponseWriter, r *http.Request) {
	filters := readFilters(r)
	result, err := h.osc.GetAggregate(r.Context(), filters)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// statsHandler handles GET /issues/stats — term-count aggregations by status, priority, source.
func (h *handlers) statsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := h.osc.Aggregate(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
