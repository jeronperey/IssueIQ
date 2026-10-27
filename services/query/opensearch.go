package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jeronperey/issueiq/shared/schema"
)

type OpenSearchClient struct {
	http  *http.Client
	url   string
	index string
}

// NewOpenSearchClient returns a client whose requests never outlive timeout, so a
// stalled OpenSearch cannot pin request goroutines forever.
func NewOpenSearchClient(url, index string, timeout time.Duration) *OpenSearchClient {
	return &OpenSearchClient{
		http:  &http.Client{Timeout: timeout},
		url:   url,
		index: index,
	}
}

// searchHitsResponse mirrors the shape OpenSearch returns for _search queries.
type searchHitsResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			Source schema.NormalizedIssue `json:"_source"`
			Sort   []any                  `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
}

// SearchParams describes one page of a search.
type SearchParams struct {
	Q       string
	Filters map[string]string
	Limit   int
	After   []any // sort values of the last hit of the previous page (search_after)
}

// SearchResult is one page plus the sort values to resume after its last hit.
type SearchResult struct {
	Issues   []schema.NormalizedIssue
	Total    int
	Relation string // "eq", or "gte" when the count was capped at trackTotalHitsUpTo
	Next     []any  // nil when this was the last page
}

// trackTotalHitsUpTo bounds the cost of counting matches. OpenSearch's default is the
// same 10,000; making it explicit lets the API report whether the total is exact.
const trackTotalHitsUpTo = 10000

// aggBucket is a single bucket returned by a terms aggregation.
type aggBucket struct {
	Key      string `json:"key"`
	DocCount int    `json:"doc_count"`
}

// aggsOnly mirrors the aggregation-only portion of an OpenSearch response.
type aggsOnly struct {
	Aggregations map[string]struct {
		Buckets []aggBucket `json:"buckets"`
	} `json:"aggregations"`
}

// aggsWithTotal extends aggsOnly with the total hit count for filtered agg queries.
type aggsWithTotal struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
	} `json:"hits"`
	Aggregations map[string]struct {
		Buckets []aggBucket `json:"buckets"`
	} `json:"aggregations"`
}

// bucketsToMap converts a terms aggregation bucket slice to a key→count map.
func bucketsToMap(buckets []aggBucket) map[string]int {
	out := make(map[string]int, len(buckets))
	for _, b := range buckets {
		out[b.Key] = b.DocCount
	}
	return out
}

// docGetResponse mirrors the shape of GET /{index}/_doc/{id}.
type docGetResponse struct {
	Found  bool                   `json:"found"`
	Source schema.NormalizedIssue `json:"_source"`
}

// GetByID fetches a single issue by its document ID via the _doc endpoint.
// Returns (nil, nil) when the document does not exist.
func (c *OpenSearchClient) GetByID(ctx context.Context, id string) (*schema.NormalizedIssue, error) {
	url := fmt.Sprintf("%s/%s/_doc/%s", c.url, c.index, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("opensearch: build get request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opensearch: get %s: %w", id, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("opensearch: get %s: status %d: %s", id, resp.StatusCode, detail)
	}

	var dr docGetResponse
	if err := json.NewDecoder(resp.Body).Decode(&dr); err != nil {
		return nil, fmt.Errorf("opensearch: decode get response: %w", err)
	}
	if !dr.Found {
		return nil, nil
	}
	return &dr.Source, nil
}

// Search runs a bool query against the issues index and returns one page.
// When q is non-empty, multi_match on title+description is placed in must and hits are
// ordered by relevance; otherwise newest first. Ties break on the keyword id so the
// order is total, which is what makes search_after paging stable (no skips/repeats).
// Unlike from+size, search_after costs the same on page 1 and page 10,000.
func (c *OpenSearchClient) Search(ctx context.Context, p SearchParams) (*SearchResult, error) {
	sort := []any{map[string]any{"created_at": "desc"}, map[string]any{"id": "asc"}}
	if p.Q != "" {
		sort = []any{map[string]any{"_score": "desc"}, map[string]any{"id": "asc"}}
	}
	req := map[string]any{
		"size":             p.Limit,
		"query":            buildSearchQuery(p.Q, p.Filters),
		"sort":             sort,
		"track_total_hits": trackTotalHitsUpTo,
	}
	if len(p.After) > 0 {
		req["search_after"] = p.After
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("opensearch: marshal search query: %w", err)
	}

	var sr searchHitsResponse
	if err := c.post(ctx, body, &sr); err != nil {
		return nil, err
	}

	res := &SearchResult{
		Issues:   make([]schema.NormalizedIssue, 0, len(sr.Hits.Hits)),
		Total:    sr.Hits.Total.Value,
		Relation: sr.Hits.Total.Relation,
	}
	for _, hit := range sr.Hits.Hits {
		res.Issues = append(res.Issues, hit.Source)
	}
	if n := len(sr.Hits.Hits); n == p.Limit && n > 0 {
		res.Next = sr.Hits.Hits[n-1].Sort
	}
	return res, nil
}

// Aggregate returns term-count buckets for status, priority, and source.
func (c *OpenSearchClient) Aggregate(ctx context.Context) (*StatsResponse, error) {
	body, err := json.Marshal(map[string]any{
		"size": 0,
		"aggs": map[string]any{
			"by_status":   map[string]any{"terms": map[string]any{"field": "status", "size": 50}},
			"by_priority": map[string]any{"terms": map[string]any{"field": "priority", "size": 50}},
			"by_source":   map[string]any{"terms": map[string]any{"field": "source", "size": 50}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("opensearch: marshal agg query: %w", err)
	}

	var ar aggsOnly
	if err := c.post(ctx, body, &ar); err != nil {
		return nil, err
	}

	return &StatsResponse{
		ByStatus:   bucketsToMap(ar.Aggregations["by_status"].Buckets),
		ByPriority: bucketsToMap(ar.Aggregations["by_priority"].Buckets),
		BySource:   bucketsToMap(ar.Aggregations["by_source"].Buckets),
	}, nil
}

// GetAggregate runs a filtered terms aggregation and returns grouped counts plus
// the total number of matching documents. When filters is empty it aggregates
// over all issues, equivalent to /issues/stats but with the total field added.
func (c *OpenSearchClient) GetAggregate(ctx context.Context, filters map[string]string) (*AggregateResponse, error) {
	query := map[string]any{"match_all": map[string]any{}}
	if len(filters) > 0 {
		termFilters := make([]any, 0, len(filters))
		for field, value := range filters {
			termFilters = append(termFilters, map[string]any{
				"term": map[string]any{field: value},
			})
		}
		query = map[string]any{"bool": map[string]any{"filter": termFilters}}
	}

	body, err := json.Marshal(map[string]any{
		"size":  0,
		"query": query,
		"aggs": map[string]any{
			"by_status":   map[string]any{"terms": map[string]any{"field": "status", "size": 50}},
			"by_priority": map[string]any{"terms": map[string]any{"field": "priority", "size": 50}},
			"by_source":   map[string]any{"terms": map[string]any{"field": "source", "size": 50}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("opensearch: marshal aggregate query: %w", err)
	}

	var ar aggsWithTotal
	if err := c.post(ctx, body, &ar); err != nil {
		return nil, err
	}

	return &AggregateResponse{
		Total:      ar.Hits.Total.Value,
		Filters:    filters,
		ByStatus:   bucketsToMap(ar.Aggregations["by_status"].Buckets),
		ByPriority: bucketsToMap(ar.Aggregations["by_priority"].Buckets),
		BySource:   bucketsToMap(ar.Aggregations["by_source"].Buckets),
	}, nil
}

func (c *OpenSearchClient) post(ctx context.Context, body []byte, dst any) error {
	url := fmt.Sprintf("%s/%s/_search", c.url, c.index)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opensearch: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("opensearch: search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("opensearch: search: status %d: %s", resp.StatusCode, detail)
	}

	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		return fmt.Errorf("opensearch: decode response: %w", err)
	}
	return nil
}

// buildSearchQuery constructs the OpenSearch query object from optional text and filters.
func buildSearchQuery(q string, filters map[string]string) map[string]any {
	if q == "" && len(filters) == 0 {
		return map[string]any{"match_all": map[string]any{}}
	}

	boolQ := map[string]any{}

	if q != "" {
		boolQ["must"] = []any{
			map[string]any{
				"multi_match": map[string]any{
					"query":  q,
					"fields": []string{"title", "description"},
				},
			},
		}
	}

	if len(filters) > 0 {
		termFilters := make([]any, 0, len(filters))
		for field, value := range filters {
			termFilters = append(termFilters, map[string]any{
				"term": map[string]any{field: value},
			})
		}
		boolQ["filter"] = termFilters
	}

	return map[string]any{"bool": boolQ}
}
