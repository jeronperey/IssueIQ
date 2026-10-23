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

func NewOpenSearchClient(url, index string) *OpenSearchClient {
	return &OpenSearchClient{
		http:  &http.Client{},
		url:   url,
		index: index,
	}
}

// indexMapping defines the settings and mappings for the OpenSearch index.
// We set number_of_replicas to 0 for simplicity, but in a production environment,
// you would likely want to configure this based on your needs.
// The mappings define the data types for each field, which helps OpenSearch optimize storage and search performance.
var indexMapping = map[string]any{
	"settings": map[string]any{
		"number_of_replicas": 0,
	},
	"mappings": map[string]any{
		"properties": map[string]any{
			"schema_version": map[string]string{"type": "keyword"},
			"id":             map[string]string{"type": "keyword"},
			"source":         map[string]string{"type": "keyword"},
			"title":          map[string]string{"type": "text"},
			"description":    map[string]string{"type": "text"},
			"status":         map[string]string{"type": "keyword"},
			"priority":       map[string]string{"type": "keyword"},
			"tags":           map[string]string{"type": "keyword"},
			"created_at":     map[string]string{"type": "date"}, // For sorting and filtering, we want to use the date type. For display, we can format it as needed.
			"updated_at":     map[string]string{"type": "date"},
			"ingested_at":    map[string]string{"type": "date"},
			"indexed_at":     map[string]string{"type": "date"},
		},
	},
}

// EnsureIndex checks whether the index exists and creates it with an explicit
// mapping if it does not. A 404 on HEAD means it needs creation.
func (c *OpenSearchClient) EnsureIndex(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.url+"/"+c.index, nil)
	if err != nil {
		return fmt.Errorf("opensearch: build head request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("opensearch: head index %q: %w", c.index, err)
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("opensearch: head index %q: unexpected status %d", c.index, resp.StatusCode)
	}

	body, err := json.Marshal(indexMapping)
	if err != nil {
		return fmt.Errorf("opensearch: marshal index mapping: %w", err)
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodPut, c.url+"/"+c.index, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opensearch: build create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err = c.http.Do(req)
	if err != nil {
		return fmt.Errorf("opensearch: create index %q: %w", c.index, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		detail, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("opensearch: create index %q: status %d: %s", c.index, resp.StatusCode, detail)
	}
	return nil
}

// UpsertDocument writes issue to OpenSearch as PUT /{index}/_doc/{id}.
// Using the document ID from the stable sha256 hash makes this operation
// idempotent: reprocessing the same issue overwrites rather than duplicates.
func (c *OpenSearchClient) UpsertDocument(ctx context.Context, issue schema.NormalizedIssue) error {
	body, err := json.Marshal(issue)
	if err != nil {
		return fmt.Errorf("opensearch: marshal document: %w", err)
	}

	url := fmt.Sprintf("%s/%s/_doc/%s", c.url, c.index, issue.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opensearch: build upsert request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("opensearch: upsert %s: %w", issue.ID, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("opensearch: upsert %s: unexpected status %d", issue.ID, resp.StatusCode)
	}
	return nil
}

// bulkItemResult is the outcome of one document in a _bulk request.
type bulkItemResult struct {
	Status int
	Err    string
}

// transient reports whether the item failed for a reason that may clear on retry
// (back-pressure or a server-side problem) as opposed to a document the index will
// never accept (mapping conflict, bad request).
func (r bulkItemResult) transient() bool { return r.Status == 429 || r.Status >= 500 }
func (r bulkItemResult) ok() bool        { return r.Status == 200 || r.Status == 201 }

// BulkUpsert indexes issues with one _bulk request, stamping IndexedAt on each.
// A non-nil error means the whole request failed (connection error, 429/5xx on
// the request itself) and nothing can be assumed about any item; callers retry it.
// Otherwise the result slice is parallel to issues.
func (c *OpenSearchClient) BulkUpsert(ctx context.Context, issues []schema.NormalizedIssue) ([]bulkItemResult, error) {
	var buf bytes.Buffer
	now := time.Now().UTC()
	for _, issue := range issues {
		issue.IndexedAt = now
		meta, _ := json.Marshal(map[string]any{"index": map[string]string{"_index": c.index, "_id": issue.ID}})
		doc, err := json.Marshal(issue)
		if err != nil {
			return nil, fmt.Errorf("opensearch: marshal %s: %w", issue.ID, err)
		}
		buf.Write(meta)
		buf.WriteByte('\n')
		buf.Write(doc)
		buf.WriteByte('\n')
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/_bulk", &buf)
	if err != nil {
		return nil, fmt.Errorf("opensearch: build bulk request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opensearch: bulk: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("opensearch: bulk read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opensearch: bulk: status %d: %.200s", resp.StatusCode, data)
	}

	var parsed struct {
		Items []map[string]struct {
			Status int `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("opensearch: bulk decode: %w", err)
	}
	if len(parsed.Items) != len(issues) {
		return nil, fmt.Errorf("opensearch: bulk returned %d items for %d docs", len(parsed.Items), len(issues))
	}
	results := make([]bulkItemResult, len(issues))
	for i, item := range parsed.Items {
		for _, v := range item { // single key: "index"
			results[i] = bulkItemResult{Status: v.Status}
			if v.Error.Type != "" {
				results[i].Err = v.Error.Type + ": " + v.Error.Reason
			}
		}
	}
	return results, nil
}
