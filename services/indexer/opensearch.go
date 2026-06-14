package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

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
