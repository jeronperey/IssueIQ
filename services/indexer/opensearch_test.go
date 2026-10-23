package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jeronperey/issueiq/shared/schema"
)

func TestEnsureIndex_AlreadyExists(t *testing.T) {
	putCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		putCalled = true
	}))
	defer srv.Close()

	c := NewOpenSearchClient(srv.URL, "issues")
	if err := c.EnsureIndex(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if putCalled {
		t.Error("create request was made even though index already exists")
	}
}

func TestEnsureIndex_CreatesWithMapping(t *testing.T) {
	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		capturedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewOpenSearchClient(srv.URL, "issues")
	if err := c.EnsureIndex(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(capturedBody, &body); err != nil {
		t.Fatalf("PUT body is not valid JSON: %v", err)
	}

	// settings.number_of_replicas must be 0 (required for single-node OpenSearch)
	settings, _ := body["settings"].(map[string]any)
	if replicas := settings["number_of_replicas"]; replicas != float64(0) {
		t.Errorf("number_of_replicas: got %v, want 0", replicas)
	}

	mappings, _ := body["mappings"].(map[string]any)
	props, _ := mappings["properties"].(map[string]any)

	for _, field := range []string{"id", "source", "status", "priority", "tags", "schema_version"} {
		fp, _ := props[field].(map[string]any)
		if fp["type"] != "keyword" {
			t.Errorf("field %q: type got %v, want keyword", field, fp["type"])
		}
	}
	for _, field := range []string{"title", "description"} {
		fp, _ := props[field].(map[string]any)
		if fp["type"] != "text" {
			t.Errorf("field %q: type got %v, want text", field, fp["type"])
		}
	}
	for _, field := range []string{"created_at", "updated_at"} {
		fp, _ := props[field].(map[string]any)
		if fp["type"] != "date" {
			t.Errorf("field %q: type got %v, want date", field, fp["type"])
		}
	}
}

func TestUpsertDocument_RoutesCorrectly(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewOpenSearchClient(srv.URL, "issues")
	issue := schema.NormalizedIssue{ID: "abc123", Title: "test issue", Source: "github"}
	if err := c.UpsertDocument(context.Background(), issue); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("method: got %q, want PUT", gotMethod)
	}
	if want := "/issues/_doc/abc123"; gotPath != want {
		t.Errorf("path: got %q, want %q", gotPath, want)
	}
}

func TestUpsertDocument_StatusCodes(t *testing.T) {
	cases := []struct {
		status  int
		wantErr bool
	}{
		{http.StatusOK, false},
		{http.StatusCreated, false},
		{http.StatusInternalServerError, true},
		{http.StatusBadRequest, true},
	}
	for _, tc := range cases {
		tc := tc
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
		}))
		c := NewOpenSearchClient(srv.URL, "issues")
		err := c.UpsertDocument(context.Background(), schema.NormalizedIssue{ID: "x"})
		srv.Close()
		if (err != nil) != tc.wantErr {
			t.Errorf("status %d: got err=%v, wantErr=%v", tc.status, err != nil, tc.wantErr)
		}
	}
}

func TestBulkUpsert_ClassifiesItems(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_bulk" {
			t.Errorf("path = %s, want /_bulk", r.URL.Path)
		}
		w.Write([]byte(`{"errors":true,"items":[
			{"index":{"status":201}},
			{"index":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"queue full"}}},
			{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad date"}}}]}`))
	}))
	defer srv.Close()

	c := NewOpenSearchClient(srv.URL, "issues")
	res, err := c.BulkUpsert(context.Background(), []schema.NormalizedIssue{{ID: "a"}, {ID: "b"}, {ID: "c"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].ok() || !res[1].transient() || res[2].ok() || res[2].transient() {
		t.Fatalf("unexpected classification: %+v", res)
	}
}

func TestBulkUpsert_RequestFailureIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	if _, err := NewOpenSearchClient(srv.URL, "issues").BulkUpsert(context.Background(), []schema.NormalizedIssue{{ID: "a"}}); err == nil {
		t.Fatal("want error on 503")
	}
}
