# IssueIQ

IssueIQ is an event-driven issue-tracking pipeline built to learn the CrowdStrike stack before my internship. It ingests raw issues from multiple sources, normalizes them through Kafka, indexes them in OpenSearch, and exposes a query API for search, filtering, and aggregation.

---

## Performance and reliability (measured)

On a single M4 laptop (everything co-located, see [docs/BENCHMARKS.md](docs/BENCHMARKS.md)):

- **33k events/sec** sustained through Kafka -> normalizer -> Kafka -> indexer -> OpenSearch with 4 partitions / 4+4 consumers (12k on 1 partition; the original unbatched code did ~1/sec).
- **p99 ingest-to-indexed 251 ms, p99 send-to-searchable 1.3 s** at 17.8k events/sec (the 1 s OpenSearch refresh interval dominates the latter).
- **0 lost, 0 duplicate documents across 30 fault-injection runs (775k acknowledged events)**: SIGKILLs, Kafka and OpenSearch restarts, malformed input, DLQ replay.

```bash
make chaos           # fault-injection suite
make bench-scaling   # throughput vs partitions/consumers
# Grafana: http://localhost:3000   Prometheus: http://localhost:9090
```

---

## Architecture

```
  HTTP client
      │
      ▼
┌─────────────────┐
│  ingestion      │  :8080
│  POST /ingest   │  validates, wraps in EventEnvelope, publishes by source_id key
└────────┬────────┘
         │
         ▼
   ┌─────────────┐
   │  raw-issues │  Kafka topic
   └─────┬───────┘
         │
         ▼
┌─────────────────┐
│  normalizer     │  consumer group: normalizer-group
│                 │  canonicalizes status/priority, generates stable SHA256 ID
│                 ├──────────────────────────────────► dead-letter (bad messages)
└────────┬────────┘
         │
         ▼
   ┌──────────────────┐
   │ normalized-issues│  Kafka topic
   └──────┬───────────┘
          │
          ▼
┌──────────────────┐
│  indexer         │  consumer group: indexer-group
│                  │  upserts NormalizedIssue to OpenSearch by document ID
│                  ├──────────────────────────────────► dead-letter (bad messages)
└────────┬─────────┘
         │
         ▼
   ┌──────────────┐
   │ OpenSearch   │  index: issues
   │              │  keyword fields for filter/agg, text fields for search
   └──────┬───────┘
          │
          ▼
┌─────────────────┐
│  query          │  :8082
│  GET /issues    │  search, filter, aggregate, get-by-ID
└─────────────────┘
```

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `raw-issues` | ingestion | normalizer | Raw `RawIssue` structs wrapped in `EventEnvelope`; keyed by `source_id` for partition ordering |
| `normalized-issues` | normalizer | indexer | Canonical `NormalizedIssue` structs ready for indexing; stable ID already assigned |
| `dead-letter` | normalizer, indexer | (manual replay) | Unprocessable messages with `reason` + original payload; written on malformed JSON, missing title, or upsert failure |

The message key on `raw-issues` is set to `source_id` so all events from the same issue land on the same partition, preserving order if the same issue is updated multiple times.

---

## OpenSearch Index

**Index:** `issues` | **Replicas:** 0 (single-node local setup)

| Field | Mapping type | Why |
|---|---|---|
| `id` | `keyword` | Stable SHA256 of `source:source_id`; used as the document ID for idempotent upserts |
| `source` | `keyword` | Exact-match filter and `terms` aggregation |
| `title` | `text` | Tokenized for full-text search via `multi_match` |
| `description` | `text` | Tokenized for full-text search via `multi_match` |
| `status` | `keyword` | Exact-match filter and `terms` aggregation |
| `priority` | `keyword` | Exact-match filter and `terms` aggregation |
| `tags` | `keyword` | Array; exact-match filter |
| `created_at` | `date` | RFC3339; sortable |
| `updated_at` | `date` | RFC3339; sortable |

`keyword` fields support exact `term` queries and aggregations. `text` fields are analyzed (tokenized) and support fuzzy/partial matching but cannot be aggregated directly. The distinction matters: querying `status` with `match` instead of `term` would work for simple values but fail for multi-word statuses and can't be used in `terms` aggs.

---

## How to Run

**Prerequisites:** Docker, Docker Compose, Go 1.22+

```bash
# Start all services (Kafka, OpenSearch, ingestion, normalizer, indexer, query)
docker compose up -d

# Wait ~15s for OpenSearch to become healthy, then seed 5 demo issues
make seed

# Confirm data is indexed
curl 'localhost:9200/issues/_count'
```

Useful Make targets:

```bash
make seed          # POST 5 fixed issues through the full pipeline
make test          # run unit tests (normalizer, indexer, shared)
make logs          # tail all service logs
make logs-query    # tail query service only
make reset         # docker compose down -v && up (wipes all data)
```

---

## Demo

```bash
# Health checks
curl localhost:8080/health       # ingestion
curl localhost:8082/health       # query

# Full-text search across title + description
curl "localhost:8082/issues/search?q=login"

# Filter by multiple fields (AND)
curl "localhost:8082/issues?status=open&priority=critical"

# Global counts by status, priority, source
curl localhost:8082/issues/stats

# Filtered aggregation: breakdown of open issues only
curl "localhost:8082/issues/aggregate?status=open"

# Compound filter aggregation
curl "localhost:8082/issues/aggregate?status=open&source=github"

# Fetch a specific issue by ID (get an ID from the list first)
curl "localhost:8082/issues?status=open"
curl localhost:8082/issues/<id_from_above>

# OpenSearch raw view
curl "localhost:9200/issues/_search?pretty&size=3"
```

---

## Query API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service health check |
| `GET` | `/issues` | List issues; optional `?status=`, `?priority=`, `?source=` filters |
| `GET` | `/issues/search` | Full-text search with `?q=`; optional filters |
| `GET` | `/issues/stats` | Global term-count aggregations (no filter, no total) |
| `GET` | `/issues/aggregate` | Filtered term-count aggregations with total hit count |
| `GET` | `/issues/{id}` | Fetch one issue by its document ID; 404 if not found |

All responses are JSON. All endpoints return `[]` (not `null`) for empty issue lists.

---

## What I Learned

Building this forced me to understand things I would have just read about otherwise:

**Kafka:** The gap between "message delivered" and "offset committed" is where at-least-once delivery actually lives. I had to think carefully about when the normalizer commits — after writing to the output topic and after the DLQ write succeeds, not before. I also learned why consumer groups matter: running two normalizer instances splits partitions between them, so you get parallelism without duplicates, but only if you have enough partitions.

**OpenSearch:** The query DSL clicked when I realized it's composable: `bool.must` affects relevance scoring, `bool.filter` affects which documents are included without touching scores. For aggregations the key insight is that agg buckets are scoped to whatever the `query` matches — if you want filtered counts, the filter goes in the main query, not in a post-filter or a separate agg clause.

**Go:** Writing HTTP service code in Go is refreshingly minimal once you know the 1.22 mux patterns (`r.PathValue`, method+path routing). The trickiest pattern was distinguishing `(nil, nil)` for "not found" from `(nil, err)` for "something broke" — callers have to handle both cases explicitly, which forces you to think about the 404 vs 500 distinction at the library level rather than the handler level.

**What I'd improve:**

- **Producer acknowledgment:** I ran an automated test suite after completing the project and it caught that all three Kafka writers were configured with the default `RequiredAcks=0` (fire-and-forget). Under a broker outage the ingestion service was returning 202 even though messages weren't confirmed. Fixed to `RequireOne` so publish failures surface as errors. `RequireOne` means the partition leader acks — with `RequireAll` you'd also wait for in-sync replicas, which only matters with replication. Locally there are no replicas, so `RequireOne` is the right call here; `RequireAll` would be the production choice.
- **Consumer restart on broker loss:** When Kafka stopped during testing, the normalizer container exited on the fetch error instead of retrying. A production consumer should loop with backoff rather than crashing. The fix is a retry loop around `FetchMessage` rather than returning the error to the caller.
- **Pagination:** The query service hardcodes `size: 20`. A real API needs `from`/`size` or cursor-based pagination for large datasets.
- **Auth:** The query API is completely open. Even basic API key middleware would make this more production-realistic.
- **DLQ observability:** Dead-letter messages are written but never alerted on. In production you'd want a consumer that monitors DLQ lag and pages someone.
- **Sorting:** Search results come back in relevance order, filter results in index order. Explicit `sort` on `updated_at` descending would make the API more useful.
- **The normalizer's status fallback:** Unknown statuses default to `"open"`, which is a business assumption baked silently into the code. That should be configurable or at least logged clearly.

---

## Future Implementations

**Agentic enrichment pipeline**

Add an `enricher` service as a new stage between the normalizer and indexer. It consumes from `normalized-issues`, calls an LLM to classify each issue — inferring affected component, likely root cause category, and suggested owner team — then publishes to a new `enriched-issues` topic that the indexer reads from instead. The architecture doesn't change; it's just a new consumer in the chain. New fields like `component`, `root_cause_category`, and `suggested_owner` get indexed alongside the existing ones and become filterable and aggregatable through the query API. This is the natural next step for the pipeline: the normalization stage makes data consistent, the enrichment stage makes it *intelligent*.

**Natural language query interface**

Add a `POST /issues/ask` endpoint to the query service that accepts a plain English question (`"show me all critical auth bugs from the last week"`), sends it to an LLM with the index field schema as context, receives back a valid OpenSearch DSL query, executes it, and returns results. No new infrastructure required — it's a thin LLM-backed layer on top of the existing OpenSearch client. The interesting constraint is prompt design: the LLM needs to know which fields are `keyword` vs `text` (affects whether to use `term` or `match`), which fields are `date` (enables range queries), and that aggregations require `keyword` fields. Getting that context into the prompt reliably is the engineering problem, not the query execution itself.
