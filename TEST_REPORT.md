# IssueIQ — Autonomous Validation Report

**Run date:** 2026-06-14  
**Tested by:** Claude Code (autonomous, per CLAUDE_CODE_TEST_BRIEF.md)  
**Repo root:** `/Users/jeronperey/Projects/IssueIQ`

---

## 1. Gate Verdict

| Gate | Requirement | Verdict |
|------|-------------|---------|
| G1 | 2–3 services exist and run (health endpoints respond 200) | **PASS** |
| G2 | Kafka runs locally, all three topics exist | **PASS** |
| G3 | One issue in via `/ingest`, same issue retrievable via `/issues/search` | **PASS** |
| G4 | Aggregate queries (`/issues/stats`, `/issues/aggregate`) return correct grouped counts | **PASS** |

**All four spec-compliance gates pass.** The project is ready to show.

---

## 2. Environment

### Discovered vs. Roadmap

| Item | Roadmap assumed | Actual |
|------|----------------|--------|
| Ingestion port | `:8080` | `:8080` ✓ |
| Query port | `:8082` | `:8082` ✓ |
| OpenSearch port | `:9200` | `:9200` ✓ |
| Kafka port | `:9092` | `:9092` ✓ |
| Ingestion route | `POST /ingest` | `POST /ingest`, `POST /ingest/batch` ✓ |
| Query routes | `/issues`, `/issues/search`, `/issues/stats`, `/issues/aggregate`, `/issues/{id}` | All present ✓ |
| Kafka topics | `raw-issues`, `normalized-issues`, `dead-letter` | All present ✓ |
| Run mechanism | `go run` or Docker | Ingestion/normalizer/indexer in Docker; query as local binary |

### Kafka topics at end of run

| Topic | End offset | Notes |
|-------|-----------|-------|
| `raw-issues` | 64 | Normalizer lag: 0 |
| `normalized-issues` | 63 | Indexer lag: 0 (offset 64 − 1 DLQ = 63) |
| `dead-letter` | 1 | One no-title message from dlq-1 test |

### OpenSearch at end of run

- **Index:** `issues`  
- **Docs:** 22 (mix of seed data + test payloads, all idempotent)  
- **Cluster status:** `green`  
- **Replicas:** `0` (correct for single-node — cluster is green, not yellow)

### Env vars consumed by each service

| Service | Key env vars |
|---------|-------------|
| ingestion | `KAFKA_ADDR` (default `localhost:9092`), `KAFKA_TOPIC` (default `raw-issues`) |
| normalizer | `KAFKA_BROKERS`, `RAW_ISSUES_TOPIC`, `NORMALIZED_ISSUES_TOPIC`, `DLQ_TOPIC`, `CONSUMER_GROUP` |
| indexer | `KAFKA_BROKERS`, `NORMALIZED_ISSUES_TOPIC`, `DLQ_TOPIC`, `CONSUMER_GROUP`, `OPENSEARCH_URL`, `OPENSEARCH_INDEX` |
| query | `OPENSEARCH_URL` (default `http://localhost:9200`), `OPENSEARCH_INDEX` (default `issues`) |

---

## 3. Per-Phase Results

### Phase 4 — Ingestion

| Test | Expected | Actual | Result |
|------|----------|--------|--------|
| Valid `POST /ingest` | 202 + `event_id` | `{"event_id":"1a54853c-...","status":"queued"}` HTTP 202 | PASS |
| Missing `source_id` | 400 | `source_id must not be empty` HTTP 400 | PASS |
| Disallowed source (`slack`) | 400 | `source "slack" is not allowed` HTTP 400 | PASS |
| Nil `raw_payload` | 400 | `raw_payload must not be nil` HTTP 400 | PASS |
| Malformed JSON (truncated) | 400, no panic | `invalid JSON` HTTP 400; health still 200 afterward | PASS |
| `/ingest/batch` 3 valid + 2 invalid | 202 `{queued:3,failed:2}` | `{"failed":2,"queued":3}` HTTP 202 | PASS |

### Phase 5 — Normalizer

| Test | Expected | Actual | Result |
|------|----------|--------|--------|
| Status `OPEN` → | `open` | `open` | PASS |
| Status `Open` → | `open` | `open` | PASS |
| Status `open` → | `open` | `open` | PASS |
| Status `frobnicated` → | `open` (default) | `open` | PASS |
| Priority `P0` → | `critical` | `critical` | PASS |
| Priority `urgent` → | `critical` | `critical` | PASS |
| Priority `blocker` → | `critical` | `critical` | PASS |
| Priority `wat` → | `medium` (default) | `medium` | PASS |
| Tags `["Go","  go  ","GO","backend","Backend"]` → | `["backend","go"]` (lowercase, dedup, sorted, non-null) | `['backend', 'go']` | PASS |
| Empty `raw_payload` (no title) → DLQ | message in dead-letter with `reason`, `original_payload`, `failed_at` | Present and correct (see §4) | PASS |
| Same `source+source_id` twice → 1 doc | count == 1 | count == 1 | PASS |
| DLQ: normalizer commits and survives after bad message | consumer still running, lag = 0 | lag=0 after dlq-1 processed | PASS |

### Phase 6 — Indexer

| Test | Expected | Actual | Result |
|------|----------|--------|--------|
| `status` mapping | `keyword` | `keyword` | PASS |
| `priority` mapping | `keyword` | `keyword` | PASS |
| `source` mapping | `keyword` | `keyword` | PASS |
| `tags` mapping | `keyword` | `keyword` | PASS |
| `id` mapping | `keyword` | `keyword` | PASS |
| `title` mapping | `text` | `text` | PASS |
| `description` mapping | `text` | `text` | PASS |
| `created_at` mapping | `date` | `date` | PASS |
| `updated_at` mapping | `date` | `date` | PASS |
| `make seed` twice — count stable | count unchanged | 19 → 19 (upsert, not insert) | PASS |
| Restart indexer — `EnsureIndex` on existing index | no error | indexer-group lag stays 0 | PASS |

### Phase 7 — Query

| Test | Expected | Actual | Result |
|------|----------|--------|--------|
| `/issues/search?q=login` | title matches, score-ranked | 2 results: "fix login bug", "login redirect loop after OAuth" | PASS |
| `/issues/search?q=Login%20Bug` (capitalized) | still matches (analyzer) | 2 results | PASS |
| `/issues?status=open&priority=critical` | only docs where both match | 5 results, all `status=open priority=critical` | PASS |
| `/issues/stats` by_status sums to total | sum == 22 | closed:1 + in_progress:1 + open:17 = 19 at that point; **sum == total** ✓ | PASS |
| `/issues/stats` by_priority sums to total | sum == total | critical:6 + high:5 + low:2 + medium:6 = 19 ✓ | PASS |
| `/issues/stats` by_source sums to total | sum == total | github:15 + mock:1 + zendesk:3 = 19 ✓ | PASS |
| `/issues/aggregate?status=open` `total` | == open count | total=17, by_status={"open":17} only | PASS |
| `/issues/aggregate?status=open` by_priority/by_source | reflect only open subset | in_progress absent from by_status; buckets scoped to open docs | PASS |
| `/issues/aggregate?status=open&source=github` | intersection only | total=14, by_source={"github":14} only | PASS |
| `/issues/search?q=zzzznonexistent` | 200 + empty array | `{"issues":[],"total":0}` HTTP 200 | PASS |
| `/issues/aggregate?status=does_not_exist` | total:0, empty buckets | `{"total":0,"by_status":{},"by_priority":{},"by_source":{}}` | PASS |
| `/issues/{real_id}` | 200 + full issue | correct title/status/source returned | PASS |
| `/issues/{fake_id}` | 404 | `not found` HTTP 404 | PASS |

---

## 4. Canary End-to-End Trace

**Issue:** `source=github`, `source_id=canary-1`, `title=trace-canary-XYZ`

**Hop 1 — Ingestion:**
```
POST /ingest → {"event_id":"93a35f1c-d8aa-4ddf-b27e-f75946b06a24","status":"queued"} HTTP 202
```

**Hop 2 — Kafka `raw-issues`:**  
Normalizer consumer group confirmed lag=0 throughout run; raw-issues offset advanced with each test batch.

**Hop 3 — `normalized-issues` → OpenSearch:**  
Indexer consumer group offset = 63 (63 normalized messages indexed), lag = 0.

**Hop 4a — Searchable via query API:**
```
GET /issues/search?q=canary
→ title=trace-canary-XYZ  id=301e1ccc215ca63445b1e3ff080195a43dc42d2ad75e9c5e8525d113da696b54
```

**Hop 4b — Retrievable by computed ID:**
```
GET /issues/301e1ccc215ca63445b1e3ff080195a43dc42d2ad75e9c5e8525d113da696b54
→ title=trace-canary-XYZ source=github status=open priority=critical
```

**Hop 5 — Independent ID verification:**
```
python3 scripts/compute_id.py github canary-1
→ 301e1ccc215ca63445b1e3ff080195a43dc42d2ad75e9c5e8525d113da696b54

_source.id from OpenSearch:
→ 301e1ccc215ca63445b1e3ff080195a43dc42d2ad75e9c5e8525d113da696b54

MATCH: Python SHA256 == Go sha256.Sum256 implementation
```

All five hops confirmed. The pipeline is end-to-end correct.

---

## 5. Adversarial Results

| Test | Expected | Actual | Result |
|------|----------|--------|--------|
| Kafka-down ingestion | Sane error (5xx or clear failure), no hang | Returns **202** in 1s — see note below | **CONCERN** |
| Large payload (~1MB in `raw_payload`) | Handled or cleanly rejected, no crash | 202; survived pipeline | PASS (no crash) |
| Unicode/emoji (`日本語 login 🐛 bug`) | Survives full pipeline, searchable | Retrieved from OpenSearch with emoji intact | PASS |
| 20× concurrent ingest of same issue | Exactly 1 document | count=1 after 20 parallel requests | PASS |
| Empty search result | 200 + `{"issues":[],"total":0}` | Exactly that | PASS |
| Empty-match aggregate | `total:0`, empty buckets, no crash | `{"total":0,"by_status":{},...}` | PASS |
| DLQ payload quality | `reason`, `original_payload`, `failed_at` all present | All three fields present; full envelope with `source_id` traceable | PASS |
| Restart durability | Data persists after service restart | 22 docs before and after query restart; OpenSearch volume-backed | PASS |

**Kafka-down note:** When Kafka was stopped and a POST was sent to ingestion, the service returned `HTTP 202` in 1 second — it did not hang or panic. However, the 202 is misleading: the `kafka.Writer` is configured without `RequiredAcks`, which defaults to `RequireNone` (0) in kafka-go. This means the producer does not wait for broker acknowledgment; messages are sent fire-and-forget. Under a real broker outage, messages ingested during that window could be silently lost. The behavior is safe (no crash, no hang) but not honest about delivery.

---

## 6. Static Sweep

| Check | Result | Notes |
|-------|--------|-------|
| `go vet ./...` | **FAIL** | `scripts/kafka-learn`: two `main()` declarations in one package |
| `go vet ./services/... ./shared/...` | PASS | Production code is clean |
| `gofmt -l .` | PASS | All files formatted |
| `go build ./...` | **FAIL** | Same `scripts/kafka-learn` issue |
| `go build ./services/... ./shared/...` | PASS | All production services build clean |
| `go test ./services/... ./shared/...` | PASS | indexer ✓, normalizer ✓, shared/schema ✓ |
| `services/ingestion` tests | n/a | No test files |
| `services/query` tests | n/a | No test files |

---

## 7. Failures in Full

### FAIL-1: `go vet ./...` and `go build ./...`

**Command:**
```bash
go vet ./...
```
**Raw output:**
```
# github.com/jeronperey/issueiq/scripts/kafka-learn
vet: scripts/kafka-learn/producer.go:14:6: main redeclared in this block
```
**Root cause:** `scripts/kafka-learn/` is a learning sandbox with `producer.go` and `consumer.go` each declaring `package main` and their own `main()` function. Go treats them as one package and rejects the duplicate declaration. This directory is already excluded from `make test`.  
**Fix:** Add `//go:build ignore` to one file, or move them into separate directories (`kafka-learn/producer/`, `kafka-learn/consumer/`).

---

### CONCERN-1: Kafka-down returns 202 (not a hard FAIL but worth documenting)

**Command:**
```bash
docker compose stop kafka
curl -s -w "HTTP %{http_code}" -X POST http://localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -d '{"source_id":"test","source":"github","raw_payload":{"title":"test","status":"open","priority":"low"}}'
```
**Output:** `{"event_id":"f372...","status":"queued"} HTTP 202`  
**Root cause:** `kafka.Writer` in `services/ingestion/producer.go` is initialized without `RequiredAcks`. kafka-go's default is `RequireNone` (0), meaning the producer does not block for broker acknowledgment. Under a Kafka outage the client-side write appears to succeed.  
**Impact:** Messages ingested during an outage may be silently lost. No crash or hang, so the service stays operational, but delivery guarantee is weaker than the 202 implies.  
**Fix:** Set `writer.RequiredAcks = kafka.RequireOne` in `NewKafkaProducer`.

---

### MINOR-1: Double "normalize:" prefix in DLQ reason

**Observed:**
```json
{"reason":"normalize: normalize: title is missing or empty", ...}
```
**Root cause:** `normalize.go` returns `fmt.Errorf("normalize: title is missing or empty")`, and the consumer wraps it again with `fmt.Errorf("normalize: %w", err)`. Human-readable but redundant.  
**Fix:** Either remove the prefix from `NormalizeIssue` or from the consumer's wrapping call.

---

## 8. Prioritized Fix List

### Blocks showing Alexandru
*Nothing here. All four spec gates pass.*

### Nice to have / post-internship

| Priority | Fix |
|----------|-----|
| Medium | **`go build ./...` fails** — add `//go:build ignore` to `scripts/kafka-learn/producer.go` or `consumer.go`. Trivial one-line fix that makes the static sweep fully clean. |
| Medium | **`RequiredAcks=0` on Kafka producer** — set `RequireOne` in `NewKafkaProducer`. Closes the silent-loss gap on broker outages; changes behavior from fire-and-forget to at-least-broker-received. |
| Low | **Double "normalize:" prefix in DLQ reason** — remove prefix from either the return in `NormalizeIssue` or the consumer's `fmt.Errorf("normalize: %w", err)` wrapper. |
| Low | **No request body size limit on ingestion** — a 1MB payload ingested successfully; no hard limit means memory pressure under abuse. Add `http.MaxBytesReader` to the handler. |
| Low | **Pagination missing from query service** — `/issues` is hardcoded `size: 20`. Add `?limit=` and `?offset=` or a cursor. |
| Low | **No tests for `services/ingestion` or `services/query`** — the normalizer and indexer have tests; the other two services have zero. At minimum, table-driven tests for the validator and route handlers. |

---

## 9. Open Conceptual Questions (self-check before messaging Alexandru)

Answer these cold without looking at the code:

1. **2 normalizers on a 2-partition topic — how does Kafka divide work? What if 3 normalizers on 2 partitions?**  
   *(Key: partition assignment within a consumer group; the third instance sits idle)*

2. **Normalizer crashes after processing offset 47, before committing. On restart, what happens and why is it safe?**  
   *(Key: at-least-once delivery; re-reads from offset 47; downstream upsert is idempotent)*

3. **Why is the Kafka message key set to `source_id`? What ordering guarantee does it provide?**  
   *(Key: same key → same partition → ordered delivery for events on a single issue)*

4. **Why `keyword` for `status` but `text` for `title`? What breaks if you swap them?**  
   *(Key: keyword = exact match + agg; text = tokenized. Swapping: `term` queries on `title` break; `terms` agg on `status` breaks)*

5. **`bool.must` vs `bool.filter` — how do they differ in scoring and performance?**  
   *(Key: must contributes to `_score`; filter does not and is cacheable. Use filter for keyword exact-match.)*

6. **Why does `/issues/stats` aggregate over everything but `/issues/aggregate?status=open` over a subset?**  
   *(Key: `stats` uses `match_all` with no query; `aggregate` puts filters in `query.bool.filter` which scopes the agg buckets)*

7. **Stable hashed ID vs random UUID under at-least-once delivery — why does it matter?**  
   *(Key: same message reprocessed → same ID → upsert overwrites → no duplicate. Random UUID → duplicate on reprocess.)*

8. **`%w` vs `%v` in `fmt.Errorf` — what's the difference?**  
   *(Key: `%w` wraps so `errors.Is`/`errors.As` can unwrap; `%v` buries the error as a string, callers can't inspect type)*

9. **Why is `NormalizeIssue` a pure function (no I/O)? What does that buy?**  
   *(Key: testable without mocks; separation of I/O (consumer reads Kafka) from logic (function transforms struct))*

---

*End of report. Three findings documented; none block the demo. The system works as specified.*
