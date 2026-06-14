#!/usr/bin/env bash
# payloads.sh — mock JSON bodies for the IssueIQ Lite test suite.
# Source this file: `source scripts/payloads.sh`
# Every payload is a shell variable holding a single-line JSON string.

# ---- Valid / happy path ----
PAYLOAD_VALID='{"source_id":"gh-101","source":"github","raw_payload":{"title":"fix login bug","description":"users cannot log in","status":"OPEN","priority":"P1","tags":["auth","backend"]}}'

# ---- Validation failures (expect 400) ----
PAYLOAD_MISSING_SOURCEID='{"source":"github","raw_payload":{"title":"no id here"}}'
PAYLOAD_BAD_SOURCE='{"source_id":"x-1","source":"slack","raw_payload":{"title":"bad source"}}'
PAYLOAD_NIL_PAYLOAD='{"source_id":"x-2","source":"github"}'
PAYLOAD_MALFORMED_JSON='{"source_id":"x-3","source":"github","raw_payload":{"title":"truncated"'  # intentionally broken

# ---- Status variants — all must normalize to "open" ----
PAYLOAD_STATUS_UPPER='{"source_id":"st-1","source":"github","raw_payload":{"title":"status upper","status":"OPEN","priority":"P2"}}'
PAYLOAD_STATUS_TITLE='{"source_id":"st-2","source":"github","raw_payload":{"title":"status title","status":"Open","priority":"P2"}}'
PAYLOAD_STATUS_LOWER='{"source_id":"st-3","source":"github","raw_payload":{"title":"status lower","status":"open","priority":"P2"}}'
PAYLOAD_STATUS_UNKNOWN='{"source_id":"st-4","source":"github","raw_payload":{"title":"status unknown","status":"frobnicated","priority":"P2"}}'  # -> default "open"

# ---- Priority variants — all must normalize to "critical" ----
PAYLOAD_PRIO_P0='{"source_id":"pr-1","source":"github","raw_payload":{"title":"prio p0","status":"open","priority":"P0"}}'
PAYLOAD_PRIO_URGENT='{"source_id":"pr-2","source":"github","raw_payload":{"title":"prio urgent","status":"open","priority":"urgent"}}'
PAYLOAD_PRIO_BLOCKER='{"source_id":"pr-3","source":"github","raw_payload":{"title":"prio blocker","status":"open","priority":"blocker"}}'
PAYLOAD_PRIO_UNKNOWN='{"source_id":"pr-4","source":"github","raw_payload":{"title":"prio unknown","status":"open","priority":"wat"}}'  # -> default "medium"

# ---- Tags normalization: ["Go","  go  ","GO"] -> ["go"] ----
PAYLOAD_TAGS='{"source_id":"tg-1","source":"github","raw_payload":{"title":"tag dedupe","status":"open","priority":"low","tags":["Go","  go  ","GO","backend","Backend"]}}'

# ---- DLQ trigger: empty payload, no title -> dead-letter ----
PAYLOAD_DLQ='{"source_id":"dlq-1","source":"github","raw_payload":{}}'

# ---- Idempotency: same source+source_id sent twice -> one doc ----
PAYLOAD_IDEMPOTENT='{"source_id":"idem-1","source":"github","raw_payload":{"title":"idempotent issue","status":"open","priority":"high"}}'

# ---- Canary: distinctive end-to-end trace ----
PAYLOAD_CANARY='{"source_id":"canary-1","source":"github","raw_payload":{"title":"trace-canary-XYZ","description":"end to end canary","status":"OPEN","priority":"P0","tags":["canary"]}}'

# ---- Unicode / emoji survival ----
PAYLOAD_UNICODE='{"source_id":"uni-1","source":"github","raw_payload":{"title":"日本語 login 🐛 bug","status":"open","priority":"high","tags":["i18n"]}}'

# ---- Batch: 3 valid + 2 invalid (expect queued:3 failed:2, or repo shape) ----
PAYLOAD_BATCH='[{"source_id":"b-1","source":"github","raw_payload":{"title":"batch one","status":"open","priority":"P1"}},{"source_id":"b-2","source":"zendesk","raw_payload":{"title":"batch two","status":"closed","priority":"P3"}},{"source_id":"b-3","source":"mock","raw_payload":{"title":"batch three","status":"open","priority":"urgent"}},{"source":"github","raw_payload":{"title":"batch no id"}},{"source_id":"b-5","source":"slack","raw_payload":{"title":"batch bad source"}}]'

# Helper: build a ~1MB payload at call time (don't store it inline)
make_large_payload() {
  local big
  big=$(head -c 1000000 < /dev/zero | tr '\0' 'a')
  printf '{"source_id":"big-1","source":"github","raw_payload":{"title":"large payload","description":"%s","status":"open","priority":"low"}}' "$big"
}
