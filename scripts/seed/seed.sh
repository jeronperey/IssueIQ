#!/usr/bin/env bash
set -euo pipefail

INGESTION_URL="${INGESTION_URL:-http://localhost:8080}"

echo "Seeding 5 mock issues to ${INGESTION_URL}/ingest/batch ..."

curl -sf -X POST "${INGESTION_URL}/ingest/batch" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "source_id": "gh-1",
      "source": "github",
      "raw_payload": {
        "title": "login redirect loop after OAuth",
        "description": "Users get stuck in infinite redirect after OAuth callback.",
        "status": "OPEN",
        "priority": "P1",
        "tags": ["auth", "bug"]
      }
    },
    {
      "source_id": "gh-2",
      "source": "github",
      "raw_payload": {
        "title": "nil pointer in user service",
        "description": "Panic on nil pointer when user profile is incomplete.",
        "status": "IN_PROGRESS",
        "priority": "P0",
        "tags": ["backend", "bug"]
      }
    },
    {
      "source_id": "zd-101",
      "source": "zendesk",
      "raw_payload": {
        "title": "dashboard loads slowly for enterprise accounts",
        "description": "Enterprise users report 10+ second load times on main dashboard.",
        "status": "open",
        "priority": "high",
        "tags": ["performance", "ui"]
      }
    },
    {
      "source_id": "gh-3",
      "source": "github",
      "raw_payload": {
        "title": "session token not invalidated on logout",
        "description": "Old session tokens remain valid after explicit logout.",
        "status": "open",
        "priority": "P0",
        "tags": ["auth", "security"]
      }
    },
    {
      "source_id": "zd-102",
      "source": "zendesk",
      "raw_payload": {
        "title": "cannot export CSV report",
        "description": "Export button on the reports page silently fails.",
        "status": "new",
        "priority": "medium",
        "tags": ["billing"]
      }
    }
  ]' | python3 -m json.tool

echo ""
echo "Verify indexing:"
echo "  curl 'http://localhost:9200/issues/_count' | python3 -m json.tool"
echo "  curl 'http://localhost:9200/issues/_search?pretty&size=5'"
