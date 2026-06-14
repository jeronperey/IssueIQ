#!/usr/bin/env python3
"""compute_id.py — independently reproduce IssueIQ's stable document ID.

The normalizer computes: ID = sha256(source + ":" + source_id), hex-encoded.
This script recomputes it WITHOUT touching the Go code, so the test can assert
that the Go implementation matches an independent reference. If these ever
disagree, the normalizer's ID logic has drifted.

Usage:
    python3 scripts/compute_id.py <source> <source_id>
    python3 scripts/compute_id.py github canary-1
"""
import hashlib
import sys


def compute_id(source: str, source_id: str) -> str:
    return hashlib.sha256(f"{source}:{source_id}".encode("utf-8")).hexdigest()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: compute_id.py <source> <source_id>", file=sys.stderr)
        sys.exit(2)
    print(compute_id(sys.argv[1], sys.argv[2]))
