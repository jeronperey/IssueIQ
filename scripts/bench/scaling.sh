#!/usr/bin/env bash
# Saturation throughput as partitions and consumers scale together (P = normalizers = indexers).
cd "$(dirname "$0")/../.."
OUT=${OUT:-docs/results/scaling.jsonl}
for rep in $(seq 1 ${REPS:-3}); do
  for n in 1 2 4 8; do
    scripts/bench/run.sh $n $n $n -mode kafka -count ${COUNT:-1000000} -workers 256 -run-id sc${n}x$(date +%s) -label "scaling-$n" -out $OUT 2>&1 | grep -E "produced|steady_state|overall|missing|redelivered"
  done
done
