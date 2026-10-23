#!/usr/bin/env bash
# Local bench harness: Kafka + OpenSearch in Docker, pipeline services as host processes
# so they can be scaled and killed freely.
#   scripts/bench/run.sh <partitions> <normalizers> <indexers> <loadgen args...>
set -euo pipefail
cd "$(dirname "$0")/../.."
P=$1; N=$2; I=$3; shift 3
BROKER=localhost:9092
mkdir -p bin && for s in ingestion normalizer indexer; do go build -o bin/$s ./services/$s; done

topics() { # recreate topics with $P partitions (consumer groups are unique per run)
  for t in raw-issues normalized-issues; do
    docker exec issueiq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t >/dev/null 2>&1 || true
  done
  for t in raw-issues normalized-issues; do
    for try in $(seq 1 30); do
      docker exec issueiq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $t --partitions $P --replication-factor 1 >/dev/null 2>&1 && break
      sleep 1
    done
  done
}
topics
GRP=bench$(date +%s)
pids=()
for i in $(seq 1 $N); do KAFKA_BROKERS=$BROKER CONSUMER_GROUP=n-$GRP METRICS_ADDR=:$((9100+i)) bin/normalizer >/tmp/bench-norm$i.log 2>&1 & pids+=($!); done
for i in $(seq 1 $I); do KAFKA_BROKERS=$BROKER CONSUMER_GROUP=i-$GRP METRICS_ADDR=:$((9200+i)) bin/indexer >/tmp/bench-idx$i.log 2>&1 & pids+=($!); done
trap 'kill ${pids[@]} 2>/dev/null || true' EXIT
sleep 12   # consumer group join + rebalance
go run ./tools/loadgen -partitions $P -label "p$P-n$N-i$I" "$@"
