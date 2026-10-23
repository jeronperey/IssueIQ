package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	docsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_docs_total", Help: "Documents handled, by outcome (indexed, dlq).",
	}, []string{"result"})
	retries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_retries_total", Help: "Retried OpenSearch documents/requests and DLQ publishes.",
	})
	commitErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_commit_errors_total", Help: "Failed offset commits (batch will be redelivered).",
	})
	bulkSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "indexer_bulk_seconds", Help: "Latency of one _bulk request.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
	batchSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "indexer_batch_seconds", Help: "Time to fully handle a batch (excluding commit).", Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
	batchSizeHist = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "indexer_batch_size", Help: "Messages per batch.", Buckets: prometheus.ExponentialBuckets(1, 2, 11),
	})
	latency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "indexer_ingest_to_index_seconds", Help: "Time from ingestion (RawIssue.ReceivedAt) to the bulk acknowledging the doc. Searchable adds up to the index refresh interval (1s).",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})
	consumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_consumer_lag", Help: "Lag of this instance's reader on normalized-issues.",
	})
)
