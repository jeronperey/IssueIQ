package main

import "github.com/prometheus/client_golang/prometheus"
import "github.com/prometheus/client_golang/prometheus/promauto"

var (
	messagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "normalizer_messages_total", Help: "Messages processed, by outcome (ok = normalized, dlq = dead-lettered).",
	}, []string{"result"})
	publishRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "normalizer_publish_retries_total", Help: "Retried Kafka batch publishes.",
	})
	commitErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "normalizer_commit_errors_total", Help: "Failed offset commits (batch will be redelivered).",
	})
	batchSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "normalizer_batch_seconds", Help: "Time to normalize, publish and (not including commit) a batch.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
	batchSizeHist = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "normalizer_batch_size", Help: "Messages per batch.", Buckets: prometheus.ExponentialBuckets(1, 2, 11),
	})
	consumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "normalizer_consumer_lag", Help: "Lag of this instance's reader on raw-issues.",
	})
)
