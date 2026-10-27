package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "issueiq_query_request_duration_seconds",
	Help:    "Query API request latency by route and status code.",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1 ms .. ~8 s
}, []string{"route", "code"})

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(c int) {
	s.code = c
	s.ResponseWriter.WriteHeader(c)
}

// instrument records latency per route pattern (a fixed label set; never the raw path).
func instrument(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		next(rec, r)
		requestDuration.WithLabelValues(route, strconv.Itoa(rec.code)).Observe(time.Since(start).Seconds())
	}
}
