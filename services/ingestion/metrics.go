package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ingestion_events_total", Help: "POST /ingest outcomes: accepted (202), rejected (400), publish_error (500).",
}, []string{"result"})
