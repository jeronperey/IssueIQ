// Package metrics exposes a Prometheus /metrics endpoint for a service.
package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Serve starts /metrics on addr in the background. An empty addr disables it.
func Serve(addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("metrics server on %s: %v", addr, err)
		}
	}()
	log.Printf("metrics on %s/metrics", addr)
}
