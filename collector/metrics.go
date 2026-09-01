package collector

import (
	"github.com/prometheus/client_golang/prometheus"
)

// metric represents a single Redis metric that can look up its own value
// from parsed INFO fields and emit it as a Prometheus metric.
type metric interface {
	// collect emits this metric's current value(s) onto ch, based on the parsed Redis INFO fields.
	// It should be best-effort — a missing key or an error during collection (e.g. ParseFloat)
	// should simply return.
	collect(ch chan<- prometheus.Metric, fields map[string]string)

	// desc returns the Prometheus descriptor for this metric.
	// It should be used by the Collector to register the metric.
	desc() *prometheus.Desc
}
