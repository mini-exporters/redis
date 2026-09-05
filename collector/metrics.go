package collector

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// metric represents a single Redis metric that can look up its own value
// from parsed INFO fields and emit it as a Prometheus metric.
type metric interface {
	// collect emits this metric's current value(s) onto ch, based on the parsed Redis INFO fields.
	// It should be best-effort — a missing key or an error during collection (e.g. ParseFloat)
	// should simply return.
	collect(ch chan<- prometheus.Metric, fields *info)

	// desc returns the Prometheus descriptor for this metric.
	// It should be used by the Collector to register the metric.
	desc() []*prometheus.Desc
}

// counterMetric implements metric for a prometheus.Counter value.
type counterMetric struct {
	key string
	d   *prometheus.Desc
}

// collect implements metric.
func (m *counterMetric) collect(ch chan<- prometheus.Metric, fields *info) {
	if value, ok := fields.normal[m.key]; ok {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			ch <- prometheus.MustNewConstMetric(m.d, prometheus.CounterValue, f)
		}
	}
}

// desc implements metric.
func (m *counterMetric) desc() []*prometheus.Desc {
	return []*prometheus.Desc{
		m.d,
	}
}

// gaugeMetric implements metric for a prometheus.Gauge value.
type gaugeMetric struct {
	key string
	d   *prometheus.Desc
}

// collect implements metric.
func (m *gaugeMetric) collect(ch chan<- prometheus.Metric, fields *info) {
	if value, ok := fields.normal[m.key]; ok {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			ch <- prometheus.MustNewConstMetric(m.d, prometheus.GaugeValue, f)
		}
	}
}

// desc implements metric.
func (m *gaugeMetric) desc() []*prometheus.Desc {
	return []*prometheus.Desc{
		m.d,
	}
}

// labelGaugeMetric implements metric for a prometheus.Gauge value that records values as labels.
type labelGaugeMetric struct {
	key string
	d   *prometheus.Desc
}

// collect implements metric.
func (m *labelGaugeMetric) collect(ch chan<- prometheus.Metric, fields *info) {
	if value, ok := fields.normal[m.key]; ok {
		ch <- prometheus.MustNewConstMetric(m.d, prometheus.GaugeValue, 1.0, value)
	}
}

// desc implements metric.
func (m *labelGaugeMetric) desc() []*prometheus.Desc {
	return []*prometheus.Desc{
		m.d,
	}
}

// booleanGaugeMetric implements metric for a prometheus.Gauge value that records ok/err values as 1/0.
type booleanGaugeMetric struct {
	key string
	d   *prometheus.Desc
}

// collect implements metric.
func (m *booleanGaugeMetric) collect(ch chan<- prometheus.Metric, fields *info) {
	if value, ok := fields.normal[m.key]; ok {
		if value == "ok" {
			ch <- prometheus.MustNewConstMetric(m.d, prometheus.GaugeValue, 1)
		} else {
			ch <- prometheus.MustNewConstMetric(m.d, prometheus.GaugeValue, 0)
		}
	}
}

// desc implements metric.
func (m *booleanGaugeMetric) desc() []*prometheus.Desc {
	return []*prometheus.Desc{
		m.d,
	}
}

// keyspaceMetric implements metric for Redis INFO Keyspace.
type keyspaceMetric struct {
	keys      *prometheus.Desc
	expires   *prometheus.Desc
	avgTTL    *prometheus.Desc
	subexpiry *prometheus.Desc
}

// collect implements metric.
func (m *keyspaceMetric) collect(ch chan<- prometheus.Metric, fields *info) {
	for _, k := range fields.keyspace {
		ch <- prometheus.MustNewConstMetric(m.keys, prometheus.GaugeValue, k.keys, k.id)
		ch <- prometheus.MustNewConstMetric(m.expires, prometheus.GaugeValue, k.expires, k.id)
		ch <- prometheus.MustNewConstMetric(m.avgTTL, prometheus.GaugeValue, k.avgTTL/1000, k.id) // convert to seconds
		ch <- prometheus.MustNewConstMetric(m.subexpiry, prometheus.GaugeValue, k.subexpiry, k.id)
	}
}

// desc implements metric.
func (m *keyspaceMetric) desc() []*prometheus.Desc {
	return []*prometheus.Desc{m.keys, m.expires, m.avgTTL, m.subexpiry}
}

// errorstatMetric implements metric for Redis INFO Errorstats.
type errorstatMetric struct {
	d *prometheus.Desc
}

// collect implements metric.
func (m *errorstatMetric) collect(ch chan<- prometheus.Metric, fields *info) {
	for _, e := range fields.errorstat {
		ch <- prometheus.MustNewConstMetric(m.d, prometheus.CounterValue, e.value, e.code)
	}
}

// desc implements metric.
func (m *errorstatMetric) desc() []*prometheus.Desc {
	return []*prometheus.Desc{
		m.d,
	}
}
