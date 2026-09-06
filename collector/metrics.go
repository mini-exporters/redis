package collector

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// metric represents a single Redis metric that can look up its own value
// from parsed INFO fields and emit it as a Prometheus metric.
type metric interface {
	// collect emits this metric's current value(s) onto ch, based on the parsed Redis INFO fields.
	// A missing fieldi s not an error, absence is silently ignored.
	// A present, but unparsable value is returned as an error, so
	// it can be surfaced via redis_last_scrape_error.
	collect(ch chan<- prometheus.Metric, fields *info) error

	// desc returns the Prometheus descriptor(s) for this metric.
	// It should be used by the Collector to register the metric.
	desc() []*prometheus.Desc
}

// counterMetric implements metric for a prometheus.Counter value.
type counterMetric struct {
	key string
	d   *prometheus.Desc
}

// collect implements metric.
func (m *counterMetric) collect(ch chan<- prometheus.Metric, fields *info) error {
	value, ok := fields.normal[m.key]
	if !ok {
		return nil
	}

	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("%s: parsing %q: %w", m.key, value, err)
	}

	ch <- prometheus.MustNewConstMetric(m.d, prometheus.CounterValue, f)
	return nil
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
func (m *gaugeMetric) collect(ch chan<- prometheus.Metric, fields *info) error {
	value, ok := fields.normal[m.key]
	if !ok {
		return nil
	}

	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("%s: parsing %q: %w", m.key, value, err)
	}

	ch <- prometheus.MustNewConstMetric(m.d, prometheus.GaugeValue, f)
	return nil
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
func (m *labelGaugeMetric) collect(ch chan<- prometheus.Metric, fields *info) error {
	value, ok := fields.normal[m.key]
	if !ok {
		return nil
	}

	ch <- prometheus.MustNewConstMetric(m.d, prometheus.GaugeValue, 1.0, value)
	return nil
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
func (m *booleanGaugeMetric) collect(ch chan<- prometheus.Metric, fields *info) error {
	value, ok := fields.normal[m.key]
	if !ok {
		return nil
	}

	if value == "ok" {
		ch <- prometheus.MustNewConstMetric(m.d, prometheus.GaugeValue, 1)
	} else {
		ch <- prometheus.MustNewConstMetric(m.d, prometheus.GaugeValue, 0)
	}

	return nil
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
func (m *keyspaceMetric) collect(ch chan<- prometheus.Metric, fields *info) error {
	var errs []error
	for _, k := range fields.keyspace {
		if err := m.emitKeyspaceGauge(ch, m.keys, k.keys, 1, k.id); err != nil {
			errs = append(errs, fmt.Errorf("db%s keys: %w", k.id, err))
		}

		if err := m.emitKeyspaceGauge(ch, m.expires, k.expires, 1, k.id); err != nil {
			errs = append(errs, fmt.Errorf("db%s expires: %w", k.id, err))
		}

		if err := m.emitKeyspaceGauge(ch, m.avgTTL, k.avgTTL, 1000, k.id); err != nil {
			errs = append(errs, fmt.Errorf("db%s avg_ttl: %w", k.id, err))
		}

		if err := m.emitKeyspaceGauge(ch, m.subexpiry, k.subexpiry, 1, k.id); err != nil {
			errs = append(errs, fmt.Errorf("db%s subexpiry: %w", k.id, err))
		}
	}

	return errors.Join(errs...)
}

// emitKeyspaceGauge is a helper that parses the provided raw value, converts it to an appropriate format
// and emits the metric into ch.
// Returns an error on invalid float value.
func (m *keyspaceMetric) emitKeyspaceGauge(ch chan<- prometheus.Metric, d *prometheus.Desc, raw *string, divisor float64, label string) error {
	if raw == nil {
		return nil
	}

	f, err := strconv.ParseFloat(*raw, 64)
	if err != nil {
		return fmt.Errorf("parsing %q: %w", *raw, err)
	}

	if divisor != 0 {
		f /= divisor
	}

	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, f, label)
	return nil
}

// desc implements metric.
func (m *keyspaceMetric) desc() []*prometheus.Desc {
	return []*prometheus.Desc{
		m.keys,
		m.expires,
		m.avgTTL,
		m.subexpiry,
	}
}

// errorstatMetric implements metric for Redis INFO Errorstats.
type errorstatMetric struct {
	d *prometheus.Desc
}

// collect implements metric.
func (m *errorstatMetric) collect(ch chan<- prometheus.Metric, fields *info) error {
	var errs []error
	for _, e := range fields.errorstat {
		f, err := strconv.ParseFloat(e.value, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("errorstat_%s: parsing %q: %w", e.code, e.value, err))
			continue
		}
		ch <- prometheus.MustNewConstMetric(m.d, prometheus.CounterValue, f, e.code)
	}

	return errors.Join(errs...)
}

// desc implements metric.
func (m *errorstatMetric) desc() []*prometheus.Desc {
	return []*prometheus.Desc{
		m.d,
	}
}
