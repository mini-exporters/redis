package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
)

// RedisCollector implements prometheus.Collector, collects and parses redis INFO metrics.
type RedisCollector struct {
	client *goredis.Client

	// Server

	// up is a gauge that represents whether or not redis is up.
	up *prometheus.Desc
}

// NewRedisCollector initializes a new redis metric collector.
func NewRedisCollector(client *goredis.Client) *RedisCollector {
	return &RedisCollector{
		client: client,

		up: prometheus.NewDesc("redis_up", "Whether or not redis is up.", nil, nil),
	}
}

// Describe implements prometheus.Collector.
func (rc *RedisCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- rc.up
}

// Collect implements prometheus.Collector.
func (rc *RedisCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	_, err := rc.client.Info(ctx).Result()
	if err != nil {
		ch <- prometheus.MustNewConstMetric(rc.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(rc.up, prometheus.GaugeValue, 1)
}
