package collector

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
)

// RedisCollector implements prometheus.Collector, collects and parses Redis INFO metrics.
type RedisCollector struct {
	client *goredis.Client

	// up is a gauge that represents whether or not Redis is up.
	// It is separated from other metrics, because it is derived based on Redis connectivity.
	up *prometheus.Desc

	metrics []metric
}

// NewRedisCollector initializes a new redis metric collector.
func NewRedisCollector(client *goredis.Client) *RedisCollector {
	// Server metrics are derived from the Server section of Redis INFO
	serverMetrics := []metric{
		&gaugeMetric{
			key: "uptime_in_seconds",
			d:   prometheus.NewDesc("redis_server_uptime_seconds", "Redis uptime in seconds (uptime_in_seconds).", nil, nil),
		},
	}

	// Clients metrics are derived from the Clients section of Redis INFO
	clientsMetrics := []metric{
		&gaugeMetric{
			key: "connected_clients",
			d:   prometheus.NewDesc("redis_clients_connected", "Number of active clients (connected_clients).", nil, nil),
		},
		&gaugeMetric{
			key: "blocked_clients",
			d:   prometheus.NewDesc("redis_clients_blocked", "Number of awaiting clients (blocked_clients).", nil, nil),
		},
		&gaugeMetric{
			key: "maxclients",
			d:   prometheus.NewDesc("redis_clients_max", "Maximum number of available clients (maxclients).", nil, nil),
		},
		&gaugeMetric{
			key: "total_blocking_keys",
			d:   prometheus.NewDesc("redis_clients_total_blocking_keys", "Number of blocking keys (total_blocking_keys).", nil, nil),
		},
	}

	// Memory metrics are derived from the Memory section of Redis INFO
	memoryMetrics := []metric{
		&gaugeMetric{
			key: "used_memory",
			d:   prometheus.NewDesc("redis_memory_used_bytes", "Memory used in bytes (used_memory).", nil, nil),
		},
		&gaugeMetric{
			key: "maxmemory",
			d:   prometheus.NewDesc("redis_memory_max_bytes", "Maximum memory in bytes (maxmemory).", nil, nil),
		},
		&gaugeMetric{
			key: "mem_fragmentation_ratio",
			d:   prometheus.NewDesc("redis_memory_fragmentation_ratio", "Memory Fragmentation ratio (mem_fragmentation_ratio).", nil, nil),
		},
		&gaugeMetric{
			key: "used_memory_rss",
			d:   prometheus.NewDesc("redis_memory_used_rss_bytes", "Memory used reported by the OS in bytes (used_memory_rss).", nil, nil),
		},
		&gaugeMetric{
			key: "used_memory_peak",
			d:   prometheus.NewDesc("redis_memory_used_peak_bytes", "Memory peak in bytes (used_memory_peak).", nil, nil),
		},
		// Deprecated in Redis 7.0
		&gaugeMetric{
			key: "used_memory_lua",
			d:   prometheus.NewDesc("redis_memory_used_lua_bytes", "Memory used by the LUA engine in bytes (used_memory_lua).", nil, nil),
		},
		// Replaced used_memory_lua
		&gaugeMetric{
			key: "used_memory_vm_eval",
			d:   prometheus.NewDesc("redis_memory_used_vm_bytes", "Memory used by the VM engines in bytes (used_memory_vm_eval).", nil, nil),
		},
	}

	metrics := slices.Concat(
		serverMetrics,
		clientsMetrics,
		memoryMetrics,
	)
	return &RedisCollector{
		client:  client,
		up:      prometheus.NewDesc("redis_up", "Whether or not Redis is up.", nil, nil),
		metrics: metrics,
	}
}

// Describe implements prometheus.Collector.
func (rc *RedisCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- rc.up

	for _, m := range rc.metrics {
		ch <- m.desc()
	}
}

// Collect implements prometheus.Collector.
func (rc *RedisCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	info, err := rc.client.Info(ctx).Result()
	if err != nil {
		ch <- prometheus.MustNewConstMetric(rc.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(rc.up, prometheus.GaugeValue, 1)

	fields := rc.parseInfo(info)
	for _, m := range rc.metrics {
		m.collect(ch, fields)
	}
}

// parseInfo parses redis INFO output into a map.
// Redis returns INFO as a series of key:value pairs separated by \r\n.
// Comment/Section lines start with a # character.
func (rc *RedisCollector) parseInfo(info string) map[string]string {
	out := make(map[string]string)
	for line := range strings.SplitSeq(info, "\r\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, valid := strings.Cut(line, ":")
		if !valid {
			continue
		}
		out[key] = value
	}

	return out
}
