package collector

import (
	"context"
	"slices"
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
	prefix := "redis_server_"
	serverMetrics := []metric{
		&gaugeMetric{
			key: "uptime_in_seconds",
			d:   prometheus.NewDesc(prefix+"uptime_seconds", "Redis uptime in seconds (uptime_in_seconds).", nil, nil),
		},
	}

	// Clients metrics are derived from the Clients section of Redis INFO
	prefix = "redis_clients_"
	clientsMetrics := []metric{
		&gaugeMetric{
			key: "connected_clients",
			d:   prometheus.NewDesc(prefix+"connected", "Number of active clients (connected_clients).", nil, nil),
		},
		&gaugeMetric{
			key: "blocked_clients",
			d:   prometheus.NewDesc(prefix+"blocked", "Number of awaiting clients (blocked_clients).", nil, nil),
		},
		&gaugeMetric{
			key: "maxclients",
			d:   prometheus.NewDesc(prefix+"max", "Maximum number of available clients (maxclients).", nil, nil),
		},
		&gaugeMetric{
			key: "total_blocking_keys",
			d:   prometheus.NewDesc(prefix+"total_blocking_keys", "Number of blocking keys (total_blocking_keys).", nil, nil),
		},
	}

	// Memory metrics are derived from the Memory section of Redis INFO
	prefix = "redis_memory_"
	memoryMetrics := []metric{
		&gaugeMetric{
			key: "used_memory",
			d:   prometheus.NewDesc(prefix+"used_bytes", "Memory used in bytes (used_memory).", nil, nil),
		},
		&gaugeMetric{
			key: "maxmemory",
			d:   prometheus.NewDesc(prefix+"max_bytes", "Maximum memory in bytes (maxmemory).", nil, nil),
		},
		&gaugeMetric{
			key: "mem_fragmentation_ratio",
			d:   prometheus.NewDesc(prefix+"fragmentation_ratio", "Memory Fragmentation ratio (mem_fragmentation_ratio).", nil, nil),
		},
		&gaugeMetric{
			key: "used_memory_rss",
			d:   prometheus.NewDesc(prefix+"used_rss_bytes", "Memory used reported by the OS in bytes (used_memory_rss).", nil, nil),
		},
		&gaugeMetric{
			key: "used_memory_peak",
			d:   prometheus.NewDesc(prefix+"used_peak_bytes", "Memory peak in bytes (used_memory_peak).", nil, nil),
		},
		// Deprecated in Redis 7.0
		&gaugeMetric{
			key: "used_memory_lua",
			d:   prometheus.NewDesc(prefix+"used_lua_bytes", "Memory used by the LUA engine in bytes (used_memory_lua).", nil, nil),
		},
		// Replaced used_memory_lua
		&gaugeMetric{
			key: "used_memory_vm_eval",
			d:   prometheus.NewDesc(prefix+"used_vm_bytes", "Memory used by the VM engines in bytes (used_memory_vm_eval).", nil, nil),
		},
	}

	// Persistence metrics are derived from the Persistence section of Redis INFO
	prefix = "redis_persistence_"
	persistenceMetrics := []metric{
		&booleanGaugeMetric{
			key: "rdb_last_bgsave_status",
			d:   prometheus.NewDesc(prefix+"rdb_last_bgsave_status", "Last BGSAVE status (rdb_last_bgsave_status).", nil, nil),
		},
		&booleanGaugeMetric{
			key: "aof_last_write_status",
			d:   prometheus.NewDesc(prefix+"aof_last_write_status", "Last AOF write status (aof_last_write_status).", nil, nil),
		},
		&gaugeMetric{
			key: "rdb_changes_since_last_save",
			d:   prometheus.NewDesc(prefix+"rdb_changes_since_last_save", "Number of operations since the last RDB save (rdb_changes_since_last_save).", nil, nil),
		},
		&gaugeMetric{
			key: "rdb_last_save_time",
			d:   prometheus.NewDesc(prefix+"rdb_last_save_timestamp_seconds", "Timestamp of the last RDB save (rdb_last_save_time).", nil, nil),
		},
		&gaugeMetric{
			key: "aof_enabled",
			d:   prometheus.NewDesc(prefix+"aof_enabled", "Is AOF enabled (aof_enabled).", nil, nil),
		},
		&gaugeMetric{
			key: "aof_current_size",
			d:   prometheus.NewDesc(prefix+"aof_current_size_bytes", "AOF current file size in bytes (aof_current_size).", nil, nil),
		},
		&gaugeMetric{
			key: "aof_base_size",
			d:   prometheus.NewDesc(prefix+"aof_base_size_bytes", "AOF file size on latest startup or rewrite in bytes (aof_base_size).", nil, nil),
		},
		&counterMetric{
			key: "rdb_saves",
			d:   prometheus.NewDesc(prefix+"rdb_saves_total", "Total number of RDB snapshots performed since startup (rdb_saves).", nil, nil),
		},
		&counterMetric{
			key: "aof_rewrites",
			d:   prometheus.NewDesc(prefix+"aof_rewrites_total", "Total number of AOF rewrites performed since startup (aof_rewrites).", nil, nil),
		},
	}

	// Stats metrics are derived from the Stats and Errorstats sections of Redis INFO
	prefix = "redis_stats_"
	statsMetrics := []metric{
		&counterMetric{
			key: "keyspace_hits",
			d:   prometheus.NewDesc(prefix+"keyspace_hits_total", "Total number of Keyspace hits (keyspace_hits).", nil, nil),
		},
		&counterMetric{
			key: "keyspace_misses",
			d:   prometheus.NewDesc(prefix+"keyspace_misses_total", "Total number of Keyspace misses (keyspace_misses).", nil, nil),
		},
		&counterMetric{
			key: "expired_keys",
			d:   prometheus.NewDesc(prefix+"expired_keys_total", "Total number of expired keys (expired_keys).", nil, nil),
		},
		&counterMetric{
			key: "evicted_keys",
			d:   prometheus.NewDesc(prefix+"evicted_keys_total", "Total number of evicted keys (evicted_keys).", nil, nil),
		},
		&counterMetric{
			key: "total_connections_received",
			d:   prometheus.NewDesc(prefix+"connections_received_total", "Total number of connections received (total_connections_received).", nil, nil),
		},
		&counterMetric{
			key: "rejected_connections",
			d:   prometheus.NewDesc(prefix+"rejected_connections_total", "Total number of rejected connections (rejected_connections).", nil, nil),
		},
		&counterMetric{
			key: "total_error_replies",
			d:   prometheus.NewDesc(prefix+"error_replies_total", "Total number of error replies (total_error_replies).", nil, nil),
		},
		&counterMetric{
			key: "total_commands_processed",
			d:   prometheus.NewDesc(prefix+"commands_processed_total", "Total number of commands processed (total_commands_processed).", nil, nil),
		},
		&gaugeMetric{
			key: "instantaneous_ops_per_sec",
			d:   prometheus.NewDesc(prefix+"instantaneous_ops_per_sec", "Number of commands processed per second (instantaneous_ops_per_sec).", nil, nil),
		},
		&counterMetric{
			key: "total_net_input_bytes",
			d:   prometheus.NewDesc(prefix+"net_input_bytes_total", "Total number of bytes read from the network (total_net_input_bytes).", nil, nil),
		},
		&counterMetric{
			key: "total_net_output_bytes",
			d:   prometheus.NewDesc(prefix+"net_output_bytes_total", "Total number of bytes written to the network (total_net_output_bytes).", nil, nil),
		},
		&errorstatMetric{
			d: prometheus.NewDesc(prefix+"errors_total", "Total number of errors (errorstat_<CODE>).", []string{"code"}, nil),
		},
	}

	// Replication metrics are derived from the Replication section of Redis INFO
	prefix = "redis_replication_"
	replicationMetrics := []metric{
		&labelGaugeMetric{
			key: "role",
			d:   prometheus.NewDesc(prefix+"role", "Role of the instance (role).", []string{"role"}, nil),
		},
		&gaugeMetric{
			key: "connected_slaves",
			d:   prometheus.NewDesc(prefix+"connected_slaves", "Number of connected replicas (connected_slaves).", nil, nil),
		},
		&gaugeMetric{
			key: "master_repl_offset",
			d:   prometheus.NewDesc(prefix+"master_repl_offset", "Current replication offset (master_repl_offset).", nil, nil),
		},
		&gaugeMetric{
			key: "repl_backlog_active",
			d:   prometheus.NewDesc(prefix+"backlog_active", "Whether or not replication backlog is active (repl_backlog_active).", nil, nil),
		},
	}

	// Keyspace metrics are derived from the Keyspace section of Redis INFO
	prefix = "redis_db_"
	keyspaceMetrics := []metric{
		&keyspaceMetric{
			keys:      prometheus.NewDesc(prefix+"keys", "Number of keys (db<N>:keys).", []string{"db"}, nil),
			expires:   prometheus.NewDesc(prefix+"expires", "Number of keys with expiry (db<N>:expires).", []string{"db"}, nil),
			avgTTL:    prometheus.NewDesc(prefix+"avg_ttl_seconds", "Average expiry TTL (db<N>:avg_ttl).", []string{"db"}, nil),
			subexpiry: prometheus.NewDesc(prefix+"subexpiry", "Number of keys that have a sub-millisecond expiration (db<N>:subexpiry).", []string{"db"}, nil),
		},
	}

	metrics := slices.Concat(
		serverMetrics,
		clientsMetrics,
		memoryMetrics,
		persistenceMetrics,
		statsMetrics,
		keyspaceMetrics,
		replicationMetrics,
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
		for _, d := range m.desc() {
			ch <- d
		}
	}
}

// Collect implements prometheus.Collector.
func (rc *RedisCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	result, err := rc.client.Info(ctx, "all").Result()
	if err != nil {
		ch <- prometheus.MustNewConstMetric(rc.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(rc.up, prometheus.GaugeValue, 1)

	fields := parseInfo(result)
	for _, m := range rc.metrics {
		m.collect(ch, fields)
	}
}
