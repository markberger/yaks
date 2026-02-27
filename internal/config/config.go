package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
)

type OTelConfig struct {
	Enabled     bool   `env:"YAKS_OTEL_ENABLED"      envDefault:"false"`
	MetricsPort int32  `env:"YAKS_METRICS_PORT"       envDefault:"9090"`
}

type GroupcacheConfig struct {
	Enabled           bool   `env:"YAKS_GROUPCACHE_ENABLED"         envDefault:"false"`
	Port              int32  `env:"YAKS_GROUPCACHE_PORT"            envDefault:"9080"`
	AdvertisedHost    string `env:"YAKS_GROUPCACHE_ADVERTISED_HOST" envDefault:""`
	CacheSizeMB       int64  `env:"YAKS_GROUPCACHE_CACHE_SIZE_MB"   envDefault:"512"`
	HeartbeatInterval int    `env:"YAKS_GROUPCACHE_HEARTBEAT_MS"    envDefault:"5000"`
	PollInterval      int    `env:"YAKS_GROUPCACHE_POLL_MS"         envDefault:"10000"`
	LeaseTTL          int    `env:"YAKS_GROUPCACHE_LEASE_TTL_MS"    envDefault:"15000"`
}

type Config struct {
	NodeID         int32  `env:"YAKS_NODE_ID"          envDefault:"0"`
	BrokerHost     string `env:"YAKS_BROKER_HOST"      envDefault:"0.0.0.0"`
	BrokerPort     int32  `env:"YAKS_BROKER_PORT"      envDefault:"9092"`
	AdvertisedHost string `env:"YAKS_ADVERTISED_HOST"  envDefault:"localhost"`
	AdvertisedPort int32  `env:"YAKS_ADVERTISED_PORT"  envDefault:"0"`
	RunMigrations  bool   `env:"YAKS_RUN_MIGRATIONS"   envDefault:"false"`

	FlushIntervalMs int `env:"YAKS_FLUSH_INTERVAL_MS" envDefault:"250"`
	FlushBytes      int `env:"YAKS_FLUSH_BYTES"        envDefault:"5242880"`

	MaterializeIntervalMs int   `env:"YAKS_MATERIALIZE_INTERVAL_MS" envDefault:"100"`
	MaterializeBatchSize  int32 `env:"YAKS_MATERIALIZE_BATCH_SIZE"  envDefault:"1000"`

	FetchMaxBytes int32 `env:"YAKS_FETCH_MAX_BYTES" envDefault:"52428800"`

	DB         metastore.Config         `envPrefix:""`
	S3         s3_client.S3ClientConfig `envPrefix:""`
	OTel       OTelConfig               `envPrefix:""`
	Groupcache GroupcacheConfig          `envPrefix:""`
}

// GetAdvertisedPort returns AdvertisedPort if set, otherwise BrokerPort.
func (c Config) GetAdvertisedPort() int32 {
	if c.AdvertisedPort != 0 {
		return c.AdvertisedPort
	}
	return c.BrokerPort
}

func Load() (Config, error) {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
