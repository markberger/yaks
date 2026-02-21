package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
)

type Config struct {
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

	DB metastore.Config         `envPrefix:""`
	S3 s3_client.S3ClientConfig `envPrefix:""`
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
