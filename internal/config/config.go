package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
)

type Config struct {
	BrokerHost string `env:"YAKS_BROKER_HOST" envDefault:"0.0.0.0"`
	BrokerPort int32  `env:"YAKS_BROKER_PORT" envDefault:"9092"`

	FlushIntervalMs int `env:"YAKS_FLUSH_INTERVAL_MS" envDefault:"250"`
	FlushBytes      int `env:"YAKS_FLUSH_BYTES"        envDefault:"5242880"`

	DB metastore.Config         `envPrefix:""`
	S3 s3_client.S3ClientConfig `envPrefix:""`
}

func Load() (Config, error) {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
