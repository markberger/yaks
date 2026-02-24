package metrics

import (
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/markberger/yaks/internal/config"
	log "github.com/sirupsen/logrus"
)

func NewClient(cfg config.StatsdConfig) statsd.ClientInterface {
	if !cfg.Enabled {
		return &statsd.NoOpClient{}
	}
	client, err := statsd.New(cfg.Addr(), statsd.WithNamespace("yaks."))
	if err != nil {
		log.WithError(err).Fatal("Failed to create StatsD client")
	}
	return client
}
