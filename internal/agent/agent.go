package agent

import (
	"context"

	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/config"
	"github.com/markberger/yaks/internal/handlers"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	"gorm.io/gorm"
)

type Agent struct {
	db        *gorm.DB
	Metastore metastore.Metastore
	broker    *broker.Broker
	s3Client  s3_client.S3Client
	bucket    string
}

func NewAgent(db *gorm.DB, cfg config.Config) *Agent {
	metastore := metastore.NewGormMetastore(db)
	broker := broker.NewBroker(0, cfg.BrokerHost, cfg.BrokerPort)
	s3Client := s3_client.CreateS3Client(cfg.S3)
	return &Agent{db, metastore, broker, s3Client, cfg.S3.Bucket}
}

// TODO: agent should not apply migrations it should be done by a separate
// cmd tool before deployment
func (a *Agent) ApplyMigrations() error {
	return a.Metastore.ApplyMigrations()
}

func (a *Agent) AddHandlers() {
	a.broker.Add(handlers.NewMetadataRequestHandler(a.broker, a.Metastore))
	a.broker.Add(handlers.NewCreateTopicsRequestHandler(a.Metastore))
	a.broker.Add(handlers.NewProduceRequestHandlerWithS3(a.Metastore, a.bucket, a.s3Client))
	a.broker.Add(handlers.NewFetchRequestHandler(a.Metastore, a.s3Client, a.bucket))
}

func (a *Agent) ListenAndServe(ctx context.Context) {
	a.broker.ListenAndServe(ctx)
}
