package agent

import (
	"context"
	"time"

	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/buffer"
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
	buffer    *buffer.WriteBuffer
}

func NewAgent(db *gorm.DB, cfg config.Config) *Agent {
	metastore := metastore.NewGormMetastore(db)
	broker := broker.NewBroker(0, cfg.BrokerHost, cfg.BrokerPort)
	s3Client := s3_client.CreateS3Client(cfg.S3)
	buf := buffer.NewWriteBuffer(
		s3Client,
		metastore,
		cfg.S3.Bucket,
		time.Duration(cfg.FlushIntervalMs)*time.Millisecond,
		cfg.FlushBytes,
	)
	return &Agent{db, metastore, broker, s3Client, cfg.S3.Bucket, buf}
}

// TODO: agent should not apply migrations it should be done by a separate
// cmd tool before deployment
func (a *Agent) ApplyMigrations() error {
	return a.Metastore.ApplyMigrations()
}

func (a *Agent) AddHandlers() {
	a.broker.Add(handlers.NewMetadataRequestHandler(a.broker, a.Metastore))
	a.broker.Add(handlers.NewCreateTopicsRequestHandler(a.Metastore))
	a.broker.Add(handlers.NewProduceRequestHandler(a.Metastore, a.buffer))
	a.broker.Add(handlers.NewFetchRequestHandler(a.Metastore, a.s3Client, a.bucket))
	a.broker.Add(handlers.NewFindCoordinatorRequestHandler(a.broker))
}

func (a *Agent) ListenAndServe(ctx context.Context) {
	go a.buffer.Start(ctx)
	a.broker.ListenAndServe(ctx)
}
