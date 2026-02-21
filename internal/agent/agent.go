package agent

import (
	"context"
	"time"

	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/buffer"
	"github.com/markberger/yaks/internal/config"
	"github.com/markberger/yaks/internal/handlers"
	"github.com/markberger/yaks/internal/materializer"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	"gorm.io/gorm"
)

type Agent struct {
	db            *gorm.DB
	Metastore     metastore.Metastore
	broker        *broker.Broker
	s3Client      s3_client.S3Client
	bucket        string
	fetchMaxBytes int32
	buffer        *buffer.WriteBuffer
	materializer  *materializer.Materializer
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
	mat := materializer.NewMaterializer(
		metastore,
		time.Duration(cfg.MaterializeIntervalMs)*time.Millisecond,
		cfg.MaterializeBatchSize,
	)
	return &Agent{db, metastore, broker, s3Client, cfg.S3.Bucket, cfg.FetchMaxBytes, buf, mat}
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
	a.broker.Add(handlers.NewFetchRequestHandler(a.Metastore, a.s3Client, a.bucket, a.fetchMaxBytes))
	a.broker.Add(handlers.NewFindCoordinatorRequestHandler(a.broker))
	a.broker.Add(handlers.NewOffsetCommitRequestHandler(a.Metastore))
	a.broker.Add(handlers.NewOffsetFetchRequestHandler(a.Metastore))
}

func (a *Agent) ListenAndServe(ctx context.Context) {
	go a.buffer.Start(ctx)
	go a.materializer.Start(ctx)
	a.broker.ListenAndServe(ctx)
}
