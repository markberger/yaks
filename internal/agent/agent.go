package agent

import (
	"context"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/buffer"
	"github.com/markberger/yaks/internal/config"
	"github.com/markberger/yaks/internal/handlers"
	"github.com/markberger/yaks/internal/materializer"
	"github.com/markberger/yaks/internal/metrics"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	log "github.com/sirupsen/logrus"
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
	metrics       statsd.ClientInterface
}

func NewAgent(db *gorm.DB, cfg config.Config) *Agent {
	metricsClient := metrics.NewClient(cfg.Statsd)
	metastore := metastore.NewGormMetastore(db)
	broker := broker.NewBroker(0, cfg.BrokerHost, cfg.BrokerPort, cfg.AdvertisedHost, cfg.GetAdvertisedPort(), metricsClient)
	s3Client := s3_client.CreateS3Client(cfg.S3)
	buf := buffer.NewWriteBuffer(
		s3Client,
		metastore,
		cfg.S3.Bucket,
		time.Duration(cfg.FlushIntervalMs)*time.Millisecond,
		cfg.FlushBytes,
		metricsClient,
	)
	mat := materializer.NewMaterializer(
		metastore,
		time.Duration(cfg.MaterializeIntervalMs)*time.Millisecond,
		cfg.MaterializeBatchSize,
		metricsClient,
	)
	return &Agent{db, metastore, broker, s3Client, cfg.S3.Bucket, cfg.FetchMaxBytes, buf, mat, metricsClient}
}

// TODO: agent should not apply migrations it should be done by a separate
// cmd tool before deployment
func (a *Agent) ApplyMigrations() error {
	return a.Metastore.ApplyMigrations()
}

func (a *Agent) AddHandlers() {
	a.broker.Add(handlers.NewMetadataRequestHandler(a.broker, a.Metastore))
	a.broker.Add(handlers.NewCreateTopicsRequestHandler(a.Metastore))
	a.broker.Add(handlers.NewProduceRequestHandler(a.Metastore, a.buffer, a.metrics))
	a.broker.Add(handlers.NewFetchRequestHandler(a.Metastore, a.s3Client, a.bucket, a.fetchMaxBytes, a.metrics))
	a.broker.Add(handlers.NewFindCoordinatorRequestHandler(a.broker))
	a.broker.Add(handlers.NewOffsetCommitRequestHandler(a.Metastore))
	a.broker.Add(handlers.NewOffsetFetchRequestHandler(a.Metastore))
	a.broker.Add(handlers.NewListOffsetsRequestHandler(a.Metastore))
}

func (a *Agent) ListenAndServe(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		a.buffer.Start(ctx)
	}()
	go func() {
		defer wg.Done()
		a.materializer.Start(ctx)
	}()

	a.broker.ListenAndServe(ctx)

	log.Info("Broker stopped, waiting for buffer and materializer to finish...")
	wg.Wait()
	log.Info("Agent shutdown complete")
}
