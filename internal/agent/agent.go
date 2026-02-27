package agent

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/buffer"
	"github.com/markberger/yaks/internal/cache"
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
	db             *gorm.DB
	Metastore      metastore.Metastore
	broker         *broker.Broker
	s3Client       s3_client.S3Client
	bucket         string
	fetchMaxBytes  int32
	buffer         *buffer.WriteBuffer
	materializer   *materializer.Materializer
	metricsHandler http.Handler
	metricsPort    int32
	cache          *cache.Cache
}

func NewAgent(db *gorm.DB, cfg config.Config) *Agent {
	metricsHandler, err := metrics.Init(cfg.OTel)
	if err != nil {
		log.WithError(err).Fatal("Failed to initialize OTel metrics")
	}

	metastore := metastore.NewGormMetastore(db)
	b := broker.NewBroker(0, cfg.BrokerHost, cfg.BrokerPort, cfg.AdvertisedHost, cfg.GetAdvertisedPort())
	rawS3Client := s3_client.CreateS3Client(cfg.S3)

	// WriteBuffer always uses the raw S3 client (writes are not cached)
	buf := buffer.NewWriteBuffer(
		rawS3Client,
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

	// Conditionally wrap fetch S3 client with groupcache
	var fetchS3Client s3_client.S3Client = rawS3Client
	var s3Cache *cache.Cache
	if cfg.Groupcache.Enabled {
		advertisedHost := cfg.Groupcache.AdvertisedHost
		if advertisedHost == "" {
			advertisedHost = cfg.AdvertisedHost
		}
		selfURL := fmt.Sprintf("http://%s:%d", advertisedHost, cfg.Groupcache.Port)
		s3Cache = cache.New(b.NodeID, metastore, rawS3Client, cfg.S3.Bucket, selfURL, cfg.Groupcache)
		fetchS3Client = cache.NewCachedS3Client(rawS3Client, s3Cache)
		log.WithField("selfURL", selfURL).Info("Groupcache enabled")
	}

	return &Agent{
		db:             db,
		Metastore:      metastore,
		broker:         b,
		s3Client:       fetchS3Client,
		bucket:         cfg.S3.Bucket,
		fetchMaxBytes:  cfg.FetchMaxBytes,
		buffer:         buf,
		materializer:   mat,
		metricsHandler: metricsHandler,
		metricsPort:    cfg.OTel.MetricsPort,
		cache:          s3Cache,
	}
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

	if a.cache != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.cache.Start(ctx)
		}()
	}

	if a.metricsHandler != nil {
		mux := http.NewServeMux()
		mux.Handle("/metrics", a.metricsHandler)
		srv := &http.Server{
			Addr:    fmt.Sprintf(":%d", a.metricsPort),
			Handler: mux,
		}
		go func() {
			log.WithField("port", a.metricsPort).Info("Serving Prometheus metrics")
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.WithError(err).Error("Metrics server error")
			}
		}()
		go func() {
			<-ctx.Done()
			srv.Shutdown(context.Background())
		}()
	}

	a.broker.ListenAndServe(ctx)

	log.Info("Broker stopped, waiting for buffer and materializer to finish...")
	wg.Wait()
	log.Info("Agent shutdown complete")
}
