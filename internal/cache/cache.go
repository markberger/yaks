package cache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/mailgun/groupcache/v2"
	"github.com/markberger/yaks/internal/config"
	"github.com/markberger/yaks/internal/metrics"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"
)

type Cache struct {
	pool      *groupcache.HTTPPool
	group     *groupcache.Group
	nodeID    int32
	selfURL   string
	metastore metastore.Metastore
	s3Client  s3_client.S3Client
	bucket    string
	cfg       config.GroupcacheConfig
}

func New(nodeID int32, ms metastore.Metastore, s3Client s3_client.S3Client, bucket string, selfURL string, cfg config.GroupcacheConfig) *Cache {
	pool := groupcache.NewHTTPPoolOpts(selfURL, &groupcache.HTTPPoolOptions{
		BasePath: "/_groupcache/",
	})

	cacheSizeBytes := cfg.CacheSizeMB * 1024 * 1024

	c := &Cache{
		pool:      pool,
		nodeID:    nodeID,
		selfURL:   selfURL,
		metastore: ms,
		s3Client:  s3Client,
		bucket:    bucket,
		cfg:       cfg,
	}

	c.group = groupcache.NewGroup("s3objects", cacheSizeBytes, groupcache.GetterFunc(
		func(ctx context.Context, key string, dest groupcache.Sink) error {
			log.WithField("key", key).Info("Cache miss: fetching from S3")
			resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &key,
			})
			if err != nil {
				return fmt.Errorf("groupcache getter: GetObject %s: %w", key, err)
			}
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("groupcache getter: read body %s: %w", key, err)
			}

			return dest.SetBytes(data, time.Time{})
		},
	))

	return c
}

func (c *Cache) Get(ctx context.Context, s3Key string) ([]byte, error) {
	var data []byte
	if err := c.group.Get(ctx, s3Key, groupcache.AllocatingByteSliceSink(&data)); err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Cache) Start(ctx context.Context) {
	var wg sync.WaitGroup

	// 1. Peer HTTP server
	mux := http.NewServeMux()
	mux.Handle("/_groupcache/", c.pool)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", c.cfg.Port),
		Handler: mux,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.WithFields(log.Fields{
			"port":    c.cfg.Port,
			"selfURL": c.selfURL,
		}).Info("Groupcache peer server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Error("Groupcache peer server error")
		}
	}()

	heartbeatInterval := time.Duration(c.cfg.HeartbeatInterval) * time.Millisecond
	pollInterval := time.Duration(c.cfg.PollInterval) * time.Millisecond
	leaseTTL := time.Duration(c.cfg.LeaseTTL) * time.Millisecond

	// 2. Heartbeat loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.heartbeat(ctx, heartbeatInterval, leaseTTL)
	}()

	// 3. Poll loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.pollPeers(ctx, pollInterval)
	}()

	<-ctx.Done()

	log.Info("Groupcache shutting down...")

	if err := c.metastore.DeleteGroupcachePeer(c.nodeID); err != nil {
		log.WithError(err).Error("Failed to deregister groupcache peer")
	}

	srv.Shutdown(context.Background())
	wg.Wait()
	log.Info("Groupcache shutdown complete")
}

func (c *Cache) heartbeat(ctx context.Context, interval, leaseTTL time.Duration) {
	// Register immediately on start
	if err := c.metastore.UpsertGroupcachePeer(c.nodeID, c.selfURL, leaseTTL); err != nil {
		log.WithError(err).Error("Groupcache heartbeat failed (initial)")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.metastore.UpsertGroupcachePeer(c.nodeID, c.selfURL, leaseTTL); err != nil {
				log.WithError(err).Error("Groupcache heartbeat failed")
			}
		}
	}
}

func (c *Cache) pollPeers(ctx context.Context, interval time.Duration) {
	// Instruments
	peersGauge, _ := metrics.Meter.Int64ObservableGauge("yaks.groupcache.peers")
	cacheGets, _ := metrics.Meter.Int64Counter("yaks.groupcache.cache.gets")
	cacheHits, _ := metrics.Meter.Int64Counter("yaks.groupcache.cache.hits")
	peerLoads, _ := metrics.Meter.Int64Counter("yaks.groupcache.peer.loads")
	localLoads, _ := metrics.Meter.Int64Counter("yaks.groupcache.local.loads")
	peerErrors, _ := metrics.Meter.Int64Counter("yaks.groupcache.peer.errors")

	var currentPeerCount int64
	metrics.Meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(peersGauge, currentPeerCount)
		return nil
	}, peersGauge)

	var prevStats groupcache.Stats

	// Poll immediately on start
	c.doPollPeers(&currentPeerCount)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.doPollPeers(&currentPeerCount)

			// Export stats deltas
			stats := c.group.Stats
			emitDelta(cacheGets, stats.Gets.Get(), &prevStats.Gets)
			emitDelta(cacheHits, stats.CacheHits.Get(), &prevStats.CacheHits)
			emitDelta(peerLoads, stats.PeerLoads.Get(), &prevStats.PeerLoads)
			emitDelta(localLoads, stats.LocalLoads.Get(), &prevStats.LocalLoads)
			emitDelta(peerErrors, stats.PeerErrors.Get(), &prevStats.PeerErrors)
		}
	}
}

func (c *Cache) doPollPeers(peerCount *int64) {
	urls, err := c.metastore.GetLiveGroupcachePeers()
	if err != nil {
		log.WithError(err).Error("Groupcache poll peers failed")
		return
	}
	c.pool.Set(urls...)
	*peerCount = int64(len(urls))
	log.WithField("peers", len(urls)).Debug("Groupcache peers updated")
}

func emitDelta(counter metric.Int64Counter, current int64, prev *groupcache.AtomicInt) {
	old := prev.Get()
	delta := current - old
	if delta > 0 {
		counter.Add(context.Background(), delta)
	}
	prev.Store(current)
}
