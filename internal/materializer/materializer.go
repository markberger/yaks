package materializer

import (
	"context"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/markberger/yaks/internal/metastore"
	log "github.com/sirupsen/logrus"
)

type Materializer struct {
	metastore metastore.Metastore
	interval  time.Duration
	batchSize int32
	metrics   statsd.ClientInterface
}

func NewMaterializer(metastore metastore.Metastore, interval time.Duration, batchSize int32, metrics statsd.ClientInterface) *Materializer {
	return &Materializer{
		metastore: metastore,
		interval:  interval,
		batchSize: batchSize,
		metrics:   metrics,
	}
}

// Start runs the background materialization loop. It blocks until ctx is
// cancelled, then performs a final drain before returning.
func (m *Materializer) Start(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.drain()
		case <-ctx.Done():
			m.drain()
			return
		}
	}
}

// drain calls MaterializeRecordBatchEvents in a loop until no events remain.
func (m *Materializer) drain() {
	m.metrics.Gauge("materializer.batch_size", float64(m.batchSize), []string{}, 1)
	for {
		start := time.Now()
		n, err := m.metastore.MaterializeRecordBatchEvents(m.batchSize)
		duration := time.Since(start)
		if err != nil {
			m.metrics.Timing("materializer.duration_ms", duration, []string{"status:error"}, 1)
			log.WithError(err).Error("Materializer: failed to materialize events")
			return
		}
		m.metrics.Timing("materializer.duration_ms", duration, []string{"status:success"}, 1)
		m.metrics.Count("materializer.events_materialized", int64(n), nil, 1)
		if n == 0 {
			return
		}
	}
}
