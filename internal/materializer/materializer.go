package materializer

import (
	"context"
	"time"

	"github.com/markberger/yaks/internal/metrics"
	"github.com/markberger/yaks/internal/metastore"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Materializer struct {
	metastore metastore.Metastore
	interval  time.Duration
	batchSize int32
}

func NewMaterializer(metastore metastore.Metastore, interval time.Duration, batchSize int32) *Materializer {
	return &Materializer{
		metastore: metastore,
		interval:  interval,
		batchSize: batchSize,
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
	ctx := context.Background()
	materializeDuration, _ := metrics.Meter.Float64Histogram("yaks.materializer.duration", metric.WithUnit("s"))
	eventsMaterialized, _ := metrics.Meter.Int64Counter("yaks.materializer.events.materialized")
	batchSizeGauge, _ := metrics.Meter.Int64Gauge("yaks.materializer.batch_size")

	batchSizeGauge.Record(ctx, int64(m.batchSize))
	for {
		start := time.Now()
		n, err := m.metastore.MaterializeRecordBatchEvents(m.batchSize)
		duration := time.Since(start)
		if err != nil {
			materializeDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
				attribute.String("status", "error"),
			))
			log.WithError(err).Error("Materializer: failed to materialize events")
			return
		}
		materializeDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
			attribute.String("status", "success"),
		))
		eventsMaterialized.Add(ctx, int64(n))
		if n == 0 {
			return
		}
	}
}
