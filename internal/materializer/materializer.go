package materializer

import (
	"context"
	"time"

	"github.com/markberger/yaks/internal/metastore"
	log "github.com/sirupsen/logrus"
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
	for {
		n, err := m.metastore.MaterializeRecordBatchEvents(m.batchSize)
		if err != nil {
			log.WithError(err).Error("Materializer: failed to materialize events")
			return
		}
		if n == 0 {
			return
		}
	}
}
