package buffer

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	log "github.com/sirupsen/logrus"
)

type PartitionKey struct {
	TopicID   uuid.UUID
	TopicName string
	Partition int32
}

type submission struct {
	Key      PartitionKey
	Data     []byte
	NRecords int64
}

// FlushPromise represents the result of a flush cycle. Callers wait on it to
// know whether their data was durably committed.
type FlushPromise struct {
	done chan struct{}
	err  error
}

// Wait blocks until the flush cycle completes and returns any error.
func (p *FlushPromise) Wait() error {
	<-p.done
	return p.err
}

type WriteBuffer struct {
	mu           sync.Mutex
	submissions  []submission
	totalBytes   int
	currentCycle *FlushPromise

	s3Client   s3_client.S3Client
	metastore  metastore.Metastore
	bucketName string

	flushInterval time.Duration
	flushBytes    int
	triggerCh     chan struct{} // buffered(1), signals early flush
}

func NewWriteBuffer(
	s3Client s3_client.S3Client,
	metastore metastore.Metastore,
	bucketName string,
	flushInterval time.Duration,
	flushBytes int,
) *WriteBuffer {
	return &WriteBuffer{
		currentCycle:  newFlushPromise(),
		s3Client:      s3Client,
		metastore:     metastore,
		bucketName:    bucketName,
		flushInterval: flushInterval,
		flushBytes:    flushBytes,
		triggerCh:     make(chan struct{}, 1),
	}
}

func newFlushPromise() *FlushPromise {
	return &FlushPromise{done: make(chan struct{})}
}

// Start runs the background flush loop. It blocks until ctx is cancelled,
// then performs a final flush before returning.
func (wb *WriteBuffer) Start(ctx context.Context) {
	ticker := time.NewTicker(wb.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wb.flush()
		case <-wb.triggerCh:
			wb.flush()
		case <-ctx.Done():
			wb.flush()
			return
		}
	}
}

// Submit adds a record batch to the buffer and returns a FlushPromise that
// resolves when the data has been flushed to S3 and committed to the DB.
func (wb *WriteBuffer) Submit(key PartitionKey, data []byte, nRecords int64) *FlushPromise {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	wb.submissions = append(wb.submissions, submission{
		Key:      key,
		Data:     data,
		NRecords: nRecords,
	})
	wb.totalBytes += len(data)

	promise := wb.currentCycle

	if wb.totalBytes >= wb.flushBytes {
		// Non-blocking send to trigger early flush
		select {
		case wb.triggerCh <- struct{}{}:
		default:
		}
	}

	return promise
}

// partitionGroup aggregates all submissions for a single (TopicID, Partition).
type partitionGroup struct {
	key      PartitionKey
	data     []byte
	nRecords int64
}

func (wb *WriteBuffer) flush() {
	// Swap out current submissions under lock
	wb.mu.Lock()
	subs := wb.submissions
	cycle := wb.currentCycle

	wb.submissions = nil
	wb.totalBytes = 0
	wb.currentCycle = newFlushPromise()
	wb.mu.Unlock()

	// Nothing to flush
	if len(subs) == 0 {
		cycle.err = nil
		close(cycle.done)
		return
	}

	// Group by partition: merge all submissions for the same (TopicID, Partition)
	type groupKey struct {
		TopicID   uuid.UUID
		Partition int32
	}
	groupOrder := []groupKey{}
	groups := make(map[groupKey]*partitionGroup)

	for _, sub := range subs {
		gk := groupKey{TopicID: sub.Key.TopicID, Partition: sub.Key.Partition}
		g, ok := groups[gk]
		if !ok {
			g = &partitionGroup{key: sub.Key}
			groups[gk] = g
			groupOrder = append(groupOrder, gk)
		}
		g.data = append(g.data, sub.Data...)
		g.nRecords += sub.NRecords
	}

	// Pack all groups into a single blob, tracking byte offsets
	var packed []byte
	type eventInfo struct {
		key        PartitionKey
		byteOffset int64
		byteLength int64
		nRecords   int64
	}
	var events []eventInfo

	for _, gk := range groupOrder {
		g := groups[gk]
		offset := int64(len(packed))
		packed = append(packed, g.data...)
		events = append(events, eventInfo{
			key:        g.key,
			byteOffset: offset,
			byteLength: int64(len(g.data)),
			nRecords:   g.nRecords,
		})
	}

	// Single S3 PUT
	s3Key := fmt.Sprintf("data/%s.batch", uuid.New().String())
	size := int64(len(packed))
	reader := bytes.NewReader(packed)
	input := &s3.PutObjectInput{
		Bucket:        aws.String(wb.bucketName),
		Key:           aws.String(s3Key),
		Body:          reader,
		ContentLength: &size,
		ContentType:   aws.String("application/octet-stream"),
	}

	_, err := wb.s3Client.PutObject(context.Background(), input)
	if err != nil {
		log.WithError(err).Error("WriteBuffer: S3 upload failed")
		cycle.err = fmt.Errorf("s3 upload failed: %w", err)
		close(cycle.done)
		return
	}

	// Build RecordBatchEvents
	batchEvents := make([]metastore.RecordBatchEvent, len(events))
	for i, e := range events {
		batchEvents[i] = metastore.RecordBatchEvent{
			TopicID:    e.key.TopicID,
			Partition:  e.key.Partition,
			NRecords:   e.nRecords,
			S3Key:      s3Key,
			ByteOffset: e.byteOffset,
			ByteLength: e.byteLength,
		}
	}

	// DB commit
	if err := wb.metastore.CommitRecordBatchEvents(batchEvents); err != nil {
		log.WithError(err).Error("WriteBuffer: DB commit failed")
		cycle.err = fmt.Errorf("db commit failed: %w", err)
		close(cycle.done)
		return
	}

	cycle.err = nil
	close(cycle.done)
}
