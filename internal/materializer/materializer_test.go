package materializer

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MaterializerTestSuite struct {
	suite.Suite
	*metastore.TestDB
}

func (s *MaterializerTestSuite) SetupSuite() {
	s.TestDB = metastore.NewTestDB()
}

func (s *MaterializerTestSuite) TearDownSuite() {
	s.TestDB.Close()
}

func TestMaterializerTestSuite(t *testing.T) {
	suite.Run(t, new(MaterializerTestSuite))
}

func (s *MaterializerTestSuite) TestBasicMaterialization() {
	T := s.T()
	ms := s.TestDB.InitMetastore()

	// Create a topic with 1 partition
	err := ms.CreateTopicV2("mat-test", 1)
	require.NoError(T, err)

	topic := ms.GetTopicByName("mat-test")
	require.NotNil(T, topic)

	// Insert record batch events
	events := []metastore.RecordBatchEvent{
		{
			TopicID:    topic.ID,
			Partition:  0,
			NRecords:   5,
			S3Key:      "data/test1.batch",
			ByteOffset: 0,
			ByteLength: 100,
		},
		{
			TopicID:    topic.ID,
			Partition:  0,
			NRecords:   3,
			S3Key:      "data/test2.batch",
			ByteOffset: 0,
			ByteLength: 60,
		},
	}
	err = ms.CommitRecordBatchEvents(events)
	require.NoError(T, err)

	// Start materializer with a short interval
	m := NewMaterializer(ms, 50*time.Millisecond, 100)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		m.Start(ctx)
		close(done)
	}()

	// Wait for at least one tick
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		T.Fatal("Start did not return after context cancellation")
	}

	// Verify RecordBatchV2 rows were created with correct offsets
	batches, err := ms.GetRecordBatchesV2("mat-test", 0, 0)
	require.NoError(T, err)
	require.Len(T, batches, 2)

	// Batches are ordered by start_offset, but within a single
	// CommitRecordBatchEvents call UUIDs are random so either event
	// may be materialized first. Verify structural correctness.
	require.Equal(T, int64(0), batches[0].StartOffset)
	require.Equal(T, batches[0].NRecords, batches[1].StartOffset)
	require.Equal(T, int64(8), batches[0].NRecords+batches[1].NRecords)

	// Verify partition end offset was updated
	partitions, err := ms.GetTopicPartitions("mat-test")
	require.NoError(T, err)
	require.Len(T, partitions, 1)
	require.Equal(T, int64(8), partitions[0].EndOffset)
}

func (s *MaterializerTestSuite) TestGracefulShutdown() {
	T := s.T()
	ms := s.TestDB.InitMetastore()

	// Create a topic
	err := ms.CreateTopicV2("shutdown-test", 1)
	require.NoError(T, err)

	topic := ms.GetTopicByName("shutdown-test")
	require.NotNil(T, topic)

	// Long interval so the ticker won't fire
	m := NewMaterializer(ms, time.Hour, 100)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		m.Start(ctx)
		close(done)
	}()

	// Insert events after materializer has started
	events := []metastore.RecordBatchEvent{
		{
			TopicID:    topic.ID,
			Partition:  0,
			NRecords:   10,
			S3Key:      "data/shutdown.batch",
			ByteOffset: 0,
			ByteLength: 200,
		},
	}
	err = ms.CommitRecordBatchEvents(events)
	require.NoError(T, err)

	// Cancel context — the final drain should materialize the events
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		T.Fatal("Start did not return after context cancellation")
	}

	// Verify the final drain materialized the events
	batches, err := ms.GetRecordBatchesV2("shutdown-test", 0, 0)
	require.NoError(T, err)
	require.Len(T, batches, 1)
	require.Equal(T, int64(0), batches[0].StartOffset)
	require.Equal(T, int64(10), batches[0].NRecords)
}

func (s *MaterializerTestSuite) TestEmptyTick() {
	T := s.T()
	ms := s.TestDB.InitMetastore()

	// Start materializer with no pending events
	m := NewMaterializer(ms, 50*time.Millisecond, 100)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		m.Start(ctx)
		close(done)
	}()

	// Let a few ticks pass with no events
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		T.Fatal("Start did not return after context cancellation")
	}
}

func (s *MaterializerTestSuite) TestDrainLoop() {
	T := s.T()
	ms := s.TestDB.InitMetastore()

	// Create a topic
	err := ms.CreateTopicV2("drain-test", 1)
	require.NoError(T, err)

	topic := ms.GetTopicByName("drain-test")
	require.NotNil(T, topic)

	// Insert more events than the batch size to verify the drain loop
	// processes all of them in a single tick
	batchSize := int32(2)
	events := make([]metastore.RecordBatchEvent, 5)
	for i := range events {
		events[i] = metastore.RecordBatchEvent{
			TopicID:    topic.ID,
			Partition:  0,
			NRecords:   1,
			S3Key:      "data/" + uuid.New().String() + ".batch",
			ByteOffset: 0,
			ByteLength: 20,
		}
	}
	err = ms.CommitRecordBatchEvents(events)
	require.NoError(T, err)

	// Start materializer with small batch size
	m := NewMaterializer(ms, 50*time.Millisecond, batchSize)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		m.Start(ctx)
		close(done)
	}()

	// Wait for at least one tick
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		T.Fatal("Start did not return after context cancellation")
	}

	// All 5 events should have been materialized despite batch size of 2
	batches, err := ms.GetRecordBatchesV2("drain-test", 0, 0)
	require.NoError(T, err)
	require.Len(T, batches, 5)
}
