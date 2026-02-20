package buffer

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockMetastore implements metastore.Metastore
type MockMetastore struct {
	mock.Mock
}

func (m *MockMetastore) ApplyMigrations() error {
	return m.Called().Error(0)
}

func (m *MockMetastore) CreateTopicV2(name string, nPartitions int32) error {
	return m.Called(name, nPartitions).Error(0)
}

func (m *MockMetastore) GetTopicsV2() ([]metastore.TopicV2, error) {
	args := m.Called()
	return args.Get(0).([]metastore.TopicV2), args.Error(1)
}

func (m *MockMetastore) GetTopicByName(name string) *metastore.TopicV2 {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*metastore.TopicV2)
}

func (m *MockMetastore) CommitRecordBatchEvents(events []metastore.RecordBatchEvent) error {
	return m.Called(events).Error(0)
}

func (m *MockMetastore) MaterializeRecordBatchEvents(nRecords int32) error {
	return m.Called(nRecords).Error(0)
}

func (m *MockMetastore) CommitRecordBatchesV2(batches []metastore.RecordBatchV2) ([]metastore.BatchCommitOutputV2, error) {
	args := m.Called(batches)
	return args.Get(0).([]metastore.BatchCommitOutputV2), args.Error(1)
}

func (m *MockMetastore) GetRecordBatchesV2(topicName string, partition int32, startOffset int64) ([]metastore.RecordBatchV2, error) {
	args := m.Called(topicName, partition, startOffset)
	return args.Get(0).([]metastore.RecordBatchV2), args.Error(1)
}

func (m *MockMetastore) GetTopicPartitions(topicName string) ([]metastore.TopicPartition, error) {
	args := m.Called(topicName)
	return args.Get(0).([]metastore.TopicPartition), args.Error(1)
}

func (m *MockMetastore) GetRecordBatchEvents(topicName string) ([]metastore.RecordBatchEvent, error) {
	args := m.Called(topicName)
	return args.Get(0).([]metastore.RecordBatchEvent), args.Error(1)
}

func (m *MockMetastore) CommitOffset(groupID string, topicID uuid.UUID, partition int32, offset int64, metadata string) error {
	return m.Called(groupID, topicID, partition, offset, metadata).Error(0)
}

func (m *MockMetastore) FetchOffset(groupID string, topicID uuid.UUID, partition int32) (int64, string, error) {
	args := m.Called(groupID, topicID, partition)
	return args.Get(0).(int64), args.Get(1).(string), args.Error(2)
}

func TestBasicFlush(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	var capturedBody []byte
	mockS3.On("PutObject", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		input := args.Get(1).(*s3.PutObjectInput)
		body, _ := io.ReadAll(input.Body)
		capturedBody = body
	}).Return(&s3.PutObjectOutput{}, nil)

	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(nil)

	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	data := []byte("hello-world")
	promise := wb.Submit(PartitionKey{TopicID: topicID, TopicName: "topic-1", Partition: 0}, data, 5)

	// Manually trigger flush
	wb.flush()

	err := promise.Wait()
	require.NoError(t, err)

	// Verify single S3 PUT with the data
	mockS3.AssertNumberOfCalls(t, "PutObject", 1)
	assert.Equal(t, data, capturedBody)

	// Verify CommitRecordBatchEvents called with correct byte offsets
	mockMeta.AssertCalled(t, "CommitRecordBatchEvents", mock.MatchedBy(func(events []metastore.RecordBatchEvent) bool {
		if len(events) != 1 {
			return false
		}
		e := events[0]
		return e.TopicID == topicID &&
			e.Partition == 0 &&
			e.NRecords == 5 &&
			e.ByteOffset == 0 &&
			e.ByteLength == int64(len(data))
	}))
}

func TestMultiPartitionPacking(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	var capturedBody []byte
	mockS3.On("PutObject", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		input := args.Get(1).(*s3.PutObjectInput)
		body, _ := io.ReadAll(input.Body)
		capturedBody = body
	}).Return(&s3.PutObjectOutput{}, nil)

	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(nil)

	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	data0 := []byte("partition-zero")
	data1 := []byte("partition-one")
	data2 := []byte("partition-two")

	wb.Submit(PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}, data0, 1)
	wb.Submit(PartitionKey{TopicID: topicID, TopicName: "t", Partition: 1}, data1, 2)
	wb.Submit(PartitionKey{TopicID: topicID, TopicName: "t", Partition: 2}, data2, 3)

	wb.flush()

	// 1 S3 object, 3 events
	mockS3.AssertNumberOfCalls(t, "PutObject", 1)

	// Verify packed data is concatenation
	expected := append(append(data0, data1...), data2...)
	assert.Equal(t, expected, capturedBody)

	mockMeta.AssertCalled(t, "CommitRecordBatchEvents", mock.MatchedBy(func(events []metastore.RecordBatchEvent) bool {
		if len(events) != 3 {
			return false
		}
		return events[0].ByteOffset == 0 && events[0].ByteLength == int64(len(data0)) &&
			events[1].ByteOffset == int64(len(data0)) && events[1].ByteLength == int64(len(data1)) &&
			events[2].ByteOffset == int64(len(data0)+len(data1)) && events[2].ByteLength == int64(len(data2))
	}))
}

func TestSamePartitionBatching(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	var capturedBody []byte
	mockS3.On("PutObject", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		input := args.Get(1).(*s3.PutObjectInput)
		body, _ := io.ReadAll(input.Body)
		capturedBody = body
	}).Return(&s3.PutObjectOutput{}, nil)

	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(nil)

	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	key := PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}
	wb.Submit(key, []byte("aaa"), 1)
	wb.Submit(key, []byte("bbb"), 2)
	wb.Submit(key, []byte("ccc"), 3)

	wb.flush()

	// All concatenated into one contiguous region → 1 event
	assert.Equal(t, []byte("aaabbbccc"), capturedBody)

	mockMeta.AssertCalled(t, "CommitRecordBatchEvents", mock.MatchedBy(func(events []metastore.RecordBatchEvent) bool {
		if len(events) != 1 {
			return false
		}
		return events[0].NRecords == 6 &&
			events[0].ByteOffset == 0 &&
			events[0].ByteLength == 9
	}))
}

func TestFlushCycleIsolation(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)
	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(nil)

	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	key := PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}

	// Cycle 1
	p1 := wb.Submit(key, []byte("cycle1"), 1)
	wb.flush()
	require.NoError(t, p1.Wait())

	// Cycle 2
	p2 := wb.Submit(key, []byte("cycle2"), 1)
	wb.flush()
	require.NoError(t, p2.Wait())

	// Two separate S3 PUTs
	mockS3.AssertNumberOfCalls(t, "PutObject", 2)
	mockMeta.AssertNumberOfCalls(t, "CommitRecordBatchEvents", 2)
}

func TestS3FailurePropagation(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return((*s3.PutObjectOutput)(nil), errors.New("s3 exploded"))

	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	key := PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}
	promise := wb.Submit(key, []byte("data"), 1)
	wb.flush()

	err := promise.Wait()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3 upload failed")

	// DB should NOT be called when S3 fails
	mockMeta.AssertNotCalled(t, "CommitRecordBatchEvents", mock.Anything)
}

func TestDBFailurePropagation(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)
	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(errors.New("db exploded"))

	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	key := PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}
	promise := wb.Submit(key, []byte("data"), 1)
	wb.flush()

	err := promise.Wait()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db commit failed")
}

func TestSizeThresholdTriggersFlush(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)
	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(nil)

	// Set flush threshold to 10 bytes
	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 10)

	ctx, cancel := context.WithCancel(context.Background())
	go wb.Start(ctx)

	key := PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}
	// Submit 15 bytes (exceeds 10 byte threshold)
	promise := wb.Submit(key, []byte("123456789012345"), 1)

	err := promise.Wait()
	require.NoError(t, err)

	cancel()
	// Give Start goroutine time to finish
	time.Sleep(50 * time.Millisecond)

	mockS3.AssertCalled(t, "PutObject", mock.Anything, mock.Anything)
}

func TestTimerFlush(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)
	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(nil)

	// Very short interval to trigger timer-based flush
	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", 50*time.Millisecond, 1<<30)

	ctx, cancel := context.WithCancel(context.Background())
	go wb.Start(ctx)

	key := PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}
	promise := wb.Submit(key, []byte("timer-data"), 1)

	// Wait for timer flush
	err := promise.Wait()
	require.NoError(t, err)

	cancel()
	time.Sleep(50 * time.Millisecond)

	mockS3.AssertCalled(t, "PutObject", mock.Anything, mock.Anything)
}

func TestConcurrentSubmits(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)
	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(nil)

	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	key := PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}

	var wg sync.WaitGroup
	nGoroutines := 50
	promises := make([]*FlushPromise, nGoroutines)

	for i := 0; i < nGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			promises[idx] = wb.Submit(key, []byte("x"), 1)
		}(i)
	}
	wg.Wait()

	wb.flush()

	for _, p := range promises {
		require.NoError(t, p.Wait())
	}
}

func TestEmptyFlush(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}

	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	// Flush with no submissions
	wb.flush()

	// No S3 PUT or DB call
	mockS3.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything)
	mockMeta.AssertNotCalled(t, "CommitRecordBatchEvents", mock.Anything)
}

func TestGracefulShutdown(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	mockMeta := &MockMetastore{}
	topicID := uuid.New()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)
	mockMeta.On("CommitRecordBatchEvents", mock.Anything).Return(nil)

	// Long interval so timer won't fire
	wb := NewWriteBuffer(mockS3, mockMeta, "test-bucket", time.Hour, 1<<30)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		wb.Start(ctx)
		close(done)
	}()

	key := PartitionKey{TopicID: topicID, TopicName: "t", Partition: 0}
	promise := wb.Submit(key, []byte("shutdown-data"), 1)

	// Cancel context to trigger shutdown + final flush
	cancel()

	// Start should return
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}

	err := promise.Wait()
	require.NoError(t, err)

	mockS3.AssertCalled(t, "PutObject", mock.Anything, mock.Anything)
	mockMeta.AssertCalled(t, "CommitRecordBatchEvents", mock.Anything)
}
