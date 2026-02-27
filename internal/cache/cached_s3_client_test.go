package cache

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/mailgun/groupcache/v2"
	"github.com/markberger/yaks/internal/config"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	testPool     *groupcache.HTTPPool
	testPoolOnce sync.Once
)

// MockMetastore implements metastore.Metastore for cache tests.
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
func (m *MockMetastore) MaterializeRecordBatchEvents(nRecords int32) (int, error) {
	args := m.Called(nRecords)
	return args.Int(0), args.Error(1)
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
func (m *MockMetastore) UpsertGroupcachePeer(nodeID int32, peerURL string, leaseDuration time.Duration) error {
	return m.Called(nodeID, peerURL, leaseDuration).Error(0)
}
func (m *MockMetastore) GetLiveGroupcachePeers() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockMetastore) DeleteGroupcachePeer(nodeID int32) error {
	return m.Called(nodeID).Error(0)
}

// newTestCache creates a Cache with a unique group name to avoid groupcache
// singleton panics across tests. The HTTPPool is created once per process;
// each test gets its own groupcache Group (deregistered on cleanup).
func newTestCache(t *testing.T, mockS3 *s3_client.MockS3Client) *Cache {
	t.Helper()

	testPoolOnce.Do(func() {
		testPool = groupcache.NewHTTPPoolOpts("http://localhost:0", &groupcache.HTTPPoolOptions{
			BasePath: "/_groupcache/",
		})
	})

	groupName := "test-" + t.Name()
	groupcache.DeregisterGroup(groupName)

	mockMS := &MockMetastore{}
	bucket := "test-bucket"

	c := &Cache{
		pool:      testPool,
		nodeID:    0,
		selfURL:   "http://localhost:0",
		metastore: mockMS,
		s3Client:  mockS3,
		bucket:    bucket,
		cfg:       config.GroupcacheConfig{CacheSizeMB: 64},
	}

	c.group = groupcache.NewGroup(groupName, 64*1024*1024, groupcache.GetterFunc(
		func(ctx context.Context, key string, dest groupcache.Sink) error {
			resp, err := mockS3.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &key,
			})
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			return dest.SetBytes(data, time.Time{})
		},
	))

	t.Cleanup(func() {
		groupcache.DeregisterGroup(groupName)
	})

	return c
}

func TestCachedS3Client_GetObject_ReturnsCachedData(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	cache := newTestCache(t, mockS3)
	client := NewCachedS3Client(mockS3, cache)

	expectedData := []byte("hello, this is s3 object data")
	key := "data/abc.batch"

	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Key == key
	})).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(expectedData)),
	}, nil).Once()

	resp, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: strPtr("test-bucket"),
		Key:    strPtr(key),
	})
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, expectedData, body)
	mockS3.AssertExpectations(t)
}

func TestCachedS3Client_GetObject_CachesRepeatedRequests(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	cache := newTestCache(t, mockS3)
	client := NewCachedS3Client(mockS3, cache)

	expectedData := []byte("cached object body")
	key := "data/def.batch"

	// S3 should only be called once — second call should be served from cache
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Key == key
	})).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(expectedData)),
	}, nil).Once()

	// First call — populates cache
	resp1, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: strPtr("test-bucket"),
		Key:    strPtr(key),
	})
	require.NoError(t, err)
	body1, _ := io.ReadAll(resp1.Body)
	assert.Equal(t, expectedData, body1)

	// Second call — should come from cache, NOT from S3
	resp2, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: strPtr("test-bucket"),
		Key:    strPtr(key),
	})
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	assert.Equal(t, expectedData, body2)

	// Verify S3 GetObject was called exactly once
	mockS3.AssertNumberOfCalls(t, "GetObject", 1)
}

func TestCachedS3Client_GetObject_DifferentKeysHitS3Separately(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	cache := newTestCache(t, mockS3)
	client := NewCachedS3Client(mockS3, cache)

	data1 := []byte("object-1")
	data2 := []byte("object-2")
	key1 := "data/key1.batch"
	key2 := "data/key2.batch"

	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Key == key1
	})).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data1)),
	}, nil).Once()

	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Key == key2
	})).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data2)),
	}, nil).Once()

	resp1, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: strPtr("test-bucket"),
		Key:    strPtr(key1),
	})
	require.NoError(t, err)
	body1, _ := io.ReadAll(resp1.Body)
	assert.Equal(t, data1, body1)

	resp2, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: strPtr("test-bucket"),
		Key:    strPtr(key2),
	})
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	assert.Equal(t, data2, body2)

	mockS3.AssertNumberOfCalls(t, "GetObject", 2)
}

func TestCachedS3Client_PutObject_PassesThrough(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	cache := newTestCache(t, mockS3)
	client := NewCachedS3Client(mockS3, cache)

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil).Once()

	input := &s3.PutObjectInput{
		Bucket: strPtr("test-bucket"),
		Key:    strPtr("data/upload.batch"),
		Body:   bytes.NewReader([]byte("upload-data")),
	}

	_, err := client.PutObject(context.Background(), input)
	require.NoError(t, err)
	mockS3.AssertCalled(t, "PutObject", mock.Anything, input)
}

func TestCachedS3Client_GetObject_S3Error(t *testing.T) {
	mockS3 := &s3_client.MockS3Client{}
	cache := newTestCache(t, mockS3)
	client := NewCachedS3Client(mockS3, cache)

	key := "data/missing.batch"

	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Key == key
	})).Return((*s3.GetObjectOutput)(nil), assert.AnError).Once()

	_, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: strPtr("test-bucket"),
		Key:    strPtr(key),
	})
	require.Error(t, err)
}

func TestCachedS3Client_ImplementsS3ClientInterface(t *testing.T) {
	var _ s3_client.S3Client = (*CachedS3Client)(nil)
}

func strPtr(s string) *string {
	return &s
}
