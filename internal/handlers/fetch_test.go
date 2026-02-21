package handlers

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func createFetchRequest(topic string, partition int32, offset int64) *kmsg.FetchRequest {
	req := kmsg.NewFetchRequest()
	req.SetVersion(3)
	t := kmsg.NewFetchRequestTopic()
	t.Topic = topic
	p := kmsg.NewFetchRequestTopicPartition()
	p.Partition = partition
	p.FetchOffset = offset
	t.Partitions = []kmsg.FetchRequestTopicPartition{p}
	req.Topics = []kmsg.FetchRequestTopic{t}
	return &req
}

// fakeBatchData returns n bytes that are large enough for the FirstOffset patch (8 bytes).
func fakeBatchData(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

func mockGetObjectReturn(data []byte) *s3.GetObjectOutput {
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}
}

// seedBatch creates a topic and commits a record batch, returning the committed output.
func seedBatch(ms metastore.Metastore, topic string, partition int32, nRecords int64, s3Key string, byteLength int64) metastore.BatchCommitOutputV2 {
	ms.CreateTopicV2(topic, partition+1)
	outputs, _ := ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: ms.GetTopicByName(topic).ID, Partition: partition, NRecords: nRecords, S3Key: s3Key, ByteLength: byteLength},
	})
	return outputs[0]
}

func (s *HandlersTestSuite) TestFetch_Success_PatchesFirstOffset() {
	ms := s.TestDB.InitMetastore()
	batchBytes := fakeBatchData(61)
	output := seedBatch(ms, "fetch-test", 0, 5, "batches/001.batch", int64(len(batchBytes)))
	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(mockGetObjectReturn(batchBytes), nil)

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)
	resp, err := handler.Handle(createFetchRequest("fetch-test", 0, 0))

	require.NoError(s.T(), err)
	fetchResp := resp.(*kmsg.FetchResponse)
	require.Len(s.T(), fetchResp.Topics, 1)
	require.Equal(s.T(), "fetch-test", fetchResp.Topics[0].Topic)
	require.Len(s.T(), fetchResp.Topics[0].Partitions, 1)

	records := fetchResp.Topics[0].Partitions[0].RecordBatches

	// The handler should pass through the full S3 object body
	require.Len(s.T(), records, len(batchBytes))

	// Verify FirstOffset was patched to the committed StartOffset
	gotOffset := binary.BigEndian.Uint64(records[:8])
	require.Equal(s.T(), uint64(output.BaseOffset), gotOffset)

	mockS3.AssertCalled(s.T(), "GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Key == "batches/001.batch" && *input.Bucket == "test-bucket"
	}))
}

func (s *HandlersTestSuite) TestFetch_OffsetPastAllBatches_EmptyResponse() {
	ms := s.TestDB.InitMetastore()
	seedBatch(ms, "fetch-empty", 0, 5, "batches/002.batch", 61)

	mockS3 := &s3_client.MockS3Client{}
	// No GetObject calls expected

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)
	resp, err := handler.Handle(createFetchRequest("fetch-empty", 0, 9999))

	require.NoError(s.T(), err)
	fetchResp := resp.(*kmsg.FetchResponse)
	require.Empty(s.T(), fetchResp.Topics[0].Partitions[0].RecordBatches)

	mockS3.AssertNotCalled(s.T(), "GetObject", mock.Anything, mock.Anything)
}

func (s *HandlersTestSuite) TestFetch_MultipleBatches_Concatenated() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("fetch-multi", 1)
	topicID := ms.GetTopicByName("fetch-multi").ID

	dataA := fakeBatchData(16)
	dataB := fakeBatchData(32)

	// Commit two batches sequentially
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 3, S3Key: "batch/a.batch", ByteLength: int64(len(dataA))},
	})
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 2, S3Key: "batch/b.batch", ByteLength: int64(len(dataB))},
	})
	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(in *s3.GetObjectInput) bool {
		return *in.Key == "batch/a.batch"
	})).Return(mockGetObjectReturn(dataA), nil)
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(in *s3.GetObjectInput) bool {
		return *in.Key == "batch/b.batch"
	})).Return(mockGetObjectReturn(dataB), nil)

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)
	resp, err := handler.Handle(createFetchRequest("fetch-multi", 0, 0))

	require.NoError(s.T(), err)
	records := resp.(*kmsg.FetchResponse).Topics[0].Partitions[0].RecordBatches

	// The handler should pass through the full S3 objects
	require.Len(s.T(), records, 48) // 16 + 32

	// Batch A: starts at offset 0 (first commit, 3 records → offsets 0-2)
	firstOffsetA := binary.BigEndian.Uint64(records[:8])
	require.Equal(s.T(), uint64(0), firstOffsetA)

	// Batch B: starts at offset 3 (second commit, 2 records → offsets 3-4)
	firstOffsetB := binary.BigEndian.Uint64(records[16 : 16+8])
	require.Equal(s.T(), uint64(3), firstOffsetB)
}

func (s *HandlersTestSuite) TestFetch_HighWatermark_ReflectsEndOffset() {
	ms := s.TestDB.InitMetastore()
	batchBytes := fakeBatchData(61)
	seedBatch(ms, "fetch-hwm", 0, 5, "batches/hwm.batch", int64(len(batchBytes)))

	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(mockGetObjectReturn(batchBytes), nil)

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)
	resp, err := handler.Handle(createFetchRequest("fetch-hwm", 0, 0))

	require.NoError(s.T(), err)
	fetchResp := resp.(*kmsg.FetchResponse)
	require.Len(s.T(), fetchResp.Topics[0].Partitions, 1)
	require.Equal(s.T(), int64(5), fetchResp.Topics[0].Partitions[0].HighWatermark)
}

func (s *HandlersTestSuite) TestFetch_HighWatermark_EmptyPartition() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("fetch-hwm-empty", 1)

	mockS3 := &s3_client.MockS3Client{}

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)
	resp, err := handler.Handle(createFetchRequest("fetch-hwm-empty", 0, 0))

	require.NoError(s.T(), err)
	fetchResp := resp.(*kmsg.FetchResponse)
	require.Len(s.T(), fetchResp.Topics[0].Partitions, 1)
	require.Equal(s.T(), int64(0), fetchResp.Topics[0].Partitions[0].HighWatermark)
}

func (s *HandlersTestSuite) TestFetch_MultipleTopicsAndPartitions() {
	ms := s.TestDB.InitMetastore()

	// Create two topics: topicA with 2 partitions, topicB with 1 partition
	ms.CreateTopicV2("fetch-multi-a", 2)
	ms.CreateTopicV2("fetch-multi-b", 1)
	topicAID := ms.GetTopicByName("fetch-multi-a").ID
	topicBID := ms.GetTopicByName("fetch-multi-b").ID

	dataA0 := fakeBatchData(16)
	dataA1 := fakeBatchData(24)
	dataB0 := fakeBatchData(32)

	// Commit batches to different topic/partitions
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicAID, Partition: 0, NRecords: 3, S3Key: "multi/a0.batch", ByteLength: int64(len(dataA0))},
		{TopicID: topicAID, Partition: 1, NRecords: 2, S3Key: "multi/a1.batch", ByteLength: int64(len(dataA1))},
		{TopicID: topicBID, Partition: 0, NRecords: 4, S3Key: "multi/b0.batch", ByteLength: int64(len(dataB0))},
	})

	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(in *s3.GetObjectInput) bool {
		return *in.Key == "multi/a0.batch"
	})).Return(mockGetObjectReturn(dataA0), nil)
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(in *s3.GetObjectInput) bool {
		return *in.Key == "multi/a1.batch"
	})).Return(mockGetObjectReturn(dataA1), nil)
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(in *s3.GetObjectInput) bool {
		return *in.Key == "multi/b0.batch"
	})).Return(mockGetObjectReturn(dataB0), nil)

	// Build a fetch request with 2 topics, topicA requesting partitions 0 and 1
	req := kmsg.NewFetchRequest()
	req.SetVersion(3)
	tA := kmsg.NewFetchRequestTopic()
	tA.Topic = "fetch-multi-a"
	pA0 := kmsg.NewFetchRequestTopicPartition()
	pA0.Partition = 0
	pA0.FetchOffset = 0
	pA1 := kmsg.NewFetchRequestTopicPartition()
	pA1.Partition = 1
	pA1.FetchOffset = 0
	tA.Partitions = []kmsg.FetchRequestTopicPartition{pA0, pA1}
	tB := kmsg.NewFetchRequestTopic()
	tB.Topic = "fetch-multi-b"
	pB0 := kmsg.NewFetchRequestTopicPartition()
	pB0.Partition = 0
	pB0.FetchOffset = 0
	tB.Partitions = []kmsg.FetchRequestTopicPartition{pB0}
	req.Topics = []kmsg.FetchRequestTopic{tA, tB}

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)
	resp, err := handler.Handle(&req)

	require.NoError(s.T(), err)
	fetchResp := resp.(*kmsg.FetchResponse)
	require.Len(s.T(), fetchResp.Topics, 2)

	// Verify topic A
	require.Equal(s.T(), "fetch-multi-a", fetchResp.Topics[0].Topic)
	require.Len(s.T(), fetchResp.Topics[0].Partitions, 2)
	require.Equal(s.T(), int32(0), fetchResp.Topics[0].Partitions[0].Partition)
	require.Len(s.T(), fetchResp.Topics[0].Partitions[0].RecordBatches, len(dataA0))
	require.Equal(s.T(), int64(3), fetchResp.Topics[0].Partitions[0].HighWatermark)
	require.Equal(s.T(), int32(1), fetchResp.Topics[0].Partitions[1].Partition)
	require.Len(s.T(), fetchResp.Topics[0].Partitions[1].RecordBatches, len(dataA1))
	require.Equal(s.T(), int64(2), fetchResp.Topics[0].Partitions[1].HighWatermark)

	// Verify topic B
	require.Equal(s.T(), "fetch-multi-b", fetchResp.Topics[1].Topic)
	require.Len(s.T(), fetchResp.Topics[1].Partitions, 1)
	require.Equal(s.T(), int32(0), fetchResp.Topics[1].Partitions[0].Partition)
	require.Len(s.T(), fetchResp.Topics[1].Partitions[0].RecordBatches, len(dataB0))
	require.Equal(s.T(), int64(4), fetchResp.Topics[1].Partitions[0].HighWatermark)
}

func (s *HandlersTestSuite) TestFetch_S3Failure_ReturnsError() {
	ms := s.TestDB.InitMetastore()
	seedBatch(ms, "fetch-s3fail", 0, 5, "batches/fail.batch", 61)

	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(
		(*s3.GetObjectOutput)(nil), errors.New("connection refused"),
	)

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)
	_, err := handler.Handle(createFetchRequest("fetch-s3fail", 0, 0))

	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "connection refused")
}

// --- Size limit tests ---

func (s *HandlersTestSuite) TestFetch_PartitionMaxBytes_LimitsBatches() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("fetch-plimit", 1)
	topicID := ms.GetTopicByName("fetch-plimit").ID

	// 3 batches of 16 bytes each
	data := fakeBatchData(16)
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 1, S3Key: "plimit/1.batch", ByteLength: 16},
	})
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 1, S3Key: "plimit/2.batch", ByteLength: 16},
	})
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 1, S3Key: "plimit/3.batch", ByteLength: 16},
	})

	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(mockGetObjectReturn(data), nil)

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)

	// PartitionMaxBytes=30 → first batch (16) always included, second (16+16=32) exceeds 30
	req := kmsg.NewFetchRequest()
	req.SetVersion(3)
	req.MaxBytes = math.MaxInt32
	t := kmsg.NewFetchRequestTopic()
	t.Topic = "fetch-plimit"
	p := kmsg.NewFetchRequestTopicPartition()
	p.Partition = 0
	p.FetchOffset = 0
	p.PartitionMaxBytes = 30
	t.Partitions = []kmsg.FetchRequestTopicPartition{p}
	req.Topics = []kmsg.FetchRequestTopic{t}

	resp, err := handler.Handle(&req)
	require.NoError(s.T(), err)
	records := resp.(*kmsg.FetchResponse).Topics[0].Partitions[0].RecordBatches
	require.Len(s.T(), records, 16) // only first batch
}

func (s *HandlersTestSuite) TestFetch_ServerMaxBytes_OverridesClient() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("fetch-srvlimit", 1)
	topicID := ms.GetTopicByName("fetch-srvlimit").ID

	// 3 batches of 20 bytes each
	data := fakeBatchData(20)
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 1, S3Key: "srv/1.batch", ByteLength: 20},
	})
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 1, S3Key: "srv/2.batch", ByteLength: 20},
	})
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 1, S3Key: "srv/3.batch", ByteLength: 20},
	})

	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(mockGetObjectReturn(data), nil)

	// Server max=30, client max=1000 → effective=30
	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", 30)

	req := createFetchRequest("fetch-srvlimit", 0, 0)
	req.MaxBytes = 1000

	resp, err := handler.Handle(req)
	require.NoError(s.T(), err)
	records := resp.(*kmsg.FetchResponse).Topics[0].Partitions[0].RecordBatches
	// First batch (20) always included, second (20+20=40) exceeds 30
	require.Len(s.T(), records, 20)

	// Verify only 1 S3 call was made, not 3
	mockS3.AssertNumberOfCalls(s.T(), "GetObject", 1)
}

func (s *HandlersTestSuite) TestFetch_FirstBatchAlwaysIncluded() {
	ms := s.TestDB.InitMetastore()
	batchBytes := fakeBatchData(100)
	seedBatch(ms, "fetch-forward", 0, 1, "forward/big.batch", 100)

	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(mockGetObjectReturn(batchBytes), nil)

	// All limits set to 10, but the single 100-byte batch must still be returned
	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", 10)

	req := kmsg.NewFetchRequest()
	req.SetVersion(3)
	req.MaxBytes = 10
	t := kmsg.NewFetchRequestTopic()
	t.Topic = "fetch-forward"
	p := kmsg.NewFetchRequestTopicPartition()
	p.Partition = 0
	p.FetchOffset = 0
	p.PartitionMaxBytes = 10
	t.Partitions = []kmsg.FetchRequestTopicPartition{p}
	req.Topics = []kmsg.FetchRequestTopic{t}

	resp, err := handler.Handle(&req)
	require.NoError(s.T(), err)
	records := resp.(*kmsg.FetchResponse).Topics[0].Partitions[0].RecordBatches
	require.Len(s.T(), records, 100)
}

func (s *HandlersTestSuite) TestFetch_MaxBytes_CumulativeAcrossPartitions() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("fetch-cumul", 2)
	topicID := ms.GetTopicByName("fetch-cumul").ID

	dataP0 := fakeBatchData(30)
	dataP1a := fakeBatchData(20)
	dataP1b := fakeBatchData(20)

	// Partition 0: one 30-byte batch
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 1, S3Key: "cumul/p0.batch", ByteLength: 30},
	})
	// Partition 1: two 20-byte batches
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 1, NRecords: 1, S3Key: "cumul/p1a.batch", ByteLength: 20},
	})
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 1, NRecords: 1, S3Key: "cumul/p1b.batch", ByteLength: 20},
	})

	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(in *s3.GetObjectInput) bool {
		return *in.Key == "cumul/p0.batch"
	})).Return(mockGetObjectReturn(dataP0), nil)
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(in *s3.GetObjectInput) bool {
		return *in.Key == "cumul/p1a.batch"
	})).Return(mockGetObjectReturn(dataP1a), nil)
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(in *s3.GetObjectInput) bool {
		return *in.Key == "cumul/p1b.batch"
	})).Return(mockGetObjectReturn(dataP1b), nil)

	// MaxBytes=55: partition 0 uses 30, partition 1 gets 25 remaining budget
	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket", math.MaxInt32)

	req := kmsg.NewFetchRequest()
	req.SetVersion(3)
	req.MaxBytes = 55
	t := kmsg.NewFetchRequestTopic()
	t.Topic = "fetch-cumul"
	p0 := kmsg.NewFetchRequestTopicPartition()
	p0.Partition = 0
	p0.FetchOffset = 0
	p0.PartitionMaxBytes = math.MaxInt32
	p1 := kmsg.NewFetchRequestTopicPartition()
	p1.Partition = 1
	p1.FetchOffset = 0
	p1.PartitionMaxBytes = math.MaxInt32
	t.Partitions = []kmsg.FetchRequestTopicPartition{p0, p1}
	req.Topics = []kmsg.FetchRequestTopic{t}

	resp, err := handler.Handle(&req)
	require.NoError(s.T(), err)
	fetchResp := resp.(*kmsg.FetchResponse)

	// Partition 0: 30 bytes (first batch always included)
	require.Len(s.T(), fetchResp.Topics[0].Partitions[0].RecordBatches, 30)
	// Partition 1: first batch (20) always included, second (20+20=40) would push total to 70 > 55
	require.Len(s.T(), fetchResp.Topics[0].Partitions[1].RecordBatches, 20)
}

