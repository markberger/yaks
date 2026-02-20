package handlers

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

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

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket")
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

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket")
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

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket")
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

func (s *HandlersTestSuite) TestFetch_S3Failure_ReturnsError() {
	ms := s.TestDB.InitMetastore()
	seedBatch(ms, "fetch-s3fail", 0, 5, "batches/fail.batch", 61)

	mockS3 := &s3_client.MockS3Client{}
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(
		(*s3.GetObjectOutput)(nil), errors.New("connection refused"),
	)

	handler := NewFetchRequestHandler(ms, mockS3, "test-bucket")
	_, err := handler.Handle(createFetchRequest("fetch-s3fail", 0, 0))

	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "connection refused")
}
