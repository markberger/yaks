package handlers

import (
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func makeListOffsetsRequest(topic string, partitionTimestamps map[int32]int64) *kmsg.ListOffsetsRequest {
	req := kmsg.NewListOffsetsRequest()
	req.SetVersion(1)
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	for partition, timestamp := range partitionTimestamps {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = partition
		p.Timestamp = timestamp
		t.Partitions = append(t.Partitions, p)
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{t}
	return &req
}

// seedPartitionOffset creates a topic and advances EndOffset by committing record batches.
func seedPartitionOffset(ms *metastore.GormMetastore, topic string, nPartitions int32, partition int32, nRecords int64) {
	ms.CreateTopicV2(topic, nPartitions)
	topicID := ms.GetTopicByName(topic).ID
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: partition, NRecords: nRecords, S3Key: "lo/test.batch", ByteLength: 100},
	})
}

func (s *HandlersTestSuite) TestListOffsets_Earliest() {
	ms := s.TestDB.InitMetastore()
	seedPartitionOffset(ms, "lo-earliest", 1, 0, 10)

	handler := NewListOffsetsRequestHandler(ms)
	resp, err := handler.Handle(makeListOffsetsRequest("lo-earliest", map[int32]int64{0: -2}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.ListOffsetsResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)

	p := response.Topics[0].Partitions[0]
	assert.Equal(s.T(), int16(0), p.ErrorCode)
	assert.Equal(s.T(), int64(0), p.Offset) // StartOffset
	assert.Equal(s.T(), int64(-1), p.Timestamp)
}

func (s *HandlersTestSuite) TestListOffsets_Latest() {
	ms := s.TestDB.InitMetastore()
	seedPartitionOffset(ms, "lo-latest", 1, 0, 10)

	handler := NewListOffsetsRequestHandler(ms)
	resp, err := handler.Handle(makeListOffsetsRequest("lo-latest", map[int32]int64{0: -1}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.ListOffsetsResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)

	p := response.Topics[0].Partitions[0]
	assert.Equal(s.T(), int16(0), p.ErrorCode)
	assert.Equal(s.T(), int64(10), p.Offset) // EndOffset
}

func (s *HandlersTestSuite) TestListOffsets_MaxTimestamp() {
	ms := s.TestDB.InitMetastore()
	seedPartitionOffset(ms, "lo-maxts", 1, 0, 10)

	handler := NewListOffsetsRequestHandler(ms)
	resp, err := handler.Handle(makeListOffsetsRequest("lo-maxts", map[int32]int64{0: -3}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.ListOffsetsResponse)
	p := response.Topics[0].Partitions[0]
	assert.Equal(s.T(), int16(0), p.ErrorCode)
	assert.Equal(s.T(), int64(10), p.Offset) // EndOffset
}

func (s *HandlersTestSuite) TestListOffsets_PositiveTimestamp() {
	ms := s.TestDB.InitMetastore()
	seedPartitionOffset(ms, "lo-poststamp", 1, 0, 10)

	handler := NewListOffsetsRequestHandler(ms)
	resp, err := handler.Handle(makeListOffsetsRequest("lo-poststamp", map[int32]int64{0: 1000}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.ListOffsetsResponse)
	p := response.Topics[0].Partitions[0]
	assert.Equal(s.T(), int16(0), p.ErrorCode)
	assert.Equal(s.T(), int64(10), p.Offset) // best-effort: EndOffset
}

func (s *HandlersTestSuite) TestListOffsets_UnknownTopic() {
	ms := s.TestDB.InitMetastore()

	handler := NewListOffsetsRequestHandler(ms)
	resp, err := handler.Handle(makeListOffsetsRequest("nonexistent", map[int32]int64{0: -1}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.ListOffsetsResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	assert.Equal(s.T(), kerr.UnknownTopicOrPartition.Code, response.Topics[0].Partitions[0].ErrorCode)
}

func (s *HandlersTestSuite) TestListOffsets_UnknownPartition() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("lo-unkpart", 1)

	handler := NewListOffsetsRequestHandler(ms)
	resp, err := handler.Handle(makeListOffsetsRequest("lo-unkpart", map[int32]int64{99: -1}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.ListOffsetsResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	assert.Equal(s.T(), kerr.UnknownTopicOrPartition.Code, response.Topics[0].Partitions[0].ErrorCode)
}

func (s *HandlersTestSuite) TestListOffsets_MultiplePartitions() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("lo-multi", 3)
	topicID := ms.GetTopicByName("lo-multi").ID
	ms.CommitRecordBatchesV2([]metastore.RecordBatchV2{
		{TopicID: topicID, Partition: 0, NRecords: 5, S3Key: "lo/m0.batch", ByteLength: 100},
		{TopicID: topicID, Partition: 1, NRecords: 10, S3Key: "lo/m1.batch", ByteLength: 100},
		{TopicID: topicID, Partition: 2, NRecords: 15, S3Key: "lo/m2.batch", ByteLength: 100},
	})

	handler := NewListOffsetsRequestHandler(ms)
	resp, err := handler.Handle(makeListOffsetsRequest("lo-multi", map[int32]int64{0: -1, 1: -1, 2: -1}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.ListOffsetsResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 3)

	offsets := make(map[int32]int64)
	for _, p := range response.Topics[0].Partitions {
		assert.Equal(s.T(), int16(0), p.ErrorCode)
		offsets[p.Partition] = p.Offset
	}
	assert.Equal(s.T(), int64(5), offsets[0])
	assert.Equal(s.T(), int64(10), offsets[1])
	assert.Equal(s.T(), int64(15), offsets[2])
}

func (s *HandlersTestSuite) TestListOffsets_EmptyPartition() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("lo-empty", 1)

	handler := NewListOffsetsRequestHandler(ms)
	resp, err := handler.Handle(makeListOffsetsRequest("lo-empty", map[int32]int64{0: -2}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.ListOffsetsResponse)
	p := response.Topics[0].Partitions[0]
	assert.Equal(s.T(), int16(0), p.ErrorCode)
	assert.Equal(s.T(), int64(0), p.Offset) // StartOffset=0 for empty partition
}
