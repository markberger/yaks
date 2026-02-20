package handlers

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func makeOffsetCommitRequest(group, topic string, partitionOffsets map[int32]int64) *kmsg.OffsetCommitRequest {
	req := kmsg.NewOffsetCommitRequest()
	req.SetVersion(4)
	req.Group = group
	t := kmsg.NewOffsetCommitRequestTopic()
	t.Topic = topic
	for partition, offset := range partitionOffsets {
		p := kmsg.NewOffsetCommitRequestTopicPartition()
		p.Partition = partition
		p.Offset = offset
		t.Partitions = append(t.Partitions, p)
	}
	req.Topics = []kmsg.OffsetCommitRequestTopic{t}
	return &req
}

func (s *HandlersTestSuite) TestOffsetCommit_Success() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("oc-success", 1)

	handler := NewOffsetCommitRequestHandler(ms)
	resp, err := handler.Handle(makeOffsetCommitRequest("group-1", "oc-success", map[int32]int64{0: 42}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetCommitResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	assert.Equal(s.T(), int16(0), response.Topics[0].Partitions[0].ErrorCode)
}

func (s *HandlersTestSuite) TestOffsetCommit_UnknownTopic() {
	ms := s.TestDB.InitMetastore()

	handler := NewOffsetCommitRequestHandler(ms)
	resp, err := handler.Handle(makeOffsetCommitRequest("group-1", "nonexistent", map[int32]int64{0: 10}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetCommitResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	assert.Equal(s.T(), kerr.UnknownTopicOrPartition.Code, response.Topics[0].Partitions[0].ErrorCode)
}

func (s *HandlersTestSuite) TestOffsetCommit_MultipleTopicsAndPartitions() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("oc-multi-a", 2)
	ms.CreateTopicV2("oc-multi-b", 1)

	handler := NewOffsetCommitRequestHandler(ms)

	req := kmsg.NewOffsetCommitRequest()
	req.SetVersion(4)
	req.Group = "group-1"

	tA := kmsg.NewOffsetCommitRequestTopic()
	tA.Topic = "oc-multi-a"
	pA0 := kmsg.NewOffsetCommitRequestTopicPartition()
	pA0.Partition = 0
	pA0.Offset = 10
	pA1 := kmsg.NewOffsetCommitRequestTopicPartition()
	pA1.Partition = 1
	pA1.Offset = 20
	tA.Partitions = []kmsg.OffsetCommitRequestTopicPartition{pA0, pA1}

	tB := kmsg.NewOffsetCommitRequestTopic()
	tB.Topic = "oc-multi-b"
	pB0 := kmsg.NewOffsetCommitRequestTopicPartition()
	pB0.Partition = 0
	pB0.Offset = 30
	tB.Partitions = []kmsg.OffsetCommitRequestTopicPartition{pB0}

	req.Topics = []kmsg.OffsetCommitRequestTopic{tA, tB}

	resp, err := handler.Handle(&req)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetCommitResponse)

	require.Len(s.T(), response.Topics, 2)
	for _, t := range response.Topics {
		for _, p := range t.Partitions {
			assert.Equal(s.T(), int16(0), p.ErrorCode)
		}
	}
}

func (s *HandlersTestSuite) TestOffsetCommit_WithMetadata() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("oc-meta", 1)
	topic := ms.GetTopicByName("oc-meta")

	handler := NewOffsetCommitRequestHandler(ms)

	req := kmsg.NewOffsetCommitRequest()
	req.SetVersion(4)
	req.Group = "group-1"
	t := kmsg.NewOffsetCommitRequestTopic()
	t.Topic = "oc-meta"
	p := kmsg.NewOffsetCommitRequestTopicPartition()
	p.Partition = 0
	p.Offset = 50
	meta := "my-metadata"
	p.Metadata = &meta
	t.Partitions = []kmsg.OffsetCommitRequestTopicPartition{p}
	req.Topics = []kmsg.OffsetCommitRequestTopic{t}

	resp, err := handler.Handle(&req)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetCommitResponse)
	assert.Equal(s.T(), int16(0), response.Topics[0].Partitions[0].ErrorCode)

	// Verify via metastore directly
	offset, metadata, err := ms.FetchOffset("group-1", topic.ID, 0)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(50), offset)
	assert.Equal(s.T(), "my-metadata", metadata)
}

func (s *HandlersTestSuite) TestOffsetCommit_UpsertOverwrite() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("oc-upsert", 1)
	topic := ms.GetTopicByName("oc-upsert")

	handler := NewOffsetCommitRequestHandler(ms)

	// Commit offset 5
	resp, err := handler.Handle(makeOffsetCommitRequest("group-1", "oc-upsert", map[int32]int64{0: 5}))
	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetCommitResponse)
	assert.Equal(s.T(), int16(0), response.Topics[0].Partitions[0].ErrorCode)

	// Commit offset 10 (overwrite)
	resp, err = handler.Handle(makeOffsetCommitRequest("group-1", "oc-upsert", map[int32]int64{0: 10}))
	require.NoError(s.T(), err)
	response = resp.(*kmsg.OffsetCommitResponse)
	assert.Equal(s.T(), int16(0), response.Topics[0].Partitions[0].ErrorCode)

	// Verify 10 wins
	offset, _, err := ms.FetchOffset("group-1", topic.ID, 0)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(10), offset)
}
