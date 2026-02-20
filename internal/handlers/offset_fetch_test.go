package handlers

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func makeOffsetFetchRequest(group, topic string, partitions []int32) *kmsg.OffsetFetchRequest {
	req := kmsg.NewOffsetFetchRequest()
	req.SetVersion(4)
	req.Group = group
	t := kmsg.NewOffsetFetchRequestTopic()
	t.Topic = topic
	t.Partitions = partitions
	req.Topics = []kmsg.OffsetFetchRequestTopic{t}
	return &req
}

func (s *HandlersTestSuite) TestOffsetFetch_Success() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("of-success", 1)
	topic := ms.GetTopicByName("of-success")

	// Commit via metastore directly
	ms.CommitOffset("group-1", topic.ID, 0, 42, "some-meta")

	handler := NewOffsetFetchRequestHandler(ms)
	resp, err := handler.Handle(makeOffsetFetchRequest("group-1", "of-success", []int32{0}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetFetchResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)

	p := response.Topics[0].Partitions[0]
	assert.Equal(s.T(), int64(42), p.Offset)
	assert.Equal(s.T(), "some-meta", *p.Metadata)
	assert.Equal(s.T(), int16(0), p.ErrorCode)
}

func (s *HandlersTestSuite) TestOffsetFetch_NoCommittedOffset() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("of-no-commit", 1)

	handler := NewOffsetFetchRequestHandler(ms)
	resp, err := handler.Handle(makeOffsetFetchRequest("group-1", "of-no-commit", []int32{0}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetFetchResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)

	p := response.Topics[0].Partitions[0]
	assert.Equal(s.T(), int64(-1), p.Offset)
	assert.Equal(s.T(), int16(0), p.ErrorCode)
}

func (s *HandlersTestSuite) TestOffsetFetch_UnknownTopic() {
	ms := s.TestDB.InitMetastore()

	handler := NewOffsetFetchRequestHandler(ms)
	resp, err := handler.Handle(makeOffsetFetchRequest("group-1", "nonexistent", []int32{0}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetFetchResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)

	p := response.Topics[0].Partitions[0]
	assert.Equal(s.T(), int64(-1), p.Offset)
	assert.Equal(s.T(), kerr.UnknownTopicOrPartition.Code, p.ErrorCode)
}

func (s *HandlersTestSuite) TestOffsetFetch_MultiplePartitions() {
	ms := s.TestDB.InitMetastore()
	ms.CreateTopicV2("of-multi-part", 3)
	topic := ms.GetTopicByName("of-multi-part")

	ms.CommitOffset("group-1", topic.ID, 0, 10, "")
	ms.CommitOffset("group-1", topic.ID, 1, 20, "")
	ms.CommitOffset("group-1", topic.ID, 2, 30, "")

	handler := NewOffsetFetchRequestHandler(ms)
	resp, err := handler.Handle(makeOffsetFetchRequest("group-1", "of-multi-part", []int32{0, 1, 2}))

	require.NoError(s.T(), err)
	response := resp.(*kmsg.OffsetFetchResponse)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 3)

	offsets := make(map[int32]int64)
	for _, p := range response.Topics[0].Partitions {
		assert.Equal(s.T(), int16(0), p.ErrorCode)
		offsets[p.Partition] = p.Offset
	}
	assert.Equal(s.T(), int64(10), offsets[0])
	assert.Equal(s.T(), int64(20), offsets[1])
	assert.Equal(s.T(), int64(30), offsets[2])
}
