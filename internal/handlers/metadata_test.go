package handlers

import (
	"sort"

	"github.com/markberger/yaks/internal/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newMetadataHandler(s *HandlersTestSuite) (*MetadataRequestHandler, *broker.Broker) {
	metastore := s.TestDB.InitMetastore()
	b := broker.NewBroker(1, "localhost", 9092, "localhost", 9092)
	return NewMetadataRequestHandler(b, metastore), b
}

func makeRequestTopic(name string) kmsg.MetadataRequestTopic {
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = &name
	return t
}

// When request.Topics is nil, all existing topics should be returned.
func (s *HandlersTestSuite) TestMetadataNilTopicsReturnsAll() {
	handler, _ := newMetadataHandler(s)
	handler.metastore.CreateTopicV2("topic-a", 1)
	handler.metastore.CreateTopicV2("topic-b", 1)
	handler.metastore.CreateTopicV2("topic-c", 1)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.Topics = nil

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	names := topicNames(response.Topics)
	sort.Strings(names)
	assert.Equal(s.T(), []string{"topic-a", "topic-b", "topic-c"}, names)
}

// When request.Topics is a non-nil empty slice, no topics should be returned.
func (s *HandlersTestSuite) TestMetadataEmptyTopicsReturnsNone() {
	handler, _ := newMetadataHandler(s)
	handler.metastore.CreateTopicV2("topic-a", 1)
	handler.metastore.CreateTopicV2("topic-b", 1)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.Topics = []kmsg.MetadataRequestTopic{}

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	assert.Empty(s.T(), response.Topics)
}

// When specific topics are requested, only those should be returned.
func (s *HandlersTestSuite) TestMetadataFilterByRequestedTopics() {
	handler, _ := newMetadataHandler(s)
	handler.metastore.CreateTopicV2("topic-a", 1)
	handler.metastore.CreateTopicV2("topic-b", 1)
	handler.metastore.CreateTopicV2("topic-c", 1)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.Topics = []kmsg.MetadataRequestTopic{
		makeRequestTopic("topic-a"),
		makeRequestTopic("topic-c"),
	}

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	names := topicNames(response.Topics)
	sort.Strings(names)
	assert.Equal(s.T(), []string{"topic-a", "topic-c"}, names)
}

// A topic with multiple partitions should have all partitions in the response.
func (s *HandlersTestSuite) TestMetadataMultiplePartitions() {
	handler, _ := newMetadataHandler(s)
	handler.metastore.CreateTopicV2("multi-part", 4)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.Topics = nil

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	require.Len(s.T(), response.Topics, 1)
	assert.Equal(s.T(), "multi-part", *response.Topics[0].Topic)
	require.Len(s.T(), response.Topics[0].Partitions, 4)

	for i := int32(0); i < 4; i++ {
		assert.Equal(s.T(), i, response.Topics[0].Partitions[i].Partition)
	}
}

// When requesting a non-existent topic without auto-creation, it should not appear.
func (s *HandlersTestSuite) TestMetadataNonExistentTopicNoAutoCreate() {
	handler, _ := newMetadataHandler(s)
	handler.metastore.CreateTopicV2("exists", 1)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.AllowAutoTopicCreation = false
	request.Topics = []kmsg.MetadataRequestTopic{
		makeRequestTopic("does-not-exist"),
	}

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	assert.Empty(s.T(), response.Topics)
}

// When requesting a non-existent topic with auto-creation enabled,
// it should be created and returned.
func (s *HandlersTestSuite) TestMetadataAutoTopicCreation() {
	handler, _ := newMetadataHandler(s)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.AllowAutoTopicCreation = true
	request.Topics = []kmsg.MetadataRequestTopic{
		makeRequestTopic("auto-created"),
	}

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	require.Len(s.T(), response.Topics, 1)
	assert.Equal(s.T(), "auto-created", *response.Topics[0].Topic)
	// Auto-created topics get 1 partition by default
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	assert.Equal(s.T(), int32(0), response.Topics[0].Partitions[0].Partition)
}

// Broker info in the response should match what was configured.
func (s *HandlersTestSuite) TestMetadataBrokerInfo() {
	handler, b := newMetadataHandler(s)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	require.Len(s.T(), response.Brokers, 1)
	assert.Equal(s.T(), b.NodeID, response.Brokers[0].NodeID)
	assert.Equal(s.T(), b.AdvertisedHost, response.Brokers[0].Host)
	assert.Equal(s.T(), b.AdvertisedPort, response.Brokers[0].Port)
	assert.Equal(s.T(), int32(0), response.ControllerID)
}

// Each partition should report the correct leader, replicas, and ISR.
func (s *HandlersTestSuite) TestMetadataPartitionLeaderAndReplicas() {
	handler, b := newMetadataHandler(s)
	handler.metastore.CreateTopicV2("leader-test", 2)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.Topics = nil

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	require.Len(s.T(), response.Topics, 1)
	for _, p := range response.Topics[0].Partitions {
		assert.Equal(s.T(), b.NodeID, p.Leader)
		assert.Equal(s.T(), []int32{b.NodeID}, p.Replicas)
		assert.Equal(s.T(), []int32{b.NodeID}, p.ISR)
		assert.Equal(s.T(), int16(0), p.ErrorCode)
	}
}

// Multiple topics with different partition counts should all be correct.
func (s *HandlersTestSuite) TestMetadataMultipleTopicsVariedPartitions() {
	handler, _ := newMetadataHandler(s)
	handler.metastore.CreateTopicV2("single", 1)
	handler.metastore.CreateTopicV2("triple", 3)
	handler.metastore.CreateTopicV2("five", 5)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.Topics = nil

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	require.Len(s.T(), response.Topics, 3)

	partitionsByTopic := make(map[string]int)
	for _, t := range response.Topics {
		partitionsByTopic[*t.Topic] = len(t.Partitions)
	}
	assert.Equal(s.T(), 1, partitionsByTopic["single"])
	assert.Equal(s.T(), 3, partitionsByTopic["triple"])
	assert.Equal(s.T(), 5, partitionsByTopic["five"])
}

// Auto-create should only create topics that don't already exist.
func (s *HandlersTestSuite) TestMetadataAutoCreateSkipsExisting() {
	handler, _ := newMetadataHandler(s)
	handler.metastore.CreateTopicV2("existing", 3)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	request.AllowAutoTopicCreation = true
	request.Topics = []kmsg.MetadataRequestTopic{
		makeRequestTopic("existing"),
		makeRequestTopic("new-topic"),
	}

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)
	response := resp.(*kmsg.MetadataResponse)

	require.Len(s.T(), response.Topics, 2)
	partitionsByTopic := make(map[string]int)
	for _, t := range response.Topics {
		partitionsByTopic[*t.Topic] = len(t.Partitions)
	}
	// Existing topic should keep its 3 partitions
	assert.Equal(s.T(), 3, partitionsByTopic["existing"])
	// Auto-created topic gets 1 partition
	assert.Equal(s.T(), 1, partitionsByTopic["new-topic"])
}

// Response version should match request version.
func (s *HandlersTestSuite) TestMetadataResponseVersion() {
	handler, _ := newMetadataHandler(s)

	for _, version := range []int16{0, 1, 4, 7} {
		request := kmsg.NewMetadataRequest()
		request.SetVersion(version)

		resp, err := handler.Handle(&request)
		require.NoError(s.T(), err)
		response := resp.(*kmsg.MetadataResponse)
		assert.Equal(s.T(), version, response.GetVersion())
	}
}

func topicNames(topics []kmsg.MetadataResponseTopic) []string {
	names := make([]string, 0, len(topics))
	for _, t := range topics {
		if t.Topic != nil {
			names = append(names, *t.Topic)
		}
	}
	return names
}
