package handlers

import (
	"errors"

	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *HandlersTestSuite) TestCreateTopicsHandlerSuccess() {
	metastore := s.TestDB.InitMetastore()
	handler := NewCreateTopicsRequestHandler(metastore)

	// When the handler receives a request to create a topic but there is an error
	request := kmsg.NewCreateTopicsRequest()
	request.Topics = []kmsg.CreateTopicsRequestTopic{
		{Topic: "test-topic", NumPartitions: 1, ReplicationFactor: 1},
	}
	r, err := handler.Handle(&request)
	response, _ := r.(*kmsg.CreateTopicsResponse)

	// The response should be successful
	require.NoError(s.T(), err)
	require.Len(s.T(), response.Topics, 1)
	require.Equal(s.T(), response.Topics[0].Topic, "test-topic")
	require.Equal(s.T(), response.Topics[0].ErrorCode, int16(0))

	// And the new topic should be in the metastore
	topics, err := metastore.GetTopics()
	require.NoError(s.T(), err)
	require.Len(s.T(), topics, 1)
	require.Equal(s.T(), topics[0].Name, "test-topic")
}

type mockMetastoreCreateTopicFails struct {
	metastore.GormMetastore
}

func (*mockMetastoreCreateTopicFails) CreateTopic(name string) error {
	return errors.New("create topic failed")
}

func (s *HandlersTestSuite) TestCreateTopicsHandlerFails() {
	wrappedMetastore := s.TestDB.InitMetastore()
	metastore := &mockMetastoreCreateTopicFails{*wrappedMetastore}
	handler := NewCreateTopicsRequestHandler(metastore)

	// When the handler receives a request to create a topic but there is an error
	request := kmsg.NewCreateTopicsRequest()
	request.Topics = []kmsg.CreateTopicsRequestTopic{
		{Topic: "test-topic", NumPartitions: 1, ReplicationFactor: 1},
	}
	r, err := handler.Handle(&request)
	response, _ := r.(*kmsg.CreateTopicsResponse)

	// The handler should return a KafkaStorageError
	require.NoError(s.T(), err)
	require.Len(s.T(), response.Topics, 1)
	require.Equal(s.T(), response.Topics[0].ErrorCode, kerr.KafkaStorageError.Code)
}
