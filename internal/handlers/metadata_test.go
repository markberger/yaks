package handlers

import (
	"github.com/markberger/yaks/internal/broker"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *HandlersTestSuite) TestMetadataRequestHandler() {
	metastore := s.TestDB.InitMetastore()
	metastore.CreateTopic("test-topic")
	broker := broker.NewBroker(1, "localhost", 9092)
	handler := NewMetadataRequestHandler(broker, metastore)

	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	response_, err := handler.Handle(&request)

	// Handler should return a response with no error
	require.NoError(s.T(), err)
	response, ok := response_.(*kmsg.MetadataResponse)
	require.True(s.T(), ok, "response should be of type kmsg.MetadataResponse")
	require.NotNil(s.T(), response)

	// Check version
	require.Equal(s.T(), request.GetVersion(), response.GetVersion())

	// Check broker info
	require.Len(s.T(), response.Brokers, 1)
	require.Equal(s.T(), broker.NodeID, response.Brokers[0].NodeID)
	require.Equal(s.T(), broker.Host, response.Brokers[0].Host)
	require.Equal(s.T(), broker.Port, response.Brokers[0].Port)

	// Check topics
	require.Len(s.T(), response.Topics, 1)
	require.Equal(s.T(), "test-topic", *response.Topics[0].Topic)
}
