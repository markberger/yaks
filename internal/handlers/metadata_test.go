package handlers

import (
	"testing"

	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type mockMetastore struct{}

func (m mockMetastore) CreateTopic(_ string, __ string) error { return nil }
func (m mockMetastore) GetTopics() ([]metastore.Topic, error) {
	result := []metastore.Topic{
		{
			Name:       "test-topic",
			S3BasePath: "s3://some-bucket/",
			MinOffset:  0,
			MaxOffset:  0,
		},
	}

	return result, nil
}
func (m mockMetastore) ApplyMigrations() error { return nil }

func TestMetadataRequestHandler(t *testing.T) {
	// When the handler gets a MetadataRequest
	mockBroker := broker.NewBroker(1, "localhost", 9092)
	handler := NewMetadataRequestHandler(mockBroker, mockMetastore{})
	request := kmsg.NewMetadataRequest()
	request.SetVersion(7)
	response_, err := handler.Handle(&request)

	// Handler should return a response with no error
	require.NoError(t, err)
	response, ok := response_.(*kmsg.MetadataResponse)
	require.True(t, ok, "response should be of type kmsg.MetadataResponse")
	require.NotNil(t, response)

	// Check version
	require.Equal(t, request.GetVersion(), response.GetVersion())

	// Check broker info
	require.Len(t, response.Brokers, 1)
	require.Equal(t, mockBroker.NodeID, response.Brokers[0].NodeID)
	require.Equal(t, mockBroker.Host, response.Brokers[0].Host)
	require.Equal(t, mockBroker.Port, response.Brokers[0].Port)

	// Check topics
	require.Len(t, response.Topics, 1)
	require.Equal(t, "test-topic", *response.Topics[0].Topic)
}
