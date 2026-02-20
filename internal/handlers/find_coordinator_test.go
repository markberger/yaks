package handlers

import (
	"github.com/markberger/yaks/internal/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *HandlersTestSuite) TestFindCoordinator_ReturnsCorrectBrokerInfo() {
	b := broker.NewBroker(1, "myhost", 9092)
	handler := NewFindCoordinatorRequestHandler(b)

	request := kmsg.NewFindCoordinatorRequest()
	request.SetVersion(2)
	request.CoordinatorKey = "my-consumer-group"
	request.CoordinatorType = 0 // group coordinator

	resp, err := handler.Handle(&request)
	require.NoError(s.T(), err)

	response := resp.(*kmsg.FindCoordinatorResponse)
	assert.Equal(s.T(), int32(1), response.NodeID)
	assert.Equal(s.T(), "myhost", response.Host)
	assert.Equal(s.T(), int32(9092), response.Port)
	assert.Equal(s.T(), int16(0), response.ErrorCode)
}

func (s *HandlersTestSuite) TestFindCoordinator_VersionHandling() {
	b := broker.NewBroker(0, "localhost", 9092)
	handler := NewFindCoordinatorRequestHandler(b)

	for _, version := range []int16{0, 1, 2} {
		request := kmsg.NewFindCoordinatorRequest()
		request.SetVersion(version)
		request.CoordinatorKey = "test-group"

		resp, err := handler.Handle(&request)
		require.NoError(s.T(), err)

		response := resp.(*kmsg.FindCoordinatorResponse)
		assert.Equal(s.T(), version, response.GetVersion())
		assert.Equal(s.T(), int32(0), response.NodeID)
		assert.Equal(s.T(), "localhost", response.Host)
		assert.Equal(s.T(), int32(9092), response.Port)
	}
}
