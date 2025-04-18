package integration

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func (s *IntegrationTestsSuite) TestCreateTopic() {
	s.NewAgent()
	client := NewConfluentAdminClient()

	ctx := context.Background()
	result, err := client.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:             "test-topic",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), result, 1)
	require.Equal(s.T(), result[0].Topic, "test-topic")
}
