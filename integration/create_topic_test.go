package integration

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"
)

func TestCreateTopic(t *testing.T) {
	NewAgent(t)
	client := NewConfluentAdminClient(t)

	ctx := context.Background()
	result, err := client.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:             "test-topic",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, result[0].Topic, "test-topic")
}
