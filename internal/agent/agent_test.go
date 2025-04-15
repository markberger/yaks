package agent

import (
	"context"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/require"
)

func TestCreateTopic(t *testing.T) {
	// Set up agent running on localhost:9092
	ctx, cancelFn := context.WithCancel(context.Background())
	defer func() {
		cancelFn()
		time.Sleep(time.Second)
	}()
	agent := NewTestAgent(t)
	agent.AddHandlers()
	agent.ApplyMigrations()
	go agent.ListenAndServe(ctx)

	// When admin client makes a metadata request we should have no topics
	adminClient := NewAdminClient(t)
	defer adminClient.Close()

	metadata, err := adminClient.GetMetadata(nil, true, 5000)
	require.NoError(t, err)
	require.Equal(t, len(metadata.Topics), 0)

	// Test admin client can create a topic
	result, err := adminClient.CreateTopics(ctx, []ckafka.TopicSpecification{
		{Topic: "test-topic", NumPartitions: 1, ReplicationFactor: 1},
	})
	require.NoError(t, err)
	require.Equal(t, len(result), 1)
	require.Equal(t, result[0].Error.Code(), ckafka.ErrNoError)

	// Confirm topic is present in the next metadata request

}

func NewTestAgent(t *testing.T) *Agent {
	testDB := metastore.NewTestDB(t)
	agent, err := NewAgent(testDB.Config, "localhost", 9092)
	require.NoError(t, err)

	err = agent.ApplyMigrations()
	require.NoError(t, err)
	return agent
}

func NewAdminClient(t *testing.T) *ckafka.AdminClient {
	config := &ckafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	adminClient, err := ckafka.NewAdminClient(config)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	return adminClient
}
