package integration

import (
	"context"
	"fmt"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *IntegrationTestsSuite) TestProduce() {
	// Setup new agent, client, and test-topic
	T := s.T()
	T.Skip("Unclear why but the confluent client sends messages in the MessageV0 format")
	s.NewAgent()
	client := NewConfluentAdminClient()

	ctx := context.Background()
	_, err := client.CreateTopics(ctx, []ckafka.TopicSpecification{
		{
			Topic:             "test-topic",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	require.NoError(T, err)

	// Produce some messages
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	require.NoError(T, err)

	// Produce messages to topic (asynchronously)
	topic := "test-topic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		producer.Produce(&ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	numOutstandingMsgs := producer.Flush(15 * 1000)
	require.Equal(T, 0, numOutstandingMsgs, "msgs failed to flush to producer")
}

func (s *IntegrationTestsSuite) TestProduceKgo() {
	// Setup new agent, client, and test-topic
	T := s.T()
	s.NewAgent()
	client := NewConfluentAdminClient()

	ctx := context.Background()
	_, err := client.CreateTopics(ctx, []ckafka.TopicSpecification{
		{
			Topic:             "test-topic",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	require.NoError(T, err)

	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx = context.Background()
	record := &kgo.Record{Topic: "test-topic", Value: []byte("bar")}
	if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
	}
}
