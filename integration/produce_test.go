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
	agent := s.NewAgent()
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

	// Confirm there are no messages in the metastore
	batches, err := agent.Metastore.GetRecordBatches("test-topic")
	require.NoError(T, err)
	require.Len(T, batches, 0)

	// Create kgo client and produce 1 message
	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	record := &kgo.Record{Topic: "test-topic", Value: []byte("bar")}
	if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
	}

	// Check metastore
	batches, err = agent.Metastore.GetRecordBatches("test-topic")
	require.NoError(T, err)
	require.Len(T, batches, 1)
	require.Equal(T, "test-topic", batches[0].Topic.Name)
	require.Equal(T, int64(0), batches[0].Topic.MinOffset)
	require.Equal(T, int64(0), batches[0].Topic.MaxOffset)
	require.Equal(T, batches[0].StartOffset, int64(0))
	require.Equal(T, batches[0].EndOffset, int64(0))

	// Check we can fetch the message
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		"test-topic": {
			0: kgo.NewOffset().At(0),
		},
	})
	fetches := cl.PollRecords(context.Background(), 1)
	if errs := fetches.Errors(); len(errs) > 0 {
		T.Errorf("Failed to fetch records: %v", errs)
	}
	records := fetches.Records()

	require.Len(T, records, 1)
	require.Equal(T, records[0].Value, []byte("bar"))
}
