package integration

import (
	"context"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *IntegrationTestsSuite) TestBufferedWriteRoundTrip() {
	T := s.T()
	agent := s.NewAgent()

	// Create topic with 2 partitions
	adminClient := NewConfluentAdminClient()
	ctx := context.Background()
	_, err := adminClient.CreateTopics(ctx, []ckafka.TopicSpecification{
		{
			Topic:             "buffered-test",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	})
	require.NoError(T, err)

	// Generate 3 UUIDs for each partition
	p0Values := make([]string, 3)
	p1Values := make([]string, 3)
	for i := 0; i < 3; i++ {
		p0Values[i] = uuid.New().String()
		p1Values[i] = uuid.New().String()
	}

	// Create kgo client with ManualPartitioner for deterministic partition assignment
	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(T, err)
	defer cl.Close()

	// Build all 6 records: 3 for partition 0, 3 for partition 1
	var records []*kgo.Record
	for _, v := range p0Values {
		records = append(records, &kgo.Record{
			Topic:     "buffered-test",
			Partition: 0,
			Value:     []byte(v),
		})
	}
	for _, v := range p1Values {
		records = append(records, &kgo.Record{
			Topic:     "buffered-test",
			Partition: 1,
			Value:     []byte(v),
		})
	}

	// Produce all records at once so they land in the same flush cycle
	if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		T.Fatalf("produce failed: %v", err)
	}

	// Materialize the buffered events
	err = agent.Metastore.MaterializeRecordBatchEvents(10)
	require.NoError(T, err)

	// Verify all events share the same S3Key (single packed object)
	events, err := agent.Metastore.GetRecordBatchEvents("buffered-test")
	require.NoError(T, err)
	require.NotEmpty(T, events)

	s3Key := events[0].S3Key
	for _, e := range events {
		require.Equal(T, s3Key, e.S3Key, "all events should share the same S3 key")
	}

	// Verify events have distinct ByteOffset values (data is packed, not overlapping)
	offsets := make(map[int64]bool)
	for _, e := range events {
		require.False(T, offsets[e.ByteOffset], "ByteOffsets should be distinct")
		offsets[e.ByteOffset] = true
	}

	// Fetch partition 0 and verify 3 records with correct UUIDs in order
	fetchCl0, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"buffered-test": {
				0: kgo.NewOffset().At(0),
			},
		}),
	)
	require.NoError(T, err)
	defer fetchCl0.Close()

	fetches0 := fetchCl0.PollRecords(ctx, 10)
	require.Empty(T, fetches0.Errors(), "fetch partition 0 should not error")
	records0 := fetches0.Records()
	require.Len(T, records0, 3, "partition 0 should have 3 records")
	for i, r := range records0 {
		require.Equal(T, p0Values[i], string(r.Value), "partition 0 record %d value mismatch", i)
	}

	// Fetch partition 1 and verify 3 records with correct UUIDs in order
	fetchCl1, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"buffered-test": {
				1: kgo.NewOffset().At(0),
			},
		}),
	)
	require.NoError(T, err)
	defer fetchCl1.Close()

	fetches1 := fetchCl1.PollRecords(ctx, 10)
	require.Empty(T, fetches1.Errors(), "fetch partition 1 should not error")
	records1 := fetches1.Records()
	require.Len(T, records1, 3, "partition 1 should have 3 records")
	for i, r := range records1 {
		require.Equal(T, p1Values[i], string(r.Value), "partition 1 record %d value mismatch", i)
	}
}
