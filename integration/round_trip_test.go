package integration

import (
	"context"
	"fmt"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func waitForEndOffset(t require.TestingT, ms metastore.Metastore, topic string, partition int32, expected int64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		partitions, err := ms.GetTopicPartitions(topic)
		if err == nil {
			for _, p := range partitions {
				if p.Partition == partition && p.EndOffset >= expected {
					fmt.Printf("[waitForEndOffset] %s/%d reached EndOffset=%d (wanted %d)\n", topic, partition, p.EndOffset, expected)
					return
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	require.Failf(t, "timeout", "timed out waiting for %s/%d EndOffset to reach %d", topic, partition, expected)
}

func (s *IntegrationTestsSuite) TestFullProduceFetchRoundTrip() {
	T := s.T()
	agent := s.NewAgent()

	// Step 2: Create topics
	fmt.Println("[round-trip] Step 2: Creating topics")
	adminClient := NewConfluentAdminClient()
	ctx := context.Background()
	_, err := adminClient.CreateTopics(ctx, []ckafka.TopicSpecification{
		{
			Topic:             "round-trip-a",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		{
			Topic:             "round-trip-b",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	require.NoError(T, err)
	fmt.Println("[round-trip] Topics created")

	// Step 3: Produce messages with deterministic partition routing
	fmt.Println("[round-trip] Step 3: Producing messages")
	seeds := []string{"localhost:9092"}
	producerCl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(T, err)
	defer producerCl.Close()

	// Generate unique values for each partition
	aP0Values := make([]string, 5)
	aP1Values := make([]string, 3)
	bP0Values := make([]string, 4)
	for i := range aP0Values {
		aP0Values[i] = uuid.New().String()
	}
	for i := range aP1Values {
		aP1Values[i] = uuid.New().String()
	}
	for i := range bP0Values {
		bP0Values[i] = uuid.New().String()
	}

	var records []*kgo.Record
	for _, v := range aP0Values {
		records = append(records, &kgo.Record{
			Topic:     "round-trip-a",
			Partition: 0,
			Value:     []byte(v),
		})
	}
	for _, v := range aP1Values {
		records = append(records, &kgo.Record{
			Topic:     "round-trip-a",
			Partition: 1,
			Value:     []byte(v),
		})
	}
	for _, v := range bP0Values {
		records = append(records, &kgo.Record{
			Topic:     "round-trip-b",
			Partition: 0,
			Value:     []byte(v),
		})
	}

	err = producerCl.ProduceSync(ctx, records...).FirstErr()
	require.NoError(T, err)
	fmt.Printf("[round-trip] Produced %d records successfully\n", len(records))

	// Step 4: Wait for materialization
	fmt.Println("[round-trip] Step 4: Waiting for materialization")
	waitForEndOffset(T, agent.Metastore, "round-trip-a", 0, 5, 5*time.Second)
	waitForEndOffset(T, agent.Metastore, "round-trip-a", 1, 3, 5*time.Second)
	waitForEndOffset(T, agent.Metastore, "round-trip-b", 0, 4, 5*time.Second)
	fmt.Println("[round-trip] Materialization complete")

	// Step 5: Consume all messages and verify
	fmt.Println("[round-trip] Step 5: Consuming and verifying all messages")
	pollTimeout := 10 * time.Second

	// Partition round-trip-a/0
	fmt.Println("[round-trip]   Fetching round-trip-a/0")
	fetchA0, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"round-trip-a": {0: kgo.NewOffset().At(0)},
		}),
	)
	require.NoError(T, err)
	defer fetchA0.Close()

	pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	fetchesA0 := fetchA0.PollRecords(pollCtx, 10)
	cancel()
	fmt.Printf("[round-trip]   round-trip-a/0: got %d records, %d errors\n", len(fetchesA0.Records()), len(fetchesA0.Errors()))
	require.Empty(T, fetchesA0.Errors())
	recordsA0 := fetchesA0.Records()
	require.Len(T, recordsA0, 5)
	for i, r := range recordsA0 {
		require.Equal(T, aP0Values[i], string(r.Value), "round-trip-a/0 record %d", i)
		require.Equal(T, int64(i), r.Offset, "round-trip-a/0 offset %d", i)
	}

	// Partition round-trip-a/1
	fmt.Println("[round-trip]   Fetching round-trip-a/1")
	fetchA1, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"round-trip-a": {1: kgo.NewOffset().At(0)},
		}),
	)
	require.NoError(T, err)
	defer fetchA1.Close()

	pollCtx, cancel = context.WithTimeout(ctx, pollTimeout)
	fetchesA1 := fetchA1.PollRecords(pollCtx, 10)
	cancel()
	fmt.Printf("[round-trip]   round-trip-a/1: got %d records, %d errors\n", len(fetchesA1.Records()), len(fetchesA1.Errors()))
	require.Empty(T, fetchesA1.Errors())
	recordsA1 := fetchesA1.Records()
	require.Len(T, recordsA1, 3)
	for i, r := range recordsA1 {
		require.Equal(T, aP1Values[i], string(r.Value), "round-trip-a/1 record %d", i)
		require.Equal(T, int64(i), r.Offset, "round-trip-a/1 offset %d", i)
	}

	// Partition round-trip-b/0
	fmt.Println("[round-trip]   Fetching round-trip-b/0")
	fetchB0, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"round-trip-b": {0: kgo.NewOffset().At(0)},
		}),
	)
	require.NoError(T, err)
	defer fetchB0.Close()

	pollCtx, cancel = context.WithTimeout(ctx, pollTimeout)
	fetchesB0 := fetchB0.PollRecords(pollCtx, 10)
	cancel()
	fmt.Printf("[round-trip]   round-trip-b/0: got %d records, %d errors\n", len(fetchesB0.Records()), len(fetchesB0.Errors()))
	require.Empty(T, fetchesB0.Errors())
	recordsB0 := fetchesB0.Records()
	require.Len(T, recordsB0, 4)
	for i, r := range recordsB0 {
		require.Equal(T, bP0Values[i], string(r.Value), "round-trip-b/0 record %d", i)
		require.Equal(T, int64(i), r.Offset, "round-trip-b/0 offset %d", i)
	}

	// Step 6: Commit consumer offsets
	fmt.Println("[round-trip] Step 6: Committing consumer offsets")
	commitReq := kmsg.NewOffsetCommitRequest()
	commitReq.Group = "test-group"
	topicA := kmsg.NewOffsetCommitRequestTopic()
	topicA.Topic = "round-trip-a"
	pA0 := kmsg.NewOffsetCommitRequestTopicPartition()
	pA0.Partition = 0
	pA0.Offset = 3
	pA1 := kmsg.NewOffsetCommitRequestTopicPartition()
	pA1.Partition = 1
	pA1.Offset = 2
	topicA.Partitions = append(topicA.Partitions, pA0, pA1)
	topicB := kmsg.NewOffsetCommitRequestTopic()
	topicB.Topic = "round-trip-b"
	pB0 := kmsg.NewOffsetCommitRequestTopicPartition()
	pB0.Partition = 0
	pB0.Offset = 4
	topicB.Partitions = append(topicB.Partitions, pB0)
	commitReq.Topics = append(commitReq.Topics, topicA, topicB)

	commitCl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	require.NoError(T, err)
	defer commitCl.Close()

	commitResp, err := commitCl.Request(ctx, &commitReq)
	require.NoError(T, err)
	commitResponse := commitResp.(*kmsg.OffsetCommitResponse)
	for _, t := range commitResponse.Topics {
		for _, p := range t.Partitions {
			fmt.Printf("[round-trip]   OffsetCommit %s/%d: errorCode=%d\n", t.Topic, p.Partition, p.ErrorCode)
			require.Equal(T, int16(0), p.ErrorCode, "offset commit error for %s/%d", t.Topic, p.Partition)
		}
	}
	fmt.Println("[round-trip] Offsets committed")

	// Step 7: Fetch committed offsets and verify
	fmt.Println("[round-trip] Step 7: Fetching committed offsets")
	fetchReq := kmsg.NewOffsetFetchRequest()
	fetchReq.Group = "test-group"
	fetchTopicA := kmsg.NewOffsetFetchRequestTopic()
	fetchTopicA.Topic = "round-trip-a"
	fetchTopicA.Partitions = []int32{0, 1}
	fetchTopicB := kmsg.NewOffsetFetchRequestTopic()
	fetchTopicB.Topic = "round-trip-b"
	fetchTopicB.Partitions = []int32{0}
	fetchReq.Topics = append(fetchReq.Topics, fetchTopicA, fetchTopicB)

	fetchResp, err := commitCl.Request(ctx, &fetchReq)
	require.NoError(T, err)
	fetchResponse := fetchResp.(*kmsg.OffsetFetchResponse)

	expectedOffsets := map[string]map[int32]int64{
		"round-trip-a": {0: 3, 1: 2},
		"round-trip-b": {0: 4},
	}
	for _, t := range fetchResponse.Topics {
		for _, p := range t.Partitions {
			fmt.Printf("[round-trip]   OffsetFetch %s/%d: offset=%d errorCode=%d\n", t.Topic, p.Partition, p.Offset, p.ErrorCode)
			require.Equal(T, int16(0), p.ErrorCode, "offset fetch error for %s/%d", t.Topic, p.Partition)
			require.Equal(T, expectedOffsets[t.Topic][p.Partition], p.Offset, "offset mismatch for %s/%d", t.Topic, p.Partition)
		}
	}
	fmt.Println("[round-trip] Committed offsets verified")

	// Step 8: Resume consumers from committed offsets and verify remaining records
	fmt.Println("[round-trip] Step 8: Resuming consumers from committed offsets")

	// round-trip-a/0 from offset 3 → should get 2 remaining records (indices 3, 4)
	fmt.Println("[round-trip]   Resuming round-trip-a/0 from offset 3")
	resumeA0, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"round-trip-a": {0: kgo.NewOffset().At(3)},
		}),
	)
	require.NoError(T, err)
	defer resumeA0.Close()

	pollCtx, cancel = context.WithTimeout(ctx, pollTimeout)
	resumeFetchesA0 := resumeA0.PollRecords(pollCtx, 10)
	cancel()
	fmt.Printf("[round-trip]   round-trip-a/0 resumed: got %d records, %d errors\n", len(resumeFetchesA0.Records()), len(resumeFetchesA0.Errors()))
	require.Empty(T, resumeFetchesA0.Errors())
	resumeRecordsA0 := resumeFetchesA0.Records()
	require.Len(T, resumeRecordsA0, 2)
	require.Equal(T, aP0Values[3], string(resumeRecordsA0[0].Value))
	require.Equal(T, aP0Values[4], string(resumeRecordsA0[1].Value))

	// round-trip-a/1 from offset 2 → should get 1 remaining record (index 2)
	fmt.Println("[round-trip]   Resuming round-trip-a/1 from offset 2")
	resumeA1, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"round-trip-a": {1: kgo.NewOffset().At(2)},
		}),
	)
	require.NoError(T, err)
	defer resumeA1.Close()

	pollCtx, cancel = context.WithTimeout(ctx, pollTimeout)
	resumeFetchesA1 := resumeA1.PollRecords(pollCtx, 10)
	cancel()
	fmt.Printf("[round-trip]   round-trip-a/1 resumed: got %d records, %d errors\n", len(resumeFetchesA1.Records()), len(resumeFetchesA1.Errors()))
	require.Empty(T, resumeFetchesA1.Errors())
	resumeRecordsA1 := resumeFetchesA1.Records()
	require.Len(T, resumeRecordsA1, 1)
	require.Equal(T, aP1Values[2], string(resumeRecordsA1[0].Value))

	// round-trip-b/0 from offset 4 → should get 0 records (at end)
	fmt.Println("[round-trip]   Resuming round-trip-b/0 from offset 4 (expect 0 records)")
	resumeB0, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			"round-trip-b": {0: kgo.NewOffset().At(4)},
		}),
	)
	require.NoError(T, err)
	defer resumeB0.Close()

	shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	resumeFetchesB0 := resumeB0.PollRecords(shortCtx, 10)
	cancel()
	resumeRecordsB0 := resumeFetchesB0.Records()
	fmt.Printf("[round-trip]   round-trip-b/0 resumed: got %d records\n", len(resumeRecordsB0))
	require.Len(T, resumeRecordsB0, 0)

	fmt.Println("[round-trip] DONE - all steps passed")
}
