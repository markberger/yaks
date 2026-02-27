package oracle

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func consumeAll(_ context.Context, cfg Config, expectedCounts map[PartKey]int64) (map[PartKey][]ConsumedRecord, error) {
	allConsumed := make(map[PartKey][]ConsumedRecord)
	for pk, count := range expectedCounts {
		records, err := consumePartition(cfg, pk.Topic, pk.Partition, int(count))
		if err != nil {
			return nil, fmt.Errorf("consume %s/%d: %w", pk.Topic, pk.Partition, err)
		}
		allConsumed[pk] = records
		cfg.Logger("[oracle]   %s/%d: consumed %d records", pk.Topic, pk.Partition, len(records))
	}
	return allConsumed, nil
}

func consumePartition(cfg Config, topic string, partition int32, expected int) ([]ConsumedRecord, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partition: kgo.NewOffset().At(0)},
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	defer cl.Close()

	var records []ConsumedRecord
	deadline := time.Now().Add(cfg.ConsumeTimeout)
	pollNum := 0
	for len(records) < expected && time.Now().Before(deadline) {
		pollNum++
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		fetches := cl.PollRecords(ctx, expected-len(records))
		cancel()

		batchRecords := fetches.Records()
		errs := fetches.Errors()
		var minOff, maxOff int64
		if len(batchRecords) > 0 {
			minOff = batchRecords[0].Offset
			maxOff = batchRecords[len(batchRecords)-1].Offset
		}
		cfg.Logger("[consumePartition] %s/%d poll #%d: got %d records (offsets %d..%d), %d errors, total so far %d/%d",
			topic, partition, pollNum, len(batchRecords), minOff, maxOff, len(errs), len(records)+len(batchRecords), expected)
		for _, e := range errs {
			cfg.Logger("[consumePartition] %s/%d poll #%d error: %v", topic, partition, pollNum, e)
		}

		for _, r := range batchRecords {
			records = append(records, ConsumedRecord{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Key:       r.Key,
				Value:     r.Value,
			})
		}
	}
	if len(records) < expected {
		return records, fmt.Errorf("%s/%d: timeout after %d polls, got %d/%d records",
			topic, partition, pollNum, len(records), expected)
	}
	return records, nil
}
