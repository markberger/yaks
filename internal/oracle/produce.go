package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func createTopics(ctx context.Context, cfg Config, topicNames []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer cl.Close()

	req := kmsg.NewCreateTopicsRequest()
	for _, name := range topicNames {
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = name
		topic.NumPartitions = int32(cfg.NumPartitions)
		topic.ReplicationFactor = 1
		req.Topics = append(req.Topics, topic)
	}

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}

	for _, t := range resp.Topics {
		if t.ErrorCode != 0 {
			return fmt.Errorf("topic %q: error code %d", t.Topic, t.ErrorCode)
		}
	}
	return nil
}

func produce(ctx context.Context, cfg Config, topicNames []string, oracleDB *sql.DB) (map[PartKey]int64, error) {
	expectedCounts := make(map[PartKey]int64)
	var countsMu sync.Mutex
	var oracleMu sync.Mutex

	totalPartitions := cfg.NumTopics * cfg.NumPartitions

	var wg sync.WaitGroup
	errCh := make(chan error, cfg.NumProducers)

	for pid := 0; pid < cfg.NumProducers; pid++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			cl, err := kgo.NewClient(
				kgo.SeedBrokers(cfg.Brokers...),
				kgo.RecordPartitioner(kgo.ManualPartitioner()),
			)
			if err != nil {
				errCh <- fmt.Errorf("producer %d: create client: %w", producerID, err)
				return
			}
			defer cl.Close()

			key := []byte(fmt.Sprintf("p%d", producerID))

			for seq := 0; seq < cfg.RecordsPerProducer; seq++ {
				idx := seq % totalPartitions
				topic := topicNames[idx/cfg.NumPartitions]
				partition := int32(idx % cfg.NumPartitions)

				value := generateRecordValue(producerID, seq)

				if err := insertOracleRecord(oracleDB, &oracleMu, producerID, topic, partition, seq, key, value); err != nil {
					errCh <- fmt.Errorf("producer %d seq %d: oracle insert: %w", producerID, seq, err)
					return
				}

				record := &kgo.Record{
					Topic:     topic,
					Partition: partition,
					Key:       key,
					Value:     value,
				}
				if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
					errCh <- fmt.Errorf("producer %d seq %d: produce: %w", producerID, seq, err)
					return
				}

				countsMu.Lock()
				expectedCounts[PartKey{topic, partition}]++
				countsMu.Unlock()
			}
			cfg.Logger("[oracle] Producer %d finished", producerID)
		}(pid)
	}
	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return nil, err
	}

	return expectedCounts, nil
}
