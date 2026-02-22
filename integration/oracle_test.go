package integration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	_ "modernc.org/sqlite"
)

const (
	NumTopics          = 1
	NumPartitions      = 1
	NumProducers       = 100
	RecordsPerProducer = 100
)

type oracleRecord struct {
	ProducerID int
	Sequence   int
	Topic      string
	Partition  int32
	Key        []byte
	Value      []byte
}

type consumedRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

type recordPayload struct {
	PID  int    `json:"pid"`
	Seq  int    `json:"seq"`
	Data string `json:"data"`
}

func openOracle(t require.TestingT, dir string) *sql.DB {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s/oracle.db", dir))
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE produced_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		producer_id INTEGER NOT NULL,
		topic TEXT NOT NULL,
		partition INTEGER NOT NULL,
		sequence INTEGER NOT NULL,
		key BLOB,
		value BLOB NOT NULL
	)`)
	require.NoError(t, err)

	return db
}

func insertOracleRecord(db *sql.DB, mu *sync.Mutex, producerID int, topic string, partition int32, seq int, key, value []byte) {
	mu.Lock()
	defer mu.Unlock()
	_, err := db.Exec(
		"INSERT INTO produced_records (producer_id, topic, partition, sequence, key, value) VALUES (?, ?, ?, ?, ?, ?)",
		producerID, topic, partition, seq, key, value,
	)
	if err != nil {
		panic(fmt.Sprintf("oracle insert failed: %v", err))
	}
}

func generateRecordValue(producerID, sequence int) []byte {
	nBytes := 32 + (sequence % 97) // 32-128 bytes of random data
	randBytes := make([]byte, nBytes)
	_, _ = rand.Read(randBytes)

	payload := recordPayload{
		PID:  producerID,
		Seq:  sequence,
		Data: hex.EncodeToString(randBytes),
	}
	b, _ := json.Marshal(payload)
	return b
}

func consumePartition(t require.TestingT, topic string, partition int32, expected int) []consumedRecord {
	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partition: kgo.NewOffset().At(0)},
		}),
	)
	require.NoError(t, err)
	defer cl.Close()

	var records []consumedRecord
	deadline := time.Now().Add(60 * time.Second)
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
		fmt.Printf("[consumePartition] %s/%d poll #%d: got %d records (offsets %d..%d), %d errors, total so far %d/%d\n",
			topic, partition, pollNum, len(batchRecords), minOff, maxOff, len(errs), len(records)+len(batchRecords), expected)
		for _, e := range errs {
			fmt.Printf("[consumePartition] %s/%d poll #%d error: %v\n", topic, partition, pollNum, e)
		}

		for _, r := range batchRecords {
			records = append(records, consumedRecord{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Key:       r.Key,
				Value:     r.Value,
			})
		}
	}
	if len(records) < expected {
		fmt.Printf("[consumePartition] %s/%d TIMEOUT: only got %d/%d records after %d polls\n",
			topic, partition, len(records), expected, pollNum)
	}
	return records
}

func (s *IntegrationTestsSuite) TestConcurrentProducerOracle() {
	T := s.T()
	agent := s.NewAgent()

	// Build topic names
	topicNames := make([]string, NumTopics)
	for i := range topicNames {
		topicNames[i] = fmt.Sprintf("oracle-topic-%d", i)
	}

	// Create topics
	fmt.Println("[oracle] Creating topics")
	adminClient := NewConfluentAdminClient()
	specs := make([]ckafka.TopicSpecification, NumTopics)
	for i, name := range topicNames {
		specs[i] = ckafka.TopicSpecification{
			Topic:             name,
			NumPartitions:     NumPartitions,
			ReplicationFactor: 1,
		}
	}
	ctx := context.Background()
	_, err := adminClient.CreateTopics(ctx, specs)
	require.NoError(T, err)
	fmt.Println("[oracle] Topics created")

	// Open SQLite oracle
	oracleDB := openOracle(T, T.TempDir())
	defer oracleDB.Close()
	var oracleMu sync.Mutex

	// Track expected counts per partition
	type partKey struct {
		topic     string
		partition int32
	}
	expectedCounts := make(map[partKey]int64)
	var countsMu sync.Mutex

	// Concurrent produce
	fmt.Printf("[oracle] Launching %d producers, %d records each\n", NumProducers, RecordsPerProducer)
	var wg sync.WaitGroup
	totalPartitions := NumTopics * NumPartitions

	for pid := 0; pid < NumProducers; pid++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			cl, err := kgo.NewClient(
				kgo.SeedBrokers("localhost:9092"),
				kgo.RecordPartitioner(kgo.ManualPartitioner()),
			)
			if err != nil {
				panic(fmt.Sprintf("producer %d: failed to create client: %v", producerID, err))
			}
			defer cl.Close()

			key := []byte(fmt.Sprintf("p%d", producerID))

			for seq := 0; seq < RecordsPerProducer; seq++ {
				// Round-robin across all partitions
				idx := seq % totalPartitions
				topic := topicNames[idx/NumPartitions]
				partition := int32(idx % NumPartitions)

				value := generateRecordValue(producerID, seq)

				// Record in oracle before producing
				insertOracleRecord(oracleDB, &oracleMu, producerID, topic, partition, seq, key, value)

				// Produce one record at a time for maximum interleaving
				record := &kgo.Record{
					Topic:     topic,
					Partition: partition,
					Key:       key,
					Value:     value,
				}
				err := cl.ProduceSync(context.Background(), record).FirstErr()
				if err != nil {
					panic(fmt.Sprintf("producer %d seq %d: produce failed: %v", producerID, seq, err))
				}

				countsMu.Lock()
				expectedCounts[partKey{topic, partition}]++
				countsMu.Unlock()
			}
			fmt.Printf("[oracle] Producer %d finished\n", producerID)
		}(pid)
	}
	wg.Wait()
	fmt.Printf("[oracle] All producers finished. Expected counts: %v\n", expectedCounts)

	// Wait for materialization
	fmt.Println("[oracle] Waiting for materialization")
	for pk, count := range expectedCounts {
		waitForEndOffset(T, agent.Metastore, pk.topic, pk.partition, count, 30*time.Second)
	}
	fmt.Println("[oracle] Materialization complete")

	// Consume all partitions
	fmt.Println("[oracle] Consuming all partitions")
	allConsumed := make(map[partKey][]consumedRecord)
	for pk, count := range expectedCounts {
		records := consumePartition(T, pk.topic, pk.partition, int(count))
		allConsumed[pk] = records
		fmt.Printf("[oracle]   %s/%d: consumed %d records\n", pk.topic, pk.partition, len(records))
	}

	// === Verification ===

	// 1. Completeness: every oracle record was consumed
	fmt.Println("[oracle] Check 1: Completeness")
	consumedValues := make(map[string]bool)
	for _, records := range allConsumed {
		for _, r := range records {
			consumedValues[string(r.Value)] = true
		}
	}
	rows, err := oracleDB.Query("SELECT producer_id, sequence, topic, partition, value FROM produced_records")
	require.NoError(T, err)
	defer rows.Close()
	var oracleCount int
	for rows.Next() {
		var pid, seq int
		var topic string
		var partition int32
		var value []byte
		require.NoError(T, rows.Scan(&pid, &seq, &topic, &partition, &value))
		require.True(T, consumedValues[string(value)],
			"record not found in consumed output: producer=%d seq=%d topic=%s partition=%d", pid, seq, topic, partition)
		oracleCount++
	}
	require.NoError(T, rows.Err())
	fmt.Printf("[oracle]   All %d oracle records found in consumed output\n", oracleCount)

	// 2. No phantom writes: consumed count matches produced count per partition
	fmt.Println("[oracle] Check 2: No phantom writes")
	for pk, count := range expectedCounts {
		require.Equal(T, int(count), len(allConsumed[pk]),
			"phantom writes detected: %s/%d expected %d got %d", pk.topic, pk.partition, count, len(allConsumed[pk]))
	}

	// 3. No duplicate offsets
	fmt.Println("[oracle] Check 3: No duplicate offsets")
	for pk, records := range allConsumed {
		offsets := make(map[int64]bool)
		for _, r := range records {
			require.False(T, offsets[r.Offset],
				"duplicate offset %d in %s/%d", r.Offset, pk.topic, pk.partition)
			offsets[r.Offset] = true
		}
	}

	// 4. Offset contiguity: offsets form [0, 1, 2, ..., N-1]
	fmt.Println("[oracle] Check 4: Offset contiguity")
	for pk, records := range allConsumed {
		offsets := make([]int64, len(records))
		for i, r := range records {
			offsets[i] = r.Offset
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		for i, o := range offsets {
			require.Equal(T, int64(i), o,
				"offset gap in %s/%d: expected %d got %d", pk.topic, pk.partition, i, o)
		}
	}

	// 5. Per-producer ordering: within each (producer, topic, partition), sequence numbers are strictly increasing
	fmt.Println("[oracle] Check 5: Per-producer ordering")
	type producerPartKey struct {
		producerID int
		topic      string
		partition  int32
	}
	producerRecords := make(map[producerPartKey][]consumedRecord)
	for pk, records := range allConsumed {
		// Sort by offset to get consumption order
		sorted := make([]consumedRecord, len(records))
		copy(sorted, records)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Offset < sorted[j].Offset })

		for _, r := range sorted {
			var payload recordPayload
			require.NoError(T, json.Unmarshal(r.Value, &payload))
			ppk := producerPartKey{payload.PID, pk.topic, pk.partition}
			producerRecords[ppk] = append(producerRecords[ppk], r)
		}
	}
	for ppk, records := range producerRecords {
		var prevSeq int = -1
		for _, r := range records {
			var payload recordPayload
			require.NoError(T, json.Unmarshal(r.Value, &payload))
			require.Greater(T, payload.Seq, prevSeq,
				"ordering violation for producer %d in %s/%d: seq %d not > prev %d",
				ppk.producerID, ppk.topic, ppk.partition, payload.Seq, prevSeq)
			prevSeq = payload.Seq
		}
	}

	// 6. Data integrity: consumed values match oracle exactly
	fmt.Println("[oracle] Check 6: Data integrity")
	oracleByKey := make(map[string][]byte) // "pid:seq" -> value
	intRows, err := oracleDB.Query("SELECT producer_id, sequence, value FROM produced_records")
	require.NoError(T, err)
	defer intRows.Close()
	for intRows.Next() {
		var pid, seq int
		var value []byte
		require.NoError(T, intRows.Scan(&pid, &seq, &value))
		oracleByKey[fmt.Sprintf("%d:%d", pid, seq)] = value
	}
	require.NoError(T, intRows.Err())

	for _, records := range allConsumed {
		for _, r := range records {
			var payload recordPayload
			require.NoError(T, json.Unmarshal(r.Value, &payload))
			key := fmt.Sprintf("%d:%d", payload.PID, payload.Seq)
			oracleValue, ok := oracleByKey[key]
			require.True(T, ok, "consumed record not found in oracle: %s", key)
			require.Equal(T, oracleValue, r.Value,
				"data integrity failure for %s: oracle and consumed values differ", key)
		}
	}

	fmt.Println("[oracle] All 6 checks passed!")
	fmt.Printf("[oracle] Total records verified: %d across %d partitions from %d producers\n",
		oracleCount, len(expectedCounts), NumProducers)
}
