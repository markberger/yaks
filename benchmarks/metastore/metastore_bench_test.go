package metastore_bench

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/require"
)

// BenchmarkConfig holds configuration for benchmark scenarios
type BenchmarkConfig struct {
	Name               string
	NumTopics          int
	PartitionsPerTopic int32
	NumEvents          int
	MinRecordsPerEvent int64
	MaxRecordsPerEvent int64
	BatchSize          int32
	WithExistingData   bool
	NumIndexers        int // Number of concurrent indexers
}

// Predefined benchmark scenarios
var (
	SmallScenario = BenchmarkConfig{
		Name:               "Small",
		NumTopics:          2,
		PartitionsPerTopic: 2,
		NumEvents:          100,
		MinRecordsPerEvent: 1,
		MaxRecordsPerEvent: 100,
		BatchSize:          50,
		WithExistingData:   false,
		NumIndexers:        1,
	}

	MediumScenario = BenchmarkConfig{
		Name:               "Medium",
		NumTopics:          5,
		PartitionsPerTopic: 4,
		NumEvents:          1000,
		MinRecordsPerEvent: 100,
		MaxRecordsPerEvent: 1000,
		BatchSize:          500,
		WithExistingData:   false,
		NumIndexers:        1,
	}

	LargeScenario = BenchmarkConfig{
		Name:               "Large",
		NumTopics:          10,
		PartitionsPerTopic: 8,
		NumEvents:          5000,
		MinRecordsPerEvent: 500,
		MaxRecordsPerEvent: 5000,
		BatchSize:          2000,
		WithExistingData:   false,
		NumIndexers:        1,
	}

	HighContentionScenario = BenchmarkConfig{
		Name:               "HighContention",
		NumTopics:          3,
		PartitionsPerTopic: 2,
		NumEvents:          2000,
		MinRecordsPerEvent: 1,
		MaxRecordsPerEvent: 10,
		BatchSize:          1000,
		WithExistingData:   false,
		NumIndexers:        1,
	}

	ExistingOffsetsScenario = BenchmarkConfig{
		Name:               "ExistingOffsets",
		NumTopics:          3,
		PartitionsPerTopic: 4,
		NumEvents:          500,
		MinRecordsPerEvent: 100,
		MaxRecordsPerEvent: 1000,
		BatchSize:          250,
		WithExistingData:   true,
		NumIndexers:        1,
	}
)

// BenchmarkMetrics holds performance metrics collected during benchmarks
type BenchmarkMetrics struct {
	EventsProcessed      int64
	PartitionsProcessed  int32
	RecordBatchesCreated int64
	ExecutionTimeMs      int64
	DatabaseOps          int64
}

// setupBenchmarkData creates test data for benchmarking
func setupBenchmarkData(ms *metastore.GormMetastore, config BenchmarkConfig) ([]metastore.TopicV2, []metastore.RecordBatchEvent, error) {
	// Create topics and partitions
	var topics []metastore.TopicV2
	for i := 0; i < config.NumTopics; i++ {
		topicName := fmt.Sprintf("bench-topic-%d", i)
		err := ms.CreateTopicV2(topicName, config.PartitionsPerTopic)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create topic %s: %w", topicName, err)
		}

		topic := ms.GetTopicByName(topicName)
		if topic == nil {
			return nil, nil, fmt.Errorf("failed to retrieve created topic %s", topicName)
		}
		topics = append(topics, *topic)
	}

	// Create existing data if requested
	if config.WithExistingData {
		err := createExistingBatchData(ms, topics)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create existing data: %w", err)
		}
	}

	// Create unprocessed record batch events
	events := make([]metastore.RecordBatchEvent, 0, config.NumEvents)

	// Use deterministic random seed for reproducible benchmarks
	rng := rand.New(rand.NewSource(42))

	// Create events with controlled distribution across topics/partitions
	for i := 0; i < config.NumEvents; i++ {
		topic := topics[i%len(topics)]
		partition := int32(rng.Intn(int(config.PartitionsPerTopic)))

		// Generate record count within specified range
		recordRange := config.MaxRecordsPerEvent - config.MinRecordsPerEvent + 1
		nRecords := config.MinRecordsPerEvent + int64(rng.Intn(int(recordRange)))

		event := metastore.RecordBatchEvent{
			BaseModel: metastore.BaseModel{
				ID:        uuid.New(),
				CreatedAt: time.Now().Add(time.Duration(i) * time.Millisecond), // Ensure ordering
			},
			TopicID:   topic.ID,
			Partition: partition,
			NRecords:  nRecords,
			S3Key:     fmt.Sprintf("s3://bench-bucket/topic-%s/partition-%d/batch-%d.parquet", topic.Name, partition, i),
			Processed: false,
		}
		events = append(events, event)
	}

	// Commit events in batches to handle large numbers of events efficiently
	commitBatchSize := 1000 // Commit in batches of 1000 events
	for i := 0; i < len(events); i += commitBatchSize {
		end := i + commitBatchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]
		err := ms.CommitRecordBatchEvents(batch)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit record batch events (batch %d-%d): %w", i, end-1, err)
		}
	}

	return topics, events, nil
}

// createExistingBatchData creates some existing RecordBatchV2 data to simulate real-world scenarios
func createExistingBatchData(ms *metastore.GormMetastore, topics []metastore.TopicV2) error {
	for _, topic := range topics {
		// Create some existing events and materialize them
		existingEvents := make([]metastore.RecordBatchEvent, 0, 10)
		for p := int32(0); p < 2; p++ { // Create data for first 2 partitions
			for i := 0; i < 5; i++ {
				event := metastore.RecordBatchEvent{
					TopicID:   topic.ID,
					Partition: p,
					NRecords:  int64(100 + i*50),
					S3Key:     fmt.Sprintf("s3://existing-bucket/topic-%s/partition-%d/existing-%d.parquet", topic.Name, p, i),
					Processed: false,
				}
				existingEvents = append(existingEvents, event)
			}
		}

		// Commit and materialize existing events
		err := ms.CommitRecordBatchEvents(existingEvents)
		if err != nil {
			return err
		}

		err = ms.MaterializeRecordBatchEvents(int32(len(existingEvents)))
		if err != nil {
			return err
		}
	}
	return nil
}

// runBenchmarkScenario executes a benchmark scenario and collects metrics
func runBenchmarkScenario(b *testing.B, config BenchmarkConfig) {
	// Setup test database once for all iterations
	testDB := metastore.NewTestDB()
	defer testDB.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create fresh metastore for each iteration
		ms := testDB.InitMetastore()

		// Setup benchmark data
		topics, events, err := setupBenchmarkData(ms, config)
		require.NoError(b, err)

		b.StartTimer()

		// Execute the function being benchmarked with concurrent indexers
		start := time.Now()
		err = runConcurrentIndexers(ms, config)
		executionTime := time.Since(start)

		b.StopTimer()

		require.NoError(b, err)

		// Collect metrics for reporting
		if i == 0 { // Report metrics only for first iteration to avoid noise
			metrics := collectBenchmarkMetrics(ms, topics, events, executionTime)
			reportBenchmarkMetrics(b, config, metrics)
		}

		// Close the database connection to prevent connection leaks
		sqlDB, _ := ms.GetDB().DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}
}

// runConcurrentIndexers runs multiple indexers concurrently
func runConcurrentIndexers(ms *metastore.GormMetastore, config BenchmarkConfig) error {
	if config.NumIndexers <= 1 {
		// Single indexer case - process all events
		for {
			err := ms.MaterializeRecordBatchEvents(config.BatchSize)
			if err != nil {
				return err
			}

			// Check if there are more unprocessed events
			hasMore, checkErr := hasUnprocessedEvents(ms)
			if checkErr != nil {
				return checkErr
			}
			if !hasMore {
				break
			}
		}
		return nil
	}

	// Multiple indexers case
	var wg sync.WaitGroup
	errChan := make(chan error, config.NumIndexers)

	// Start concurrent indexers
	for i := 0; i < config.NumIndexers; i++ {
		wg.Add(1)
		go func(indexerID int) {
			defer wg.Done()

			// Each indexer continues processing until no more events are available
			for {
				// The MaterializeRecordBatchEvents function uses FOR UPDATE SKIP LOCKED
				// so multiple indexers can safely run concurrently
				err := ms.MaterializeRecordBatchEvents(config.BatchSize)
				if err != nil {
					errChan <- fmt.Errorf("indexer %d failed: %w", indexerID, err)
					return
				}

				// Check if there are more unprocessed events
				hasMore, checkErr := hasUnprocessedEvents(ms)
				if checkErr != nil {
					errChan <- fmt.Errorf("indexer %d failed to check for more events: %w", indexerID, checkErr)
					return
				}
				if !hasMore {
					break
				}
			}
		}(i)
	}

	// Wait for all indexers to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// hasUnprocessedEvents checks if there are any unprocessed events remaining
func hasUnprocessedEvents(ms *metastore.GormMetastore) (bool, error) {
	var count int64
	err := ms.GetDB().Model(&metastore.RecordBatchEvent{}).Where("processed = false").Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// collectBenchmarkMetrics gathers performance metrics after benchmark execution
func collectBenchmarkMetrics(ms *metastore.GormMetastore, topics []metastore.TopicV2, originalEvents []metastore.RecordBatchEvent, executionTime time.Duration) BenchmarkMetrics {
	metrics := BenchmarkMetrics{
		ExecutionTimeMs: executionTime.Milliseconds(),
	}

	// Count processed events
	for _, topic := range topics {
		events, _ := ms.GetRecordBatchEvents(topic.Name)
		for _, event := range events {
			if event.Processed {
				metrics.EventsProcessed++
			}
		}

		// Count created record batches
		batches, _ := ms.GetRecordBatchV2(topic.Name, 0)
		metrics.RecordBatchesCreated += int64(len(batches))

		// Count updated partitions
		partitions, _ := ms.GetTopicPartitions(topic.Name)
		for _, partition := range partitions {
			if partition.EndOffset > 0 {
				metrics.PartitionsProcessed++
			}
		}
	}

	return metrics
}

// reportBenchmarkMetrics logs additional metrics beyond standard Go benchmark output
func reportBenchmarkMetrics(b *testing.B, config BenchmarkConfig, metrics BenchmarkMetrics) {
	b.Logf("Scenario: %s", config.Name)
	b.Logf("Concurrent Indexers: %d", config.NumIndexers)
	b.Logf("Events Processed: %d", metrics.EventsProcessed)
	b.Logf("Partitions Updated: %d", metrics.PartitionsProcessed)
	b.Logf("Record Batches Created: %d", metrics.RecordBatchesCreated)
	b.Logf("Execution Time: %dms", metrics.ExecutionTimeMs)

	if metrics.EventsProcessed > 0 {
		eventsPerSecond := float64(metrics.EventsProcessed) / (float64(metrics.ExecutionTimeMs) / 1000.0)
		b.Logf("Events/Second: %.2f", eventsPerSecond)
	}
}

// Core benchmark functions

func BenchmarkMaterializeRecordBatchEvents_Small(b *testing.B) {
	runBenchmarkScenario(b, SmallScenario)
}

func BenchmarkMaterializeRecordBatchEvents_Medium(b *testing.B) {
	runBenchmarkScenario(b, MediumScenario)
}

func BenchmarkMaterializeRecordBatchEvents_Large(b *testing.B) {
	runBenchmarkScenario(b, LargeScenario)
}

func BenchmarkMaterializeRecordBatchEvents_HighContention(b *testing.B) {
	runBenchmarkScenario(b, HighContentionScenario)
}

func BenchmarkMaterializeRecordBatchEvents_ExistingOffsets(b *testing.B) {
	runBenchmarkScenario(b, ExistingOffsetsScenario)
}

// Batch size optimization benchmarks

func BenchmarkMaterializeRecordBatchEvents_BatchSize100(b *testing.B) {
	config := MediumScenario
	config.Name = "BatchSize100"
	config.BatchSize = 100
	runBenchmarkScenario(b, config)
}

func BenchmarkMaterializeRecordBatchEvents_BatchSize500(b *testing.B) {
	config := MediumScenario
	config.Name = "BatchSize500"
	config.BatchSize = 500
	runBenchmarkScenario(b, config)
}

func BenchmarkMaterializeRecordBatchEvents_BatchSize1000(b *testing.B) {
	config := MediumScenario
	config.Name = "BatchSize1000"
	config.BatchSize = 1000
	runBenchmarkScenario(b, config)
}

func BenchmarkMaterializeRecordBatchEvents_BatchSize2000(b *testing.B) {
	config := MediumScenario
	config.Name = "BatchSize2000"
	config.BatchSize = 2000
	runBenchmarkScenario(b, config)
}

// Multi-partition stress test
func BenchmarkMaterializeRecordBatchEvents_MultiPartition(b *testing.B) {
	config := BenchmarkConfig{
		Name:               "MultiPartition",
		NumTopics:          5,
		PartitionsPerTopic: 16,
		NumEvents:          2000,
		MinRecordsPerEvent: 50,
		MaxRecordsPerEvent: 500,
		BatchSize:          1000,
		WithExistingData:   false,
		NumIndexers:        1,
	}
	runBenchmarkScenario(b, config)
}

// Window function stress test (many small events)
func BenchmarkMaterializeRecordBatchEvents_WindowFunctionStress(b *testing.B) {
	config := BenchmarkConfig{
		Name:               "WindowFunctionStress",
		NumTopics:          2,
		PartitionsPerTopic: 4,
		NumEvents:          5000,
		MinRecordsPerEvent: 1,
		MaxRecordsPerEvent: 5,
		BatchSize:          2500,
		WithExistingData:   false,
		NumIndexers:        1,
	}
	runBenchmarkScenario(b, config)
}

// Concurrent indexer benchmarks

func BenchmarkMaterializeRecordBatchEvents_Concurrent2Indexers(b *testing.B) {
	config := MediumScenario
	config.Name = "Concurrent2Indexers"
	config.NumIndexers = 2
	config.NumEvents = 50000 // More events to better test concurrency
	runBenchmarkScenario(b, config)
}

func BenchmarkMaterializeRecordBatchEvents_Concurrent4Indexers(b *testing.B) {
	config := MediumScenario
	config.Name = "Concurrent4Indexers"
	config.NumIndexers = 4
	config.NumEvents = 50000 // More events to better test concurrency
	runBenchmarkScenario(b, config)
}

func BenchmarkMaterializeRecordBatchEvents_Concurrent8Indexers(b *testing.B) {
	config := LargeScenario
	config.Name = "Concurrent8Indexers"
	config.NumIndexers = 8
	config.NumEvents = 50000 // More events to better test concurrency
	runBenchmarkScenario(b, config)
}

// High contention with multiple indexers
func BenchmarkMaterializeRecordBatchEvents_HighContentionConcurrent(b *testing.B) {
	config := BenchmarkConfig{
		Name:               "HighContentionConcurrent",
		NumTopics:          3,
		PartitionsPerTopic: 2,
		NumEvents:          50000,
		MinRecordsPerEvent: 1,
		MaxRecordsPerEvent: 10,
		BatchSize:          500,
		WithExistingData:   false,
		NumIndexers:        4,
	}
	runBenchmarkScenario(b, config)
}

// Multi-partition with concurrent indexers
func BenchmarkMaterializeRecordBatchEvents_MultiPartitionConcurrent(b *testing.B) {
	config := BenchmarkConfig{
		Name:               "MultiPartitionConcurrent",
		NumTopics:          5,
		PartitionsPerTopic: 16,
		NumEvents:          50000,
		MinRecordsPerEvent: 50,
		MaxRecordsPerEvent: 500,
		BatchSize:          500,
		WithExistingData:   false,
		NumIndexers:        6,
	}
	runBenchmarkScenario(b, config)
}
