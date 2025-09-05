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
		batches, _ := ms.GetRecordBatchesV2(topic.Name, 0)
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

//
// CommitRecordBatchesV2 Benchmarks
//

// CommitBenchmarkConfig holds configuration for CommitRecordBatchesV2 benchmark scenarios
type CommitBenchmarkConfig struct {
	Name                string
	NumTopics           int
	PartitionsPerTopic  int32
	BatchesPerCommit    int
	MinRecordsPerBatch  int64
	MaxRecordsPerBatch  int64
	NumWriters          int
	WithExistingOffsets bool
	CommitsPerWriter    int
	PartitionStrategy   string // "concentrated", "distributed", "random"
}

// Predefined CommitRecordBatchesV2 benchmark scenarios
var (
	SmallCommitScenario = CommitBenchmarkConfig{
		Name:                "SmallCommit",
		NumTopics:           2,
		PartitionsPerTopic:  2,
		BatchesPerCommit:    10,
		MinRecordsPerBatch:  10,
		MaxRecordsPerBatch:  100,
		NumWriters:          1,
		WithExistingOffsets: false,
		CommitsPerWriter:    50,
		PartitionStrategy:   "distributed",
	}

	MediumCommitScenario = CommitBenchmarkConfig{
		Name:                "MediumCommit",
		NumTopics:           3,
		PartitionsPerTopic:  4,
		BatchesPerCommit:    50,
		MinRecordsPerBatch:  50,
		MaxRecordsPerBatch:  500,
		NumWriters:          1,
		WithExistingOffsets: false,
		CommitsPerWriter:    100,
		PartitionStrategy:   "distributed",
	}

	LargeCommitScenario = CommitBenchmarkConfig{
		Name:                "LargeCommit",
		NumTopics:           5,
		PartitionsPerTopic:  8,
		BatchesPerCommit:    200,
		MinRecordsPerBatch:  100,
		MaxRecordsPerBatch:  1000,
		NumWriters:          1,
		WithExistingOffsets: false,
		CommitsPerWriter:    50,
		PartitionStrategy:   "distributed",
	}

	HighContentionCommitScenario = CommitBenchmarkConfig{
		Name:                "HighContentionCommit",
		NumTopics:           2,
		PartitionsPerTopic:  2,
		BatchesPerCommit:    25,
		MinRecordsPerBatch:  10,
		MaxRecordsPerBatch:  50,
		NumWriters:          50,
		WithExistingOffsets: false,
		CommitsPerWriter:    100,
		PartitionStrategy:   "concentrated", // All writers target same partitions
	}

	LowContentionCommitScenario = CommitBenchmarkConfig{
		Name:                "LowContentionCommit",
		NumTopics:           4,
		PartitionsPerTopic:  8,
		BatchesPerCommit:    25,
		MinRecordsPerBatch:  10,
		MaxRecordsPerBatch:  50,
		NumWriters:          4,
		WithExistingOffsets: false,
		CommitsPerWriter:    100,
		PartitionStrategy:   "distributed", // Writers spread across partitions
	}

	ExistingOffsetsCommitScenario = CommitBenchmarkConfig{
		Name:                "ExistingOffsetsCommit",
		NumTopics:           3,
		PartitionsPerTopic:  4,
		BatchesPerCommit:    30,
		MinRecordsPerBatch:  50,
		MaxRecordsPerBatch:  200,
		NumWriters:          1,
		WithExistingOffsets: true,
		CommitsPerWriter:    75,
		PartitionStrategy:   "distributed",
	}
)

// CommitBenchmarkMetrics holds performance metrics for CommitRecordBatchesV2 benchmarks
type CommitBenchmarkMetrics struct {
	TotalCommits         int64
	TotalBatches         int64
	TotalRecords         int64
	SuccessfulCommits    int64
	FailedCommits        int64
	ExecutionTimeMs      int64
	AverageCommitLatency float64
	BatchesPerSecond     float64
	RecordsPerSecond     float64
	PartitionsUpdated    int32
}

// setupCommitBenchmarkData creates test data for CommitRecordBatchesV2 benchmarking
func setupCommitBenchmarkData(ms *metastore.GormMetastore, config CommitBenchmarkConfig) ([]metastore.TopicV2, error) {
	// Create topics and partitions
	var topics []metastore.TopicV2
	for i := 0; i < config.NumTopics; i++ {
		topicName := fmt.Sprintf("commit-bench-topic-%d", i)
		err := ms.CreateTopicV2(topicName, config.PartitionsPerTopic)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic %s: %w", topicName, err)
		}

		topic := ms.GetTopicByName(topicName)
		if topic == nil {
			return nil, fmt.Errorf("failed to retrieve created topic %s", topicName)
		}
		topics = append(topics, *topic)
	}

	// Create existing offset data if requested
	if config.WithExistingOffsets {
		err := createExistingCommitData(ms, topics)
		if err != nil {
			return nil, fmt.Errorf("failed to create existing commit data: %w", err)
		}
	}

	return topics, nil
}

// createExistingCommitData creates some existing RecordBatchV2 data to simulate real-world scenarios
func createExistingCommitData(ms *metastore.GormMetastore, topics []metastore.TopicV2) error {
	for _, topic := range topics {
		// Create some existing batches for each partition
		var existingBatches []metastore.RecordBatchV2
		for p := int32(0); p < 2; p++ { // Create data for first 2 partitions
			for i := 0; i < 5; i++ {
				batch := metastore.RecordBatchV2{
					TopicID:   topic.ID,
					Partition: p,
					NRecords:  int64(100 + i*25),
					S3Key:     fmt.Sprintf("s3://existing-commit-bucket/topic-%s/partition-%d/existing-%d.parquet", topic.Name, p, i),
				}
				existingBatches = append(existingBatches, batch)
			}
		}

		// Commit existing batches
		_, err := ms.CommitRecordBatchesV2(existingBatches)
		if err != nil {
			return err
		}
	}
	return nil
}

// generateCommitBatches generates a slice of RecordBatchV2 for testing
func generateCommitBatches(topics []metastore.TopicV2, config CommitBenchmarkConfig, writerID int, commitID int, rng *rand.Rand) []metastore.RecordBatchV2 {
	batches := make([]metastore.RecordBatchV2, 0, config.BatchesPerCommit)

	for i := 0; i < config.BatchesPerCommit; i++ {
		var topic metastore.TopicV2
		var partition int32

		switch config.PartitionStrategy {
		case "concentrated":
			// All writers target the same few partitions (high contention)
			topic = topics[0]
			partition = int32(i % 2) // Only use first 2 partitions
		case "distributed":
			// Spread batches across all available topics and partitions
			topicIdx := (writerID*config.BatchesPerCommit + i) % len(topics)
			topic = topics[topicIdx]
			partition = int32((writerID*config.BatchesPerCommit + i) % int(config.PartitionsPerTopic))
		case "random":
			// Random distribution
			topic = topics[rng.Intn(len(topics))]
			partition = int32(rng.Intn(int(config.PartitionsPerTopic)))
		default:
			// Default to distributed
			topicIdx := (writerID*config.BatchesPerCommit + i) % len(topics)
			topic = topics[topicIdx]
			partition = int32((writerID*config.BatchesPerCommit + i) % int(config.PartitionsPerTopic))
		}

		// Generate record count within specified range
		recordRange := config.MaxRecordsPerBatch - config.MinRecordsPerBatch + 1
		nRecords := config.MinRecordsPerBatch + int64(rng.Intn(int(recordRange)))

		batch := metastore.RecordBatchV2{
			TopicID:   topic.ID,
			Partition: partition,
			NRecords:  nRecords,
			S3Key:     fmt.Sprintf("s3://commit-bench-bucket/topic-%s/partition-%d/writer-%d-commit-%d-batch-%d.parquet", topic.Name, partition, writerID, commitID, i),
		}
		batches = append(batches, batch)
	}

	return batches
}

// runCommitBenchmarkScenario executes a CommitRecordBatchesV2 benchmark scenario
func runCommitBenchmarkScenario(b *testing.B, config CommitBenchmarkConfig) {
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
		topics, err := setupCommitBenchmarkData(ms, config)
		require.NoError(b, err)

		b.StartTimer()

		// Execute the benchmark with concurrent writers
		start := time.Now()
		err = runConcurrentCommitWriters(ms, topics, config)
		executionTime := time.Since(start)

		b.StopTimer()

		require.NoError(b, err)

		// Collect metrics for reporting
		if i == 0 { // Report metrics only for first iteration to avoid noise
			metrics := collectCommitBenchmarkMetrics(ms, topics, config, executionTime)
			reportCommitBenchmarkMetrics(b, config, metrics)
		}

		// Close the database connection to prevent connection leaks
		sqlDB, _ := ms.GetDB().DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}
}

// runConcurrentCommitWriters runs multiple writers concurrently
func runConcurrentCommitWriters(ms *metastore.GormMetastore, topics []metastore.TopicV2, config CommitBenchmarkConfig) error {
	if config.NumWriters <= 1 {
		// Single writer case
		return runSingleCommitWriter(ms, topics, config, 0)
	}

	// Multiple writers case
	var wg sync.WaitGroup
	errChan := make(chan error, config.NumWriters)

	// Start concurrent writers
	for writerID := 0; writerID < config.NumWriters; writerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := runSingleCommitWriter(ms, topics, config, id)
			if err != nil {
				errChan <- fmt.Errorf("writer %d failed: %w", id, err)
			}
		}(writerID)
	}

	// Wait for all writers to complete
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

// runSingleCommitWriter runs a single writer that performs multiple commits
func runSingleCommitWriter(ms *metastore.GormMetastore, topics []metastore.TopicV2, config CommitBenchmarkConfig, writerID int) error {
	// Use deterministic random seed based on writer ID for reproducible results
	rng := rand.New(rand.NewSource(int64(42 + writerID)))

	for commitID := 0; commitID < config.CommitsPerWriter; commitID++ {
		// Generate batches for this commit
		batches := generateCommitBatches(topics, config, writerID, commitID, rng)

		// Commit the batches
		_, err := ms.CommitRecordBatchesV2(batches)
		if err != nil {
			return fmt.Errorf("commit %d failed: %w", commitID, err)
		}
	}

	return nil
}

// collectCommitBenchmarkMetrics gathers performance metrics after benchmark execution
func collectCommitBenchmarkMetrics(ms *metastore.GormMetastore, topics []metastore.TopicV2, config CommitBenchmarkConfig, executionTime time.Duration) CommitBenchmarkMetrics {
	metrics := CommitBenchmarkMetrics{
		ExecutionTimeMs: executionTime.Milliseconds(),
		TotalCommits:    int64(config.NumWriters * config.CommitsPerWriter),
	}

	// Count created record batches and total records
	for _, topic := range topics {
		batches, _ := ms.GetRecordBatchesV2(topic.Name, 0)
		metrics.TotalBatches += int64(len(batches))

		for _, batch := range batches {
			metrics.TotalRecords += batch.NRecords
		}

		// Count updated partitions
		partitions, _ := ms.GetTopicPartitions(topic.Name)
		for _, partition := range partitions {
			if partition.EndOffset > 0 {
				metrics.PartitionsUpdated++
			}
		}
	}

	// Calculate derived metrics
	metrics.SuccessfulCommits = metrics.TotalCommits // Assume all succeeded if we got here
	metrics.FailedCommits = 0

	if metrics.ExecutionTimeMs > 0 {
		executionSeconds := float64(metrics.ExecutionTimeMs) / 1000.0
		metrics.BatchesPerSecond = float64(metrics.TotalBatches) / executionSeconds
		metrics.RecordsPerSecond = float64(metrics.TotalRecords) / executionSeconds
		metrics.AverageCommitLatency = executionSeconds / float64(metrics.TotalCommits) * 1000.0 // ms per commit
	}

	return metrics
}

// reportCommitBenchmarkMetrics logs additional metrics beyond standard Go benchmark output
func reportCommitBenchmarkMetrics(b *testing.B, config CommitBenchmarkConfig, metrics CommitBenchmarkMetrics) {
	b.Logf("Scenario: %s", config.Name)
	b.Logf("Concurrent Writers: %d", config.NumWriters)
	b.Logf("Partition Strategy: %s", config.PartitionStrategy)
	b.Logf("Total Commits: %d", metrics.TotalCommits)
	b.Logf("Total Batches: %d", metrics.TotalBatches)
	b.Logf("Total Records: %d", metrics.TotalRecords)
	b.Logf("Partitions Updated: %d", metrics.PartitionsUpdated)
	b.Logf("Execution Time: %dms", metrics.ExecutionTimeMs)
	b.Logf("Average Commit Latency: %.2fms", metrics.AverageCommitLatency)

	if metrics.BatchesPerSecond > 0 {
		b.Logf("Batches/Second: %.2f", metrics.BatchesPerSecond)
	}
	if metrics.RecordsPerSecond > 0 {
		b.Logf("Records/Second: %.2f", metrics.RecordsPerSecond)
	}
}

// Core CommitRecordBatchesV2 benchmark functions

func BenchmarkCommitRecordBatchesV2_Small(b *testing.B) {
	runCommitBenchmarkScenario(b, SmallCommitScenario)
}

func BenchmarkCommitRecordBatchesV2_Medium(b *testing.B) {
	runCommitBenchmarkScenario(b, MediumCommitScenario)
}

func BenchmarkCommitRecordBatchesV2_Large(b *testing.B) {
	runCommitBenchmarkScenario(b, LargeCommitScenario)
}

func BenchmarkCommitRecordBatchesV2_ExistingOffsets(b *testing.B) {
	runCommitBenchmarkScenario(b, ExistingOffsetsCommitScenario)
}

// Batch size optimization benchmarks

func BenchmarkCommitRecordBatchesV2_BatchSize10(b *testing.B) {
	config := MediumCommitScenario
	config.Name = "BatchSize10"
	config.BatchesPerCommit = 10
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_BatchSize25(b *testing.B) {
	config := MediumCommitScenario
	config.Name = "BatchSize25"
	config.BatchesPerCommit = 25
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_BatchSize100(b *testing.B) {
	config := MediumCommitScenario
	config.Name = "BatchSize100"
	config.BatchesPerCommit = 100
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_BatchSize500(b *testing.B) {
	config := LargeCommitScenario
	config.Name = "BatchSize500"
	config.BatchesPerCommit = 500
	runCommitBenchmarkScenario(b, config)
}

// Concurrent writer benchmarks - Lock contention testing

func BenchmarkCommitRecordBatchesV2_Concurrent2Writers(b *testing.B) {
	config := MediumCommitScenario
	config.Name = "Concurrent2Writers"
	config.NumWriters = 2
	config.CommitsPerWriter = 150 // More commits to better test concurrency
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_Concurrent4Writers(b *testing.B) {
	config := MediumCommitScenario
	config.Name = "Concurrent4Writers"
	config.NumWriters = 4
	config.CommitsPerWriter = 150
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_Concurrent8Writers(b *testing.B) {
	config := LargeCommitScenario
	config.Name = "Concurrent8Writers"
	config.NumWriters = 8
	config.CommitsPerWriter = 100
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_Concurrent16Writers(b *testing.B) {
	config := LargeCommitScenario
	config.Name = "Concurrent16Writers"
	config.NumWriters = 16
	config.CommitsPerWriter = 50
	runCommitBenchmarkScenario(b, config)
}

// High contention scenarios

func BenchmarkCommitRecordBatchesV2_HighContention(b *testing.B) {
	runCommitBenchmarkScenario(b, HighContentionCommitScenario)
}

func BenchmarkCommitRecordBatchesV2_LowContention(b *testing.B) {
	runCommitBenchmarkScenario(b, LowContentionCommitScenario)
}

// Partition strategy comparison

func BenchmarkCommitRecordBatchesV2_ConcentratedPartitions(b *testing.B) {
	config := MediumCommitScenario
	config.Name = "ConcentratedPartitions"
	config.NumWriters = 4
	config.PartitionStrategy = "concentrated"
	config.CommitsPerWriter = 100
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_DistributedPartitions(b *testing.B) {
	config := MediumCommitScenario
	config.Name = "DistributedPartitions"
	config.NumWriters = 4
	config.PartitionStrategy = "distributed"
	config.CommitsPerWriter = 100
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_RandomPartitions(b *testing.B) {
	config := MediumCommitScenario
	config.Name = "RandomPartitions"
	config.NumWriters = 4
	config.PartitionStrategy = "random"
	config.CommitsPerWriter = 100
	runCommitBenchmarkScenario(b, config)
}

// Stress test scenarios

func BenchmarkCommitRecordBatchesV2_HighThroughputStress(b *testing.B) {
	config := CommitBenchmarkConfig{
		Name:                "HighThroughputStress",
		NumTopics:           8,
		PartitionsPerTopic:  16,
		BatchesPerCommit:    100,
		MinRecordsPerBatch:  100,
		MaxRecordsPerBatch:  1000,
		NumWriters:          8,
		WithExistingOffsets: false,
		CommitsPerWriter:    200,
		PartitionStrategy:   "distributed",
	}
	runCommitBenchmarkScenario(b, config)
}

func BenchmarkCommitRecordBatchesV2_SmallBatchHighFrequency(b *testing.B) {
	config := CommitBenchmarkConfig{
		Name:                "SmallBatchHighFrequency",
		NumTopics:           3,
		PartitionsPerTopic:  4,
		BatchesPerCommit:    5,
		MinRecordsPerBatch:  1,
		MaxRecordsPerBatch:  10,
		NumWriters:          6,
		WithExistingOffsets: false,
		CommitsPerWriter:    500,
		PartitionStrategy:   "random",
	}
	runCommitBenchmarkScenario(b, config)
}
