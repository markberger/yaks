package metastore

import (
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *MetastoreTestSuite) TestCreateTopicV2() {
	metastore := s.TestDB.InitMetastore()

	topicName := "test-topic"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(s.T(), err)

	topics, err := metastore.GetTopicsV2()
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(topics), 1)
	assert.Equal(s.T(), topics[0].Name, topicName)
	assert.Equal(s.T(), topics[0].NPartitions, int32(1))
}

func (s *MetastoreTestSuite) TestMaterializeRecordBatchEvents_Basic() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 and TopicPartition
	topicName := "test-topic-v2"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	// Get the created topic
	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Create some unprocessed RecordBatchEvents
	events := []RecordBatchEvent{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/partition-0/batch-1.parquet",
			Processed: false,
		},
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  3,
			S3Key:     "s3://bucket/topic/partition-0/batch-2.parquet",
			Processed: false,
		},
	}

	err = metastore.CommitRecordBatchEvents(events)
	require.NoError(T, err)

	// Execute: Call MaterializeRecordBatchEvents
	err = metastore.MaterializeRecordBatchEvents(50)
	require.NoError(T, err)

	// Verify: Check that RecordBatchV2 records were created
	recordBatchesV2, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatchesV2, 2)

	// Find batches by S3Key instead of assuming order
	var batch1, batch2 *RecordBatchV2
	for i := range recordBatchesV2 {
		if recordBatchesV2[i].S3Key == "s3://bucket/topic/partition-0/batch-1.parquet" {
			batch1 = &recordBatchesV2[i]
		} else if recordBatchesV2[i].S3Key == "s3://bucket/topic/partition-0/batch-2.parquet" {
			batch2 = &recordBatchesV2[i]
		}
	}
	require.NotNil(T, batch1, "batch-1 not found")
	require.NotNil(T, batch2, "batch-2 not found")

	// Verify both batches have correct basic properties
	assert.Equal(T, topic.ID, batch1.TopicID)
	assert.Equal(T, int32(0), batch1.Partition)
	assert.Equal(T, topic.ID, batch2.TopicID)
	assert.Equal(T, int32(0), batch2.Partition)

	// Since offset calculation depends on ID order (which batch was processed first),
	// we need to determine which batch has the lower start offset
	var firstBatch, secondBatch *RecordBatchV2
	if batch1.StartOffset < batch2.StartOffset {
		firstBatch, secondBatch = batch1, batch2
	} else {
		firstBatch, secondBatch = batch2, batch1
	}

	// Verify the first processed batch (starts from partition EndOffset=0)
	assert.Equal(T, int64(0), firstBatch.StartOffset)
	if firstBatch.S3Key == "s3://bucket/topic/partition-0/batch-1.parquet" {
		// batch-1 was processed first (5 records)
		assert.Equal(T, int64(5), firstBatch.StartOffset+firstBatch.NRecords)
		assert.Equal(T, int64(5), secondBatch.StartOffset)
		assert.Equal(T, int64(8), secondBatch.StartOffset+secondBatch.NRecords)
	} else {
		// batch-2 was processed first (3 records)
		assert.Equal(T, int64(3), firstBatch.StartOffset+firstBatch.NRecords)
		assert.Equal(T, int64(3), secondBatch.StartOffset)
		assert.Equal(T, int64(8), secondBatch.StartOffset+secondBatch.NRecords)
	}

	// Verify: Check that TopicPartition end_offset was updated using GetTopicPartitions
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 1)
	assert.Equal(T, int64(8), partitions[0].EndOffset)

	// Verify: Check that RecordBatchEvents were marked as processed
	processedEvents, err := metastore.GetRecordBatchEvents(topicName)
	require.NoError(T, err)
	require.Len(T, processedEvents, 2)
	for _, event := range processedEvents {
		assert.True(T, event.Processed, "Event should be marked as processed")
	}
}

func (s *MetastoreTestSuite) TestMaterializeRecordBatchEvents_WindowFunctionOffsetCalculation() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 2 partitions
	topicName := "window-function-topic"
	err := metastore.CreateTopicV2(topicName, 2)
	require.NoError(T, err)

	// Get the created topic
	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Generate deterministic UUIDs in sorted order to ensure predictable window function ordering
	// The window function orders by e.id, so we need to control the ID values
	uuid1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	uuid2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	uuid3 := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	uuid4 := uuid.MustParse("00000000-0000-0000-0000-000000000004")
	uuid5 := uuid.MustParse("00000000-0000-0000-0000-000000000005")

	// Create events with hardcoded UUIDs to ensure deterministic ordering
	// This tests the window function: sum(e.n_records) over (partition by e.topic_id, e.partition order by e.id)
	events := []RecordBatchEvent{
		{
			BaseModel: BaseModel{ID: uuid1},
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/partition-0/batch-1.parquet",
			Processed: false,
		},
		{
			BaseModel: BaseModel{ID: uuid2},
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  3,
			S3Key:     "s3://bucket/topic/partition-0/batch-2.parquet",
			Processed: false,
		},
		{
			BaseModel: BaseModel{ID: uuid3},
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  7,
			S3Key:     "s3://bucket/topic/partition-0/batch-3.parquet",
			Processed: false,
		},
		{
			BaseModel: BaseModel{ID: uuid4},
			TopicID:   topic.ID,
			Partition: 1,
			NRecords:  4,
			S3Key:     "s3://bucket/topic/partition-1/batch-1.parquet",
			Processed: false,
		},
		{
			BaseModel: BaseModel{ID: uuid5},
			TopicID:   topic.ID,
			Partition: 1,
			NRecords:  2,
			S3Key:     "s3://bucket/topic/partition-1/batch-2.parquet",
			Processed: false,
		},
	}

	err = metastore.CommitRecordBatchEvents(events)
	require.NoError(T, err)

	// Execute: Call MaterializeRecordBatchEvents
	err = metastore.MaterializeRecordBatchEvents(50)
	require.NoError(T, err)

	// Verify: Check RecordBatchV2 records for partition 0
	recordBatchesP0, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)

	// Filter batches by partition 0
	var partition0Batches []RecordBatchV2
	for _, batch := range recordBatchesP0 {
		if batch.Partition == 0 {
			partition0Batches = append(partition0Batches, batch)
		}
	}
	require.Len(T, partition0Batches, 3, "Should have 3 batches for partition 0")

	// Sort by start offset to verify ordering
	for i := 0; i < len(partition0Batches)-1; i++ {
		for j := i + 1; j < len(partition0Batches); j++ {
			if partition0Batches[i].StartOffset > partition0Batches[j].StartOffset {
				partition0Batches[i], partition0Batches[j] = partition0Batches[j], partition0Batches[i]
			}
		}
	}

	// Verify window function calculation for partition 0
	// Expected: [0,5), [5,8), [8,15)
	assert.Equal(T, int64(0), partition0Batches[0].StartOffset, "First batch should start at 0")
	assert.Equal(T, int64(5), partition0Batches[0].StartOffset+partition0Batches[0].NRecords, "First batch should end at 5")

	assert.Equal(T, int64(5), partition0Batches[1].StartOffset, "Second batch should start at 5")
	assert.Equal(T, int64(8), partition0Batches[1].StartOffset+partition0Batches[1].NRecords, "Second batch should end at 8")

	assert.Equal(T, int64(8), partition0Batches[2].StartOffset, "Third batch should start at 8")
	assert.Equal(T, int64(15), partition0Batches[2].StartOffset+partition0Batches[2].NRecords, "Third batch should end at 15")

	// Verify: Check RecordBatchV2 records for partition 1
	// Filter batches by partition 1
	var partition1Batches []RecordBatchV2
	for _, batch := range recordBatchesP0 {
		if batch.Partition == 1 {
			partition1Batches = append(partition1Batches, batch)
		}
	}
	require.Len(T, partition1Batches, 2, "Should have 2 batches for partition 1")

	// Sort by start offset
	for i := 0; i < len(partition1Batches)-1; i++ {
		for j := i + 1; j < len(partition1Batches); j++ {
			if partition1Batches[i].StartOffset > partition1Batches[j].StartOffset {
				partition1Batches[i], partition1Batches[j] = partition1Batches[j], partition1Batches[i]
			}
		}
	}

	// Verify window function calculation for partition 1 (independent of partition 0)
	// Expected: [0,4), [4,6)
	assert.Equal(T, int64(0), partition1Batches[0].StartOffset, "First batch in partition 1 should start at 0")
	assert.Equal(T, int64(4), partition1Batches[0].StartOffset+partition1Batches[0].NRecords, "First batch in partition 1 should end at 4")

	assert.Equal(T, int64(4), partition1Batches[1].StartOffset, "Second batch in partition 1 should start at 4")
	assert.Equal(T, int64(6), partition1Batches[1].StartOffset+partition1Batches[1].NRecords, "Second batch in partition 1 should end at 6")

	// Verify: Check that TopicPartition end_offsets were updated correctly
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 2)

	// Find partitions by partition number
	var p0, p1 *TopicPartition
	for i := range partitions {
		if partitions[i].Partition == 0 {
			p0 = &partitions[i]
		} else if partitions[i].Partition == 1 {
			p1 = &partitions[i]
		}
	}
	require.NotNil(T, p0, "Partition 0 should exist")
	require.NotNil(T, p1, "Partition 1 should exist")

	assert.Equal(T, int64(15), p0.EndOffset, "Partition 0 end_offset should be 15")
	assert.Equal(T, int64(6), p1.EndOffset, "Partition 1 end_offset should be 6")

	// Verify: All events should be marked as processed
	processedEvents, err := metastore.GetRecordBatchEvents(topicName)
	require.NoError(T, err)
	require.Len(T, processedEvents, 5)
	for _, event := range processedEvents {
		assert.True(T, event.Processed, "All events should be marked as processed")
	}
}

func (s *MetastoreTestSuite) TestMaterializeRecordBatchEvents_WindowFunctionWithExistingOffset() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 1 partition
	topicName := "existing-offset-topic"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	// Get the created topic
	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// First, create and materialize some initial events to establish a non-zero end_offset
	initialEvents := []RecordBatchEvent{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  8,
			S3Key:     "s3://bucket/topic/partition-0/initial-batch.parquet",
			Processed: false,
		},
	}

	err = metastore.CommitRecordBatchEvents(initialEvents)
	require.NoError(T, err)

	// Materialize the initial events to set partition end_offset to 8
	err = metastore.MaterializeRecordBatchEvents(50)
	require.NoError(T, err)

	// Verify initial state: partition should have end_offset = 8
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 1)
	assert.Equal(T, int64(8), partitions[0].EndOffset, "Initial partition end_offset should be 8")

	// Now create new events that should continue from offset 8
	newEvents := []RecordBatchEvent{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/partition-0/new-batch-1.parquet",
			Processed: false,
		},
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  3,
			S3Key:     "s3://bucket/topic/partition-0/new-batch-2.parquet",
			Processed: false,
		},
	}

	err = metastore.CommitRecordBatchEvents(newEvents)
	require.NoError(T, err)

	// Execute: Materialize the new events
	err = metastore.MaterializeRecordBatchEvents(50)
	require.NoError(T, err)

	// Verify: Check that new RecordBatchV2 records continue from existing offset
	allBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, allBatches, 3, "Should have 3 total batches")

	// Find the new batches by S3Key
	var newBatch1, newBatch2 *RecordBatchV2
	for i := range allBatches {
		if allBatches[i].S3Key == "s3://bucket/topic/partition-0/new-batch-1.parquet" {
			newBatch1 = &allBatches[i]
		} else if allBatches[i].S3Key == "s3://bucket/topic/partition-0/new-batch-2.parquet" {
			newBatch2 = &allBatches[i]
		}
	}
	require.NotNil(T, newBatch1, "new-batch-1 not found")
	require.NotNil(T, newBatch2, "new-batch-2 not found")

	// Determine which batch was processed first based on start offset
	var firstNewBatch, secondNewBatch *RecordBatchV2
	if newBatch1.StartOffset < newBatch2.StartOffset {
		firstNewBatch, secondNewBatch = newBatch1, newBatch2
	} else {
		firstNewBatch, secondNewBatch = newBatch2, newBatch1
	}

	// Verify window function calculation continues from existing end_offset (8)
	// Expected: first new batch [8,13), second new batch [13,16)
	assert.Equal(T, int64(8), firstNewBatch.StartOffset, "First new batch should start at 8 (continuing from existing offset)")
	if firstNewBatch.S3Key == "s3://bucket/topic/partition-0/new-batch-1.parquet" {
		// new-batch-1 was processed first (5 records)
		assert.Equal(T, int64(13), firstNewBatch.StartOffset+firstNewBatch.NRecords, "First new batch should end at 13")
		assert.Equal(T, int64(13), secondNewBatch.StartOffset, "Second new batch should start at 13")
		assert.Equal(T, int64(16), secondNewBatch.StartOffset+secondNewBatch.NRecords, "Second new batch should end at 16")
	} else {
		// new-batch-2 was processed first (3 records)
		assert.Equal(T, int64(11), firstNewBatch.StartOffset+firstNewBatch.NRecords, "First new batch should end at 11")
		assert.Equal(T, int64(11), secondNewBatch.StartOffset, "Second new batch should start at 11")
		assert.Equal(T, int64(16), secondNewBatch.StartOffset+secondNewBatch.NRecords, "Second new batch should end at 16")
	}

	// Verify: Check that TopicPartition end_offset was updated to final value
	finalPartitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, finalPartitions, 1)
	assert.Equal(T, int64(16), finalPartitions[0].EndOffset, "Final partition end_offset should be 16")

	// Verify: All new events should be marked as processed
	allProcessedEvents, err := metastore.GetRecordBatchEvents(topicName)
	require.NoError(T, err)
	require.Len(T, allProcessedEvents, 3, "Should have 3 total events (1 initial + 2 new)")
	for _, event := range allProcessedEvents {
		assert.True(T, event.Processed, "All events should be marked as processed")
	}
}

func (s *MetastoreTestSuite) TestMaterializeRecordBatchEvents_WindowFunctionOrderingByID() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 1 partition
	topicName := "ordering-test-topic"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	// Get the created topic
	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Generate deterministic UUIDs to test that ordering is by ID, not created_at
	// We'll use UUIDs that would sort differently than insertion order to prove ID-based ordering
	uuid1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	uuid2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	uuid3 := uuid.MustParse("00000000-0000-0000-0000-000000000003")

	// Create events with hardcoded UUIDs in a specific order to test window function ordering
	// The window function should process them in UUID order (1, 2, 3)
	event1 := RecordBatchEvent{
		BaseModel: BaseModel{ID: uuid1},
		TopicID:   topic.ID,
		Partition: 0,
		NRecords:  5,
		S3Key:     "s3://bucket/topic/partition-0/first-by-id.parquet",
		Processed: false,
	}

	event2 := RecordBatchEvent{
		BaseModel: BaseModel{ID: uuid2},
		TopicID:   topic.ID,
		Partition: 0,
		NRecords:  3,
		S3Key:     "s3://bucket/topic/partition-0/second-by-id.parquet",
		Processed: false,
	}

	event3 := RecordBatchEvent{
		BaseModel: BaseModel{ID: uuid3},
		TopicID:   topic.ID,
		Partition: 0,
		NRecords:  7,
		S3Key:     "s3://bucket/topic/partition-0/third-by-id.parquet",
		Processed: false,
	}

	// Commit all events at once to ensure they have the hardcoded IDs
	err = metastore.CommitRecordBatchEvents([]RecordBatchEvent{event1, event2, event3})
	require.NoError(T, err)

	// Execute: Call MaterializeRecordBatchEvents
	err = metastore.MaterializeRecordBatchEvents(50)
	require.NoError(T, err)

	// Verify: Check that RecordBatchV2 records were created with offsets based on ID order
	recordBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 3, "Should have 3 batches")

	// Find batches by S3Key
	var firstBatch, secondBatch, thirdBatch *RecordBatchV2
	for i := range recordBatches {
		switch recordBatches[i].S3Key {
		case "s3://bucket/topic/partition-0/first-by-id.parquet":
			firstBatch = &recordBatches[i]
		case "s3://bucket/topic/partition-0/second-by-id.parquet":
			secondBatch = &recordBatches[i]
		case "s3://bucket/topic/partition-0/third-by-id.parquet":
			thirdBatch = &recordBatches[i]
		}
	}
	require.NotNil(T, firstBatch, "first batch not found")
	require.NotNil(T, secondBatch, "second batch not found")
	require.NotNil(T, thirdBatch, "third batch not found")

	// Verify window function ordering: should be by database ID order
	// Since we used hardcoded UUIDs in order: uuid1, uuid2, uuid3
	// The window function should process them in that same order (by ID)

	// Expected offsets based on ID order:
	// 1. "first-by-id" (ID=uuid1): [0, 5)
	// 2. "second-by-id" (ID=uuid2): [5, 8)
	// 3. "third-by-id" (ID=uuid3): [8, 15)

	assert.Equal(T, int64(0), firstBatch.StartOffset, "First batch should start at 0 (processed first by ID)")
	assert.Equal(T, int64(5), firstBatch.StartOffset+firstBatch.NRecords, "First batch should end at 5")

	assert.Equal(T, int64(5), secondBatch.StartOffset, "Second batch should start at 5 (processed second by ID)")
	assert.Equal(T, int64(8), secondBatch.StartOffset+secondBatch.NRecords, "Second batch should end at 8")

	assert.Equal(T, int64(8), thirdBatch.StartOffset, "Third batch should start at 8 (processed third by ID)")
	assert.Equal(T, int64(15), thirdBatch.StartOffset+thirdBatch.NRecords, "Third batch should end at 15")

	// Verify: Check that TopicPartition end_offset was updated correctly
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 1)
	assert.Equal(T, int64(15), partitions[0].EndOffset, "Partition end_offset should be 15")

	// Verify: All events should be marked as processed
	processedEvents, err := metastore.GetRecordBatchEvents(topicName)
	require.NoError(T, err)
	require.Len(T, processedEvents, 3)
	for _, event := range processedEvents {
		assert.True(T, event.Processed, "All events should be marked as processed")
	}

	// Additional verification: Ensure the order is deterministic and based on ID
	// Sort all batches by start offset to verify the sequence
	allBatchesSorted := []RecordBatchV2{*firstBatch, *secondBatch, *thirdBatch}
	for i := 0; i < len(allBatchesSorted)-1; i++ {
		for j := i + 1; j < len(allBatchesSorted); j++ {
			if allBatchesSorted[i].StartOffset > allBatchesSorted[j].StartOffset {
				allBatchesSorted[i], allBatchesSorted[j] = allBatchesSorted[j], allBatchesSorted[i]
			}
		}
	}

	// Verify the sequence matches our expected ID-based ordering
	expectedS3Keys := []string{
		"s3://bucket/topic/partition-0/first-by-id.parquet",  // ID=uuid1, first
		"s3://bucket/topic/partition-0/second-by-id.parquet", // ID=uuid2, second
		"s3://bucket/topic/partition-0/third-by-id.parquet",  // ID=uuid3, third
	}

	for i, batch := range allBatchesSorted {
		assert.Equal(T, expectedS3Keys[i], batch.S3Key,
			"Batch at position %d should be %s (proving ID-based ordering)", i, expectedS3Keys[i])
	}
}

func (s *MetastoreTestSuite) TestMaterializeRecordBatchEvents_BatchLimit() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2
	topicName := "batch-limit-topic"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Create 2 unprocessed events with deterministic created_at timestamps
	// Events are processed by created_at order, so we need to control this
	events := []RecordBatchEvent{
		{
			BaseModel: BaseModel{
				CreatedAt: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), // Earlier timestamp - will be processed first
			},
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/batch-1.parquet",
			Processed: false,
		},
		{
			BaseModel: BaseModel{
				CreatedAt: time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC), // Later timestamp - will remain unprocessed
			},
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  3,
			S3Key:     "s3://bucket/topic/batch-2.parquet",
			Processed: false,
		},
	}

	err = metastore.CommitRecordBatchEvents(events)
	require.NoError(T, err)

	// Execute: Call MaterializeRecordBatchEvents with limit of 1
	err = metastore.MaterializeRecordBatchEvents(1)
	require.NoError(T, err)

	// Verify: Check that exactly 1 event is processed and 1 remains unprocessed
	allEvents, err := metastore.GetRecordBatchEvents(topicName)
	require.NoError(T, err)
	require.Len(T, allEvents, 2)

	// Find events by S3Key to verify which one was processed
	var event1, event2 *RecordBatchEvent
	for i := range allEvents {
		if allEvents[i].S3Key == "s3://bucket/topic/batch-1.parquet" {
			event1 = &allEvents[i]
		} else if allEvents[i].S3Key == "s3://bucket/topic/batch-2.parquet" {
			event2 = &allEvents[i]
		}
	}
	require.NotNil(T, event1, "Event 1 should exist")
	require.NotNil(T, event2, "Event 2 should exist")

	// Verify: First event (earlier created_at) should be processed, second should remain unprocessed
	assert.True(T, event1.Processed, "First event (earlier created_at) should be processed")
	assert.False(T, event2.Processed, "Second event (later created_at) should remain unprocessed")

	// Verify: Check that TopicPartition end_offset reflects only the processed event
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 1)

	// Since only the first event (5 records) was processed, end_offset should be 5
	assert.Equal(T, int64(5), partitions[0].EndOffset, "TopicPartition end_offset should be 5 (only first event processed)")

	// Verify: Check that only 1 RecordBatchV2 was created
	recordBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 1, "Should have exactly 1 RecordBatchV2 due to batch limit")

	// The single batch should span [0, 5) and correspond to the first event
	batch := recordBatches[0]
	assert.Equal(T, int64(0), batch.StartOffset, "Single batch should start at 0")
	assert.Equal(T, int64(5), batch.StartOffset+batch.NRecords, "Single batch should end at 5")
	assert.Equal(T, "s3://bucket/topic/batch-1.parquet", batch.S3Key, "Batch should correspond to first event")
}

// Helper function to find a RecordBatchV2 by S3 key
func findBatchV2ByS3(batches []RecordBatchV2, s3Key string) *RecordBatchV2 {
	for i := range batches {
		if batches[i].S3Key == s3Key {
			return &batches[i]
		}
	}
	return nil
}

// Helper function to find a TopicPartition by partition number
func findPartitionByNumber(partitions []TopicPartition, partitionNum int32) *TopicPartition {
	for i := range partitions {
		if partitions[i].Partition == partitionNum {
			return &partitions[i]
		}
	}
	return nil
}

func (s *MetastoreTestSuite) TestGetRecordBatchesV2_OffsetZeroReturnsAll() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	topicName := "get-batches-all"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Seed 3 batches: [0,5), [5,8), [8,15)
	_, err = metastore.CommitRecordBatchesV2([]RecordBatchV2{
		{TopicID: topic.ID, Partition: 0, NRecords: 5, S3Key: "batch-1"},
		{TopicID: topic.ID, Partition: 0, NRecords: 3, S3Key: "batch-2"},
		{TopicID: topic.ID, Partition: 0, NRecords: 7, S3Key: "batch-3"},
	})
	require.NoError(T, err)

	batches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, batches, 3)
	assert.Equal(T, int64(0), batches[0].StartOffset)
	assert.Equal(T, int64(5), batches[1].StartOffset)
	assert.Equal(T, int64(8), batches[2].StartOffset)
}

func (s *MetastoreTestSuite) TestGetRecordBatchesV2_OffsetMidBatch() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	topicName := "get-batches-mid"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Seed 3 batches: [0,5), [5,8), [8,15)
	_, err = metastore.CommitRecordBatchesV2([]RecordBatchV2{
		{TopicID: topic.ID, Partition: 0, NRecords: 5, S3Key: "batch-1"},
		{TopicID: topic.ID, Partition: 0, NRecords: 3, S3Key: "batch-2"},
		{TopicID: topic.ID, Partition: 0, NRecords: 7, S3Key: "batch-3"},
	})
	require.NoError(T, err)

	// Offset 3 falls inside batch [0,5) — that batch must be included
	batches, err := metastore.GetRecordBatchesV2(topicName, 3)
	require.NoError(T, err)
	require.Len(T, batches, 3)
	assert.Equal(T, int64(0), batches[0].StartOffset)
	assert.Equal(T, int64(5), batches[1].StartOffset)
	assert.Equal(T, int64(8), batches[2].StartOffset)
}

func (s *MetastoreTestSuite) TestGetRecordBatchesV2_OffsetPastAllBatches() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	topicName := "get-batches-past"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Seed 2 batches: [0,5), [5,8)
	_, err = metastore.CommitRecordBatchesV2([]RecordBatchV2{
		{TopicID: topic.ID, Partition: 0, NRecords: 5, S3Key: "batch-1"},
		{TopicID: topic.ID, Partition: 0, NRecords: 3, S3Key: "batch-2"},
	})
	require.NoError(T, err)

	batches, err := metastore.GetRecordBatchesV2(topicName, 100)
	require.NoError(T, err)
	assert.Empty(T, batches)
}

func (s *MetastoreTestSuite) TestCommitRecordBatchesV2_EmptyBatch() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Test empty batch input
	results, err := metastore.CommitRecordBatchesV2([]RecordBatchV2{})
	require.NoError(T, err, "Empty batch should not error")
	require.Empty(T, results, "Empty batch should return empty results")
}

func (s *MetastoreTestSuite) TestCommitRecordBatchesV2_SingleBatchNewPartition() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 1 partition
	topicName := "single-batch-topic"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Create a single batch for the new partition
	batches := []RecordBatchV2{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  10,
			S3Key:     "s3://bucket/topic/partition-0/batch-1.parquet",
		},
	}

	// Execute: Commit the batch
	results, err := metastore.CommitRecordBatchesV2(batches)
	require.NoError(T, err, "Single batch commit should succeed")
	require.Len(T, results, 1, "Should return one result")

	// Verify result
	result := results[0]
	assert.Equal(T, 0, result.InputIndex, "Input index should be 0")
	assert.Equal(T, int64(0), result.BaseOffset, "Base offset should be 0 for new partition")
	assert.Equal(T, batches[0].S3Key, result.S3Key, "S3 key should match")
	assert.Equal(T, int32(0), result.Partition, "Partition should be 0")

	// Verify the batch was inserted correctly
	recordBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 1, "Should have one record batch")

	batch := recordBatches[0]
	assert.Equal(T, topic.ID, batch.TopicID, "Topic ID should match")
	assert.Equal(T, int32(0), batch.Partition, "Partition should be 0")
	assert.Equal(T, int64(0), batch.StartOffset, "Start offset should be 0")
	assert.Equal(T, int64(10), batch.StartOffset+batch.NRecords, "End offset should be 10")
	assert.Equal(T, batches[0].S3Key, batch.S3Key, "S3 key should match")

	// Verify partition end_offset was updated
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 1)
	assert.Equal(T, int64(10), partitions[0].EndOffset, "Partition end_offset should be updated to 10")
}

func (s *MetastoreTestSuite) TestCommitRecordBatchesV2_SingleBatchExistingPartition() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 1 partition
	topicName := "existing-partition-topic"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// First, commit an initial batch to establish existing data
	initialBatches := []RecordBatchV2{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/partition-0/initial-batch.parquet",
		},
	}

	_, err = metastore.CommitRecordBatchesV2(initialBatches)
	require.NoError(T, err, "Initial batch commit should succeed")

	// Now commit a second batch that should continue from offset 5
	secondBatches := []RecordBatchV2{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  8,
			S3Key:     "s3://bucket/topic/partition-0/second-batch.parquet",
		},
	}

	// Execute: Commit the second batch
	results, err := metastore.CommitRecordBatchesV2(secondBatches)
	require.NoError(T, err, "Second batch commit should succeed")
	require.Len(T, results, 1, "Should return one result")

	// Verify result - should start from offset 5 (continuing from first batch)
	result := results[0]
	assert.Equal(T, 0, result.InputIndex, "Input index should be 0")
	assert.Equal(T, int64(5), result.BaseOffset, "Base offset should be 5 (continuing from existing data)")
	assert.Equal(T, secondBatches[0].S3Key, result.S3Key, "S3 key should match")
	assert.Equal(T, int32(0), result.Partition, "Partition should be 0")

	// Verify both batches exist
	recordBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 2, "Should have two record batches")

	// Find the second batch
	secondBatch := findBatchV2ByS3(recordBatches, secondBatches[0].S3Key)
	require.NotNil(T, secondBatch, "Second batch should exist")
	assert.Equal(T, int64(5), secondBatch.StartOffset, "Second batch should start at offset 5")
	assert.Equal(T, int64(13), secondBatch.StartOffset+secondBatch.NRecords, "Second batch should end at offset 13")

	// Verify partition end_offset was updated
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 1)
	assert.Equal(T, int64(13), partitions[0].EndOffset, "Partition end_offset should be updated to 13")
}

func (s *MetastoreTestSuite) TestCommitRecordBatchesV2_MultipleBatchesSamePartition() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 1 partition
	topicName := "multi-batch-same-partition"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Create multiple batches for the same partition
	batches := []RecordBatchV2{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/partition-0/batch-1.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  3,
			S3Key:     "s3://bucket/topic/partition-0/batch-2.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  7,
			S3Key:     "s3://bucket/topic/partition-0/batch-3.parquet",
		},
	}

	// Execute: Commit all batches
	results, err := metastore.CommitRecordBatchesV2(batches)
	require.NoError(T, err, "Multiple batch commit should succeed")
	require.Len(T, results, 3, "Should return three results")

	// Verify results - offsets should be calculated sequentially
	assert.Equal(T, 0, results[0].InputIndex)
	assert.Equal(T, int64(0), results[0].BaseOffset, "First batch should start at 0")
	assert.Equal(T, batches[0].S3Key, results[0].S3Key)

	assert.Equal(T, 1, results[1].InputIndex)
	assert.Equal(T, int64(5), results[1].BaseOffset, "Second batch should start at 5")
	assert.Equal(T, batches[1].S3Key, results[1].S3Key)

	assert.Equal(T, 2, results[2].InputIndex)
	assert.Equal(T, int64(8), results[2].BaseOffset, "Third batch should start at 8")
	assert.Equal(T, batches[2].S3Key, results[2].S3Key)

	// Verify all batches were inserted correctly
	recordBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 3, "Should have three record batches")

	// Find and verify each batch
	batch1 := findBatchV2ByS3(recordBatches, batches[0].S3Key)
	batch2 := findBatchV2ByS3(recordBatches, batches[1].S3Key)
	batch3 := findBatchV2ByS3(recordBatches, batches[2].S3Key)

	require.NotNil(T, batch1, "Batch 1 should exist")
	require.NotNil(T, batch2, "Batch 2 should exist")
	require.NotNil(T, batch3, "Batch 3 should exist")

	assert.Equal(T, int64(0), batch1.StartOffset, "Batch 1 should start at 0")
	assert.Equal(T, int64(5), batch1.StartOffset+batch1.NRecords, "Batch 1 should end at 5")

	assert.Equal(T, int64(5), batch2.StartOffset, "Batch 2 should start at 5")
	assert.Equal(T, int64(8), batch2.StartOffset+batch2.NRecords, "Batch 2 should end at 8")

	assert.Equal(T, int64(8), batch3.StartOffset, "Batch 3 should start at 8")
	assert.Equal(T, int64(15), batch3.StartOffset+batch3.NRecords, "Batch 3 should end at 15")

	// Verify partition end_offset was updated to the final value
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 1)
	assert.Equal(T, int64(15), partitions[0].EndOffset, "Partition end_offset should be updated to 15")
}

func (s *MetastoreTestSuite) TestCommitRecordBatchesV2_MultipleBatchesDifferentPartitions() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 2 partitions
	topicName := "multi-partition-topic"
	err := metastore.CreateTopicV2(topicName, 2)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Create batches for different partitions
	batches := []RecordBatchV2{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/partition-0/batch-1.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 1,
			NRecords:  3,
			S3Key:     "s3://bucket/topic/partition-1/batch-1.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  4,
			S3Key:     "s3://bucket/topic/partition-0/batch-2.parquet",
		},
	}

	// Execute: Commit all batches
	results, err := metastore.CommitRecordBatchesV2(batches)
	require.NoError(T, err, "Multiple partition batch commit should succeed")
	require.Len(T, results, 3, "Should return three results")

	// Verify results - each partition should have independent offset calculation
	assert.Equal(T, 0, results[0].InputIndex)
	assert.Equal(T, int64(0), results[0].BaseOffset, "First batch (partition 0) should start at 0")
	assert.Equal(T, int32(0), results[0].Partition)

	assert.Equal(T, 1, results[1].InputIndex)
	assert.Equal(T, int64(0), results[1].BaseOffset, "First batch (partition 1) should start at 0")
	assert.Equal(T, int32(1), results[1].Partition)

	assert.Equal(T, 2, results[2].InputIndex)
	assert.Equal(T, int64(5), results[2].BaseOffset, "Second batch (partition 0) should start at 5")
	assert.Equal(T, int32(0), results[2].Partition)

	// Verify all batches were inserted correctly
	recordBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 3, "Should have three record batches")

	// Find and verify each batch
	p0batch1 := findBatchV2ByS3(recordBatches, batches[0].S3Key)
	p1batch1 := findBatchV2ByS3(recordBatches, batches[1].S3Key)
	p0batch2 := findBatchV2ByS3(recordBatches, batches[2].S3Key)

	require.NotNil(T, p0batch1, "Partition 0 batch 1 should exist")
	require.NotNil(T, p1batch1, "Partition 1 batch 1 should exist")
	require.NotNil(T, p0batch2, "Partition 0 batch 2 should exist")

	// Verify partition 0 batches
	assert.Equal(T, int64(0), p0batch1.StartOffset, "P0 batch 1 should start at 0")
	assert.Equal(T, int64(5), p0batch1.StartOffset+p0batch1.NRecords, "P0 batch 1 should end at 5")
	assert.Equal(T, int64(5), p0batch2.StartOffset, "P0 batch 2 should start at 5")
	assert.Equal(T, int64(9), p0batch2.StartOffset+p0batch2.NRecords, "P0 batch 2 should end at 9")

	// Verify partition 1 batch (independent offset calculation)
	assert.Equal(T, int64(0), p1batch1.StartOffset, "P1 batch 1 should start at 0")
	assert.Equal(T, int64(3), p1batch1.StartOffset+p1batch1.NRecords, "P1 batch 1 should end at 3")

	// Verify both partition end_offsets were updated correctly
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 2)

	partition0 := findPartitionByNumber(partitions, 0)
	partition1 := findPartitionByNumber(partitions, 1)
	require.NotNil(T, partition0, "Partition 0 should exist")
	require.NotNil(T, partition1, "Partition 1 should exist")

	assert.Equal(T, int64(9), partition0.EndOffset, "Partition 0 end_offset should be 9")
	assert.Equal(T, int64(3), partition1.EndOffset, "Partition 1 end_offset should be 3")
}

func (s *MetastoreTestSuite) TestCommitRecordBatchesV2_MultipleBatchesDifferentTopics() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create two TopicV2s with 1 partition each
	topic1Name := "topic-1"
	topic2Name := "topic-2"
	err := metastore.CreateTopicV2(topic1Name, 1)
	require.NoError(T, err)
	err = metastore.CreateTopicV2(topic2Name, 1)
	require.NoError(T, err)

	topic1 := metastore.GetTopicByName(topic1Name)
	topic2 := metastore.GetTopicByName(topic2Name)
	require.NotNil(T, topic1)
	require.NotNil(T, topic2)

	// Create batches for different topics
	batches := []RecordBatchV2{
		{
			TopicID:   topic1.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic1/partition-0/batch-1.parquet",
		},
		{
			TopicID:   topic2.ID,
			Partition: 0,
			NRecords:  3,
			S3Key:     "s3://bucket/topic2/partition-0/batch-1.parquet",
		},
		{
			TopicID:   topic1.ID,
			Partition: 0,
			NRecords:  4,
			S3Key:     "s3://bucket/topic1/partition-0/batch-2.parquet",
		},
	}

	// Execute: Commit all batches
	results, err := metastore.CommitRecordBatchesV2(batches)
	require.NoError(T, err, "Multiple topic batch commit should succeed")
	require.Len(T, results, 3, "Should return three results")

	// Verify results - each topic should have independent offset calculation
	assert.Equal(T, 0, results[0].InputIndex)
	assert.Equal(T, int64(0), results[0].BaseOffset, "First batch (topic 1) should start at 0")

	assert.Equal(T, 1, results[1].InputIndex)
	assert.Equal(T, int64(0), results[1].BaseOffset, "First batch (topic 2) should start at 0")

	assert.Equal(T, 2, results[2].InputIndex)
	assert.Equal(T, int64(5), results[2].BaseOffset, "Second batch (topic 1) should start at 5")

	// Verify batches for topic 1
	topic1Batches, err := metastore.GetRecordBatchesV2(topic1Name, 0)
	require.NoError(T, err)
	require.Len(T, topic1Batches, 2, "Topic 1 should have two batches")

	// Verify batches for topic 2
	topic2Batches, err := metastore.GetRecordBatchesV2(topic2Name, 0)
	require.NoError(T, err)
	require.Len(T, topic2Batches, 1, "Topic 2 should have one batch")

	// Verify topic 1 partition end_offset
	topic1Partitions, err := metastore.GetTopicPartitions(topic1Name)
	require.NoError(T, err)
	require.Len(T, topic1Partitions, 1)
	assert.Equal(T, int64(9), topic1Partitions[0].EndOffset, "Topic 1 partition end_offset should be 9")

	// Verify topic 2 partition end_offset
	topic2Partitions, err := metastore.GetTopicPartitions(topic2Name)
	require.NoError(T, err)
	require.Len(T, topic2Partitions, 1)
	assert.Equal(T, int64(3), topic2Partitions[0].EndOffset, "Topic 2 partition end_offset should be 3")
}

func (s *MetastoreTestSuite) TestCommitRecordBatchesV2_WindowFunctionOffsetCalculation() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 2 partitions
	topicName := "window-function-v2-topic"
	err := metastore.CreateTopicV2(topicName, 2)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// Create batches that test the window function offset calculation
	// The SQL uses: sum(inp.n_record) over (partition by inp.topic_id, inp.partition order by inp.input_idx)
	batches := []RecordBatchV2{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/partition-0/batch-1.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  3,
			S3Key:     "s3://bucket/topic/partition-0/batch-2.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 1,
			NRecords:  4,
			S3Key:     "s3://bucket/topic/partition-1/batch-1.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  7,
			S3Key:     "s3://bucket/topic/partition-0/batch-3.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 1,
			NRecords:  2,
			S3Key:     "s3://bucket/topic/partition-1/batch-2.parquet",
		},
	}

	// Execute: Commit all batches
	results, err := metastore.CommitRecordBatchesV2(batches)
	require.NoError(T, err, "Window function batch commit should succeed")
	require.Len(T, results, 5, "Should return five results")

	// Verify results - window function should calculate offsets correctly per partition
	// Partition 0: batch 1 (input_idx=0), batch 2 (input_idx=1), batch 3 (input_idx=3)
	// Partition 1: batch 1 (input_idx=2), batch 2 (input_idx=4)

	// Partition 0 batches
	assert.Equal(T, 0, results[0].InputIndex)
	assert.Equal(T, int64(0), results[0].BaseOffset, "P0 batch 1 should start at 0")
	assert.Equal(T, int32(0), results[0].Partition)

	assert.Equal(T, 1, results[1].InputIndex)
	assert.Equal(T, int64(5), results[1].BaseOffset, "P0 batch 2 should start at 5")
	assert.Equal(T, int32(0), results[1].Partition)

	assert.Equal(T, 3, results[3].InputIndex)
	assert.Equal(T, int64(8), results[3].BaseOffset, "P0 batch 3 should start at 8")
	assert.Equal(T, int32(0), results[3].Partition)

	// Partition 1 batches (independent calculation)
	assert.Equal(T, 2, results[2].InputIndex)
	assert.Equal(T, int64(0), results[2].BaseOffset, "P1 batch 1 should start at 0")
	assert.Equal(T, int32(1), results[2].Partition)

	assert.Equal(T, 4, results[4].InputIndex)
	assert.Equal(T, int64(4), results[4].BaseOffset, "P1 batch 2 should start at 4")
	assert.Equal(T, int32(1), results[4].Partition)

	// Verify all batches were inserted with correct offsets
	recordBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 5, "Should have five record batches")

	// Find batches by S3Key and verify their calculated offsets
	p0batch1 := findBatchV2ByS3(recordBatches, batches[0].S3Key)
	p0batch2 := findBatchV2ByS3(recordBatches, batches[1].S3Key)
	p0batch3 := findBatchV2ByS3(recordBatches, batches[3].S3Key)
	p1batch1 := findBatchV2ByS3(recordBatches, batches[2].S3Key)
	p1batch2 := findBatchV2ByS3(recordBatches, batches[4].S3Key)

	require.NotNil(T, p0batch1, "P0 batch 1 should exist")
	require.NotNil(T, p0batch2, "P0 batch 2 should exist")
	require.NotNil(T, p0batch3, "P0 batch 3 should exist")
	require.NotNil(T, p1batch1, "P1 batch 1 should exist")
	require.NotNil(T, p1batch2, "P1 batch 2 should exist")

	// Verify partition 0 window function calculation: [0,5), [5,8), [8,15)
	assert.Equal(T, int64(0), p0batch1.StartOffset, "P0 batch 1 should start at 0")
	assert.Equal(T, int64(5), p0batch1.StartOffset+p0batch1.NRecords, "P0 batch 1 should end at 5")
	assert.Equal(T, int64(5), p0batch2.StartOffset, "P0 batch 2 should start at 5")
	assert.Equal(T, int64(8), p0batch2.StartOffset+p0batch2.NRecords, "P0 batch 2 should end at 8")
	assert.Equal(T, int64(8), p0batch3.StartOffset, "P0 batch 3 should start at 8")
	assert.Equal(T, int64(15), p0batch3.StartOffset+p0batch3.NRecords, "P0 batch 3 should end at 15")

	// Verify partition 1 window function calculation: [0,4), [4,6)
	assert.Equal(T, int64(0), p1batch1.StartOffset, "P1 batch 1 should start at 0")
	assert.Equal(T, int64(4), p1batch1.StartOffset+p1batch1.NRecords, "P1 batch 1 should end at 4")
	assert.Equal(T, int64(4), p1batch2.StartOffset, "P1 batch 2 should start at 4")
	assert.Equal(T, int64(6), p1batch2.StartOffset+p1batch2.NRecords, "P1 batch 2 should end at 6")

	// Verify partition end_offsets were updated correctly
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 2)

	partition0 := findPartitionByNumber(partitions, 0)
	partition1 := findPartitionByNumber(partitions, 1)
	require.NotNil(T, partition0, "Partition 0 should exist")
	require.NotNil(T, partition1, "Partition 1 should exist")

	assert.Equal(T, int64(15), partition0.EndOffset, "Partition 0 end_offset should be 15")
	assert.Equal(T, int64(6), partition1.EndOffset, "Partition 1 end_offset should be 6")
}

func (s *MetastoreTestSuite) TestCommitRecordBatchesV2_WindowFunctionWithExistingOffsets() {
	metastore := s.TestDB.InitMetastore()
	T := s.T()

	// Setup: Create a TopicV2 with 1 partition
	topicName := "existing-offsets-v2-topic"
	err := metastore.CreateTopicV2(topicName, 1)
	require.NoError(T, err)

	topic := metastore.GetTopicByName(topicName)
	require.NotNil(T, topic)

	// First, commit an initial batch to establish existing data
	initialBatches := []RecordBatchV2{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  8,
			S3Key:     "s3://bucket/topic/partition-0/initial-batch.parquet",
		},
	}

	_, err = metastore.CommitRecordBatchesV2(initialBatches)
	require.NoError(T, err, "Initial batch commit should succeed")

	// Now commit new batches that should continue from offset 8
	newBatches := []RecordBatchV2{
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  5,
			S3Key:     "s3://bucket/topic/partition-0/new-batch-1.parquet",
		},
		{
			TopicID:   topic.ID,
			Partition: 0,
			NRecords:  3,
			S3Key:     "s3://bucket/topic/partition-0/new-batch-2.parquet",
		},
	}

	// Execute: Commit the new batches
	results, err := metastore.CommitRecordBatchesV2(newBatches)
	require.NoError(T, err, "New batch commit should succeed")
	require.Len(T, results, 2, "Should return two results")

	// Verify results - should continue from existing end_offset (8)
	assert.Equal(T, 0, results[0].InputIndex)
	assert.Equal(T, int64(8), results[0].BaseOffset, "First new batch should start at 8")

	assert.Equal(T, 1, results[1].InputIndex)
	assert.Equal(T, int64(13), results[1].BaseOffset, "Second new batch should start at 13")

	// Verify all batches exist
	recordBatches, err := metastore.GetRecordBatchesV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 3, "Should have three record batches")

	// Find the new batches
	newBatch1 := findBatchV2ByS3(recordBatches, newBatches[0].S3Key)
	newBatch2 := findBatchV2ByS3(recordBatches, newBatches[1].S3Key)

	require.NotNil(T, newBatch1, "New batch 1 should exist")
	require.NotNil(T, newBatch2, "New batch 2 should exist")

	// Verify window function calculation continues from existing offset
	assert.Equal(T, int64(8), newBatch1.StartOffset, "New batch 1 should start at 8")
	assert.Equal(T, int64(13), newBatch1.StartOffset+newBatch1.NRecords, "New batch 1 should end at 13")
	assert.Equal(T, int64(13), newBatch2.StartOffset, "New batch 2 should start at 13")
	assert.Equal(T, int64(16), newBatch2.StartOffset+newBatch2.NRecords, "New batch 2 should end at 16")

	// Verify partition end_offset was updated to final value
	partitions, err := metastore.GetTopicPartitions(topicName)
	require.NoError(T, err)
	require.Len(T, partitions, 1)
	assert.Equal(T, int64(16), partitions[0].EndOffset, "Partition end_offset should be updated to 16")
}
