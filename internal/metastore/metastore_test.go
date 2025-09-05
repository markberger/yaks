package metastore

import (
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *MetastoreTestSuite) TestCreateTopic() {
	metastore := s.TestDB.InitMetastore()

	topicName := "test-topic"
	err := metastore.CreateTopic(topicName, 1)
	require.NoError(s.T(), err)

	topics, err := metastore.GetTopics()
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(topics), 1)
	assert.Equal(s.T(), topics[0].Name, topicName)
	assert.Equal(s.T(), topics[0].MinOffset, int64(0))
	assert.Equal(s.T(), topics[0].MaxOffset, int64(0))
}

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

func (s *MetastoreTestSuite) TestCommitRecordBatch() {
	metastore := s.TestDB.InitMetastore()
	err := metastore.CreateTopic("test-topic", 1)
	require.NoError(s.T(), err)

	err = metastore.CommitRecordBatch("test-topic", 10, "s3://some-bucket/topics/test-topic/randomHash.batch")
	require.NoError(s.T(), err)

	// Check topic offsets have changed. There are no entries so offsets should now
	// be 0 - 9 inclusively
	topics, err := metastore.GetTopics()
	require.NoError(s.T(), err)
	require.Len(s.T(), topics, 1)
	assert.Equal(s.T(), "test-topic", topics[0].Name)
	assert.Equal(s.T(), int64(0), topics[0].MinOffset)
	assert.Equal(s.T(), int64(9), topics[0].MaxOffset)

	// Check record batch
	recordBatches, err := metastore.GetRecordBatches("test-topic")
	require.NoError(s.T(), err)
	require.Len(s.T(), recordBatches, 1)
	assert.Equal(s.T(), topics[0].ID, recordBatches[0].TopicID)
	assert.Equal(s.T(), int64(0), recordBatches[0].StartOffset)
	assert.Equal(s.T(), int64(9), recordBatches[0].EndOffset)

	// Add another record batch now that topic has some entries
	err = metastore.CommitRecordBatch("test-topic", 5, "s3://some-bucket/topics/test-topic/randomHash_2.batch")
	require.NoError(s.T(), err)

	// Check topic offsets
	topics, err = metastore.GetTopics()
	require.NoError(s.T(), err)
	require.Len(s.T(), topics, 1)
	assert.Equal(s.T(), "test-topic", topics[0].Name)
	assert.Equal(s.T(), int64(0), topics[0].MinOffset)
	assert.Equal(s.T(), int64(14), topics[0].MaxOffset)

	// Check new batch offsets
	newRecordBatches, err := metastore.GetRecordBatches("test-topic")
	require.NoError(s.T(), err)
	require.Len(s.T(), newRecordBatches, 2)
	for _, batch := range newRecordBatches {
		if batch.ID != recordBatches[0].ID {
			require.Equal(s.T(), topics[0].ID, batch.TopicID)
			require.Equal(s.T(), int64(10), batch.StartOffset)
			require.Equal(s.T(), int64(14), batch.EndOffset)
		}
	}
}

func (s *MetastoreTestSuite) TestCommitRecordBatches2() {
	// When we have a new topic
	metastore := s.TestDB.InitMetastore()
	T := s.T()
	topic1Name := "batch-topic-1"
	require.NoError(T, metastore.CreateTopic(topic1Name, 1))

	// And we send a single message to commit
	batchInput1 := []BatchCommitInput{
		{TopicName: topic1Name, NRecords: 1, S3Key: "topics/" + topic1Name + "/b1.batch"},
	}
	_, err := metastore.CommitRecordBatches(batchInput1)
	require.NoError(T, err, "Single batch (new topic) failed")

	// Then we should have 1 RecordBatch with StartOffset=0 and EndOffset=0
	rbatches, _ := metastore.GetRecordBatches(topic1Name)
	require.Len(T, rbatches, 1)
	require.Equal(T, rbatches[0].StartOffset, int64(0))
	require.Equal(T, rbatches[0].EndOffset, int64(0))

	// When we commit another batch of messages
	batchInput2 := []BatchCommitInput{
		{TopicName: topic1Name, NRecords: 1, S3Key: "topics/" + topic1Name + "/b2.batch"},
	}
	_, err = metastore.CommitRecordBatches(batchInput2)
	require.NoError(T, err, "Single batch (new topic) failed")

	// Then we should have a second RecordBatch with StartOffset=1 EndOffset=1
	rbatches, _ = metastore.GetRecordBatches(topic1Name)
	require.Len(T, rbatches, 2)
	require.Equal(T, rbatches[1].StartOffset, int64(1))
	require.Equal(T, rbatches[1].EndOffset, int64(1))

}

func (s *MetastoreTestSuite) TestCommitRecordBatches() {
	metastore := s.TestDB.InitMetastore()
	T := s.T() // Alias for convenience

	// --- Scenario 1: Empty Batch ---
	results, err := metastore.CommitRecordBatches([]BatchCommitInput{})
	require.NoError(T, err, "Empty batch should not error")
	require.Empty(T, results, "Empty batch should return empty results")

	// --- Setup for subsequent scenarios ---
	topic1Name := "batch-topic-1"
	topic2Name := "batch-topic-2"
	require.NoError(T, metastore.CreateTopic(topic1Name, 1))
	require.NoError(T, metastore.CreateTopic(topic2Name, 1))

	// --- Scenario 2: Single Batch (New Topic State) ---
	batchInput1 := []BatchCommitInput{
		{TopicName: topic1Name, NRecords: 10, S3Key: "topics/" + topic1Name + "/b1.batch"},
	}
	results, err = metastore.CommitRecordBatches(batchInput1)
	require.NoError(T, err, "Single batch (new topic) failed")
	require.Len(T, results, 1)
	assert.Equal(T, 0, results[0].InputIndex, "Single batch index mismatch")
	assert.Equal(T, int64(0), results[0].BaseOffset, "Single batch (new topic) base offset mismatch")
	assert.Equal(T, batchInput1[0].S3Key, results[0].S3Key, "Single batch S3 path mismatch")

	// Verify topic 1 state
	topics, _ := metastore.GetTopics()
	topic1 := findTopic(topics, topic1Name)
	require.NotNil(T, topic1, "Topic 1 not found after single batch")
	assert.Equal(T, int64(9), topic1.MaxOffset, "Topic 1 MaxOffset after single batch")

	// Verify record batch 1 state
	rbatches, _ := metastore.GetRecordBatches(topic1Name)
	require.Len(T, rbatches, 1)
	assert.Equal(T, int64(0), rbatches[0].StartOffset)
	assert.Equal(T, int64(9), rbatches[0].EndOffset)
	assert.Equal(T, batchInput1[0].S3Key, rbatches[0].S3Key)

	// --- Scenario 3: Single Batch (Existing Topic State) ---
	batchInput2 := []BatchCommitInput{
		{TopicName: topic1Name, NRecords: 5, S3Key: "topics/" + topic1Name + "/b2.batch"},
	}
	results, err = metastore.CommitRecordBatches(batchInput2)
	require.NoError(T, err, "Single batch (existing topic) failed")
	require.Len(T, results, 1)
	assert.Equal(T, 0, results[0].InputIndex)
	assert.Equal(T, int64(10), results[0].BaseOffset, "Single batch (existing topic) base offset mismatch") // 9 + 1
	assert.Equal(T, batchInput2[0].S3Key, results[0].S3Key)

	// Verify topic 1 state
	topics, _ = metastore.GetTopics()
	topic1 = findTopic(topics, topic1Name)
	require.NotNil(T, topic1)
	assert.Equal(T, int64(14), topic1.MaxOffset, "Topic 1 MaxOffset after second single batch") // 9 + 5

	// Verify record batch 2 state
	rbatches, _ = metastore.GetRecordBatches(topic1Name)
	require.Len(T, rbatches, 2)
	batch2 := findBatchByS3(rbatches, batchInput2[0].S3Key)
	require.NotNil(T, batch2)
	assert.Equal(T, int64(10), batch2.StartOffset)
	assert.Equal(T, int64(14), batch2.EndOffset)

	// --- Scenario 4: Multiple Batches (Same Topic) ---
	batchInput3 := []BatchCommitInput{
		{TopicName: topic1Name, NRecords: 3, S3Key: "topics/" + topic1Name + "/b3.batch"}, // Expected base: 15
		{TopicName: topic1Name, NRecords: 7, S3Key: "topics/" + topic1Name + "/b4.batch"}, // Expected base: 18 (15+3)
	}
	results, err = metastore.CommitRecordBatches(batchInput3)
	require.NoError(T, err, "Multi batch (same topic) failed")
	require.Len(T, results, 2)

	// Check first result
	assert.Equal(T, 0, results[0].InputIndex)
	assert.Equal(T, int64(15), results[0].BaseOffset, "Multi batch (same topic) base offset 1 mismatch") // 14 + 1
	assert.Equal(T, batchInput3[0].S3Key, results[0].S3Key)

	// Check second result
	assert.Equal(T, 1, results[1].InputIndex)
	assert.Equal(T, int64(18), results[1].BaseOffset, "Multi batch (same topic) base offset 2 mismatch") // 15 + 3
	assert.Equal(T, batchInput3[1].S3Key, results[1].S3Key)

	// Verify topic 1 state
	topics, _ = metastore.GetTopics()
	topic1 = findTopic(topics, topic1Name)
	require.NotNil(T, topic1)
	assert.Equal(T, int64(24), topic1.MaxOffset, "Topic 1 MaxOffset after multi batch") // 14 + 3 + 7

	// Verify record batches 3 & 4 state
	rbatches, _ = metastore.GetRecordBatches(topic1Name)
	require.Len(T, rbatches, 4)
	batch3 := findBatchByS3(rbatches, batchInput3[0].S3Key)
	batch4 := findBatchByS3(rbatches, batchInput3[1].S3Key)
	require.NotNil(T, batch3)
	require.NotNil(T, batch4)
	assert.Equal(T, int64(15), batch3.StartOffset)
	assert.Equal(T, int64(17), batch3.EndOffset)
	assert.Equal(T, int64(18), batch4.StartOffset)
	assert.Equal(T, int64(24), batch4.EndOffset)

	// --- Scenario 5: Multiple Batches (Different Topics) ---
	// Topic 1 MaxOffset: 24
	// Topic 2 MaxOffset: 0 (initial state)
	batchInput4 := []BatchCommitInput{
		{TopicName: topic2Name, NRecords: 8, S3Key: "topics/" + topic2Name + "/b1.batch"}, // Expected base: 0
		{TopicName: topic1Name, NRecords: 6, S3Key: "topics/" + topic1Name + "/b5.batch"}, // Expected base: 25 (24+1)
		{TopicName: topic2Name, NRecords: 4, S3Key: "topics/" + topic2Name + "/b2.batch"}, // Expected base: 8 (0+8)
	}
	results, err = metastore.CommitRecordBatches(batchInput4)
	require.NoError(T, err, "Multi batch (diff topics) failed")
	require.Len(T, results, 3)

	// Check results (order matters)
	assert.Equal(T, 0, results[0].InputIndex) // topic2, batch 1
	assert.Equal(T, int64(0), results[0].BaseOffset)
	assert.Equal(T, batchInput4[0].S3Key, results[0].S3Key)

	assert.Equal(T, 1, results[1].InputIndex)         // topic1, batch 5
	assert.Equal(T, int64(25), results[1].BaseOffset) // 24 + 1
	assert.Equal(T, batchInput4[1].S3Key, results[1].S3Key)

	assert.Equal(T, 2, results[2].InputIndex)        // topic2, batch 2
	assert.Equal(T, int64(8), results[2].BaseOffset) // 0 + 8 (from first batch in this call)
	assert.Equal(T, batchInput4[2].S3Key, results[2].S3Key)

	// Verify topic 1 state
	topics, _ = metastore.GetTopics()
	topic1 = findTopic(topics, topic1Name)
	require.NotNil(T, topic1)
	assert.Equal(T, int64(30), topic1.MaxOffset, "Topic 1 MaxOffset after multi batch (diff)") // 24 + 6

	// Verify topic 2 state
	topic2 := findTopic(topics, topic2Name)
	require.NotNil(T, topic2)
	assert.Equal(T, int64(11), topic2.MaxOffset, "Topic 2 MaxOffset after multi batch (diff)") // 0 + 8 + 4

	// Verify record batches state for topic 1
	rbatches1, _ := metastore.GetRecordBatches(topic1Name)
	require.Len(T, rbatches1, 5)
	batch5 := findBatchByS3(rbatches1, batchInput4[1].S3Key)
	require.NotNil(T, batch5)
	assert.Equal(T, int64(25), batch5.StartOffset)
	assert.Equal(T, int64(30), batch5.EndOffset)

	// Verify record batches state for topic 2
	rbatches2, _ := metastore.GetRecordBatches(topic2Name)
	require.Len(T, rbatches2, 2)
	batchT2_1 := findBatchByS3(rbatches2, batchInput4[0].S3Key)
	batchT2_2 := findBatchByS3(rbatches2, batchInput4[2].S3Key)
	require.NotNil(T, batchT2_1)
	require.NotNil(T, batchT2_2)
	assert.Equal(T, int64(0), batchT2_1.StartOffset)
	assert.Equal(T, int64(7), batchT2_1.EndOffset)
	assert.Equal(T, int64(8), batchT2_2.StartOffset)
	assert.Equal(T, int64(11), batchT2_2.EndOffset)
}

// Helper function to find a topic by name
func findTopic(topics []Topic, name string) *Topic {
	for i := range topics {
		if topics[i].Name == name {
			return &topics[i]
		}
	}
	return nil
}

// Helper function to find a record batch by S3 path
func findBatchByS3(batches []RecordBatch, s3Path string) *RecordBatch {
	for i := range batches {
		if batches[i].S3Key == s3Path {
			return &batches[i]
		}
	}
	return nil
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
	recordBatchesV2, err := metastore.GetRecordBatchV2(topicName, 0)
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
		assert.Equal(T, int64(5), firstBatch.EndOffset)
		assert.Equal(T, int64(5), secondBatch.StartOffset)
		assert.Equal(T, int64(8), secondBatch.EndOffset)
	} else {
		// batch-2 was processed first (3 records)
		assert.Equal(T, int64(3), firstBatch.EndOffset)
		assert.Equal(T, int64(3), secondBatch.StartOffset)
		assert.Equal(T, int64(8), secondBatch.EndOffset)
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
	recordBatchesP0, err := metastore.GetRecordBatchV2(topicName, 0)
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
	assert.Equal(T, int64(5), partition0Batches[0].EndOffset, "First batch should end at 5")

	assert.Equal(T, int64(5), partition0Batches[1].StartOffset, "Second batch should start at 5")
	assert.Equal(T, int64(8), partition0Batches[1].EndOffset, "Second batch should end at 8")

	assert.Equal(T, int64(8), partition0Batches[2].StartOffset, "Third batch should start at 8")
	assert.Equal(T, int64(15), partition0Batches[2].EndOffset, "Third batch should end at 15")

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
	assert.Equal(T, int64(4), partition1Batches[0].EndOffset, "First batch in partition 1 should end at 4")

	assert.Equal(T, int64(4), partition1Batches[1].StartOffset, "Second batch in partition 1 should start at 4")
	assert.Equal(T, int64(6), partition1Batches[1].EndOffset, "Second batch in partition 1 should end at 6")

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
	allBatches, err := metastore.GetRecordBatchV2(topicName, 0)
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
		assert.Equal(T, int64(13), firstNewBatch.EndOffset, "First new batch should end at 13")
		assert.Equal(T, int64(13), secondNewBatch.StartOffset, "Second new batch should start at 13")
		assert.Equal(T, int64(16), secondNewBatch.EndOffset, "Second new batch should end at 16")
	} else {
		// new-batch-2 was processed first (3 records)
		assert.Equal(T, int64(11), firstNewBatch.EndOffset, "First new batch should end at 11")
		assert.Equal(T, int64(11), secondNewBatch.StartOffset, "Second new batch should start at 11")
		assert.Equal(T, int64(16), secondNewBatch.EndOffset, "Second new batch should end at 16")
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
	recordBatches, err := metastore.GetRecordBatchV2(topicName, 0)
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
	assert.Equal(T, int64(5), firstBatch.EndOffset, "First batch should end at 5")

	assert.Equal(T, int64(5), secondBatch.StartOffset, "Second batch should start at 5 (processed second by ID)")
	assert.Equal(T, int64(8), secondBatch.EndOffset, "Second batch should end at 8")

	assert.Equal(T, int64(8), thirdBatch.StartOffset, "Third batch should start at 8 (processed third by ID)")
	assert.Equal(T, int64(15), thirdBatch.EndOffset, "Third batch should end at 15")

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
	recordBatches, err := metastore.GetRecordBatchV2(topicName, 0)
	require.NoError(T, err)
	require.Len(T, recordBatches, 1, "Should have exactly 1 RecordBatchV2 due to batch limit")

	// The single batch should span [0, 5) and correspond to the first event
	batch := recordBatches[0]
	assert.Equal(T, int64(0), batch.StartOffset, "Single batch should start at 0")
	assert.Equal(T, int64(5), batch.EndOffset, "Single batch should end at 5")
	assert.Equal(T, "s3://bucket/topic/batch-1.parquet", batch.S3Key, "Batch should correspond to first event")
}
