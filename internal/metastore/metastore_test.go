package metastore

import (
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
