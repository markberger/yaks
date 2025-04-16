package metastore

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *MetastoreTestSuite) TestCreateTopic() {
	metastore := s.TestDB.InitMetastore()

	topicName := "test-topic"
	s3Path := "s3://some-bucket/topics/test-topic"
	err := metastore.CreateTopic(topicName, s3Path)
	require.NoError(s.T(), err)

	topics, err := metastore.GetTopics()
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(topics), 1)
	assert.Equal(s.T(), topics[0].Name, topicName)
	assert.Equal(s.T(), topics[0].S3BasePath, s3Path)
	assert.Equal(s.T(), topics[0].MinOffset, int64(0))
	assert.Equal(s.T(), topics[0].MaxOffset, int64(0))
}

func (s *MetastoreTestSuite) TestCommitRecordBatch() {
	metastore := s.TestDB.InitMetastore()
	err := metastore.CreateTopic("test-topic", "s3://some-bucket/topics/test-topic")
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
