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
