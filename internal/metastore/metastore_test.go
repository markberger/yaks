package metastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTopic(t *testing.T) {
	tdb := NewTestDB()
	defer tdb.Close()
	metastore := NewGormMetastore(tdb.DB)
	metastore.ApplyMigrations()

	topicName := "test-topic"
	s3Path := "s3://some-bucket/topics/test-topic"
	err := metastore.CreateTopic(topicName, s3Path)
	require.NoError(t, err)

	topics, err := metastore.GetTopics()
	require.NoError(t, err)
	require.Equal(t, len(topics), 1)
	assert.Equal(t, topics[0].Name, topicName)
	assert.Equal(t, topics[0].S3BasePath, s3Path)
	assert.Equal(t, topics[0].MinOffset, int64(0))
	assert.Equal(t, topics[0].MaxOffset, int64(0))
}
