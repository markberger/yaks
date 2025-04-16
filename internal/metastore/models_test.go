package metastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func (s *MetastoreTestSuite) TestTopicModel() {
	db := s.TestDB.InitDB()
	db.AutoMigrate(&Topic{})

	tests := []struct {
		name    string
		topic   *Topic
		wantErr bool
	}{
		{
			name: "valid topic",
			topic: &Topic{
				Name:       "test-topic",
				S3BasePath: "s3://bucket/test-topic",
				MinOffset:  0,
				MaxOffset:  100,
			},
			wantErr: false,
		},
		{
			name: "invalid offset",
			topic: &Topic{
				Name:       "invalid-topic",
				S3BasePath: "s3://bucket/invalid-topic",
				MinOffset:  100,
				MaxOffset:  0, // Invalid: max < min
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			err := db.Create(tt.topic).Error
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, tt.topic.ID)
				assert.False(t, tt.topic.CreatedAt.IsZero())
				assert.False(t, tt.topic.UpdatedAt.IsZero())

				// Verify we can retrieve it
				var found Topic
				err := db.First(&found, "id = ?", tt.topic.ID).Error
				assert.NoError(t, err)
				assert.Equal(t, tt.topic.Name, found.Name)
				assert.Equal(t, tt.topic.S3BasePath, found.S3BasePath)
				assert.Equal(t, tt.topic.MinOffset, found.MinOffset)
				assert.Equal(t, tt.topic.MaxOffset, found.MaxOffset)
			}
		})
	}
}

func (s *MetastoreTestSuite) TestRecordBatchModel() {
	db := s.TestDB.InitDB()
	db.AutoMigrate(&Topic{}, &RecordBatch{})

	// Create a topic first
	topic := &Topic{
		Name:       "test-topic",
		S3BasePath: "s3://bucket/test-topic",
		MinOffset:  0,
		MaxOffset:  1000,
	}
	assert.NoError(s.T(), db.Create(topic).Error)

	tests := []struct {
		name    string
		batch   *RecordBatch
		wantErr bool
	}{
		{
			name: "valid batch",
			batch: &RecordBatch{
				Topic:       *topic,
				StartOffset: 0,
				EndOffset:   100,
				S3Path:      "s3://bucket/test-topic/0-100",
			},
			wantErr: false,
		},
		{
			name: "invalid offset",
			batch: &RecordBatch{
				Topic:       *topic,
				StartOffset: 100,
				EndOffset:   0, // Invalid: end < start
				S3Path:      "s3://bucket/test-topic/100-0",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			err := db.Create(tt.batch).Error
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, tt.batch.ID)
				assert.False(t, tt.batch.CreatedAt.IsZero())
				assert.False(t, tt.batch.UpdatedAt.IsZero())

				// Verify we can retrieve it with the topic relation
				var found RecordBatch
				err := db.Preload("Topic").First(&found, "id = ?", tt.batch.ID).Error
				assert.NoError(t, err)
				assert.Equal(t, tt.batch.S3Path, found.S3Path)
				assert.Equal(t, tt.batch.StartOffset, found.StartOffset)
				assert.Equal(t, tt.batch.EndOffset, found.EndOffset)
				assert.Equal(t, topic.ID, found.Topic.ID)
			}
		})
	}
}
