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
				Name:      "test-topic",
				MinOffset: 0,
				MaxOffset: 100,
			},
			wantErr: false,
		},
		{
			name: "invalid offset",
			topic: &Topic{
				Name:      "invalid-topic",
				MinOffset: 100,
				MaxOffset: 0, // Invalid: max < min
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
				assert.Equal(t, tt.topic.MinOffset, found.MinOffset)
				assert.Equal(t, tt.topic.MaxOffset, found.MaxOffset)
			}
		})
	}
}
