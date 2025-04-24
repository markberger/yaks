package metastore

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type BaseModel struct {
	ID        uuid.UUID      `gorm:"type:uuid; primary_key; default:gen_random_uuid()"`
	CreatedAt time.Time      `gorm:"autoCreateTime"`
	UpdatedAt time.Time      `gorm:"autoUpdateTime"`
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

// Represents a kafka topic
type Topic struct {
	BaseModel
	Name      string `gorm:"size:255; not null; index"`
	MinOffset int64  `gorm:"type:bigint; not null; check:min_offset >= 0"`
	MaxOffset int64  `gorm:"type:bigint; not null; check:max_offset >= 0; check:max_offset >= min_offset"`
}

// A record batch represents a contiguous group of kafka messages
type RecordBatch struct {
	BaseModel
	TopicID     uuid.UUID `gorm:"type:uuid; index"`
	Topic       Topic     `gorm:"foreignKey:TopicID"`
	StartOffset int64     `gorm:"type:bigint; not null; check:start_offset >= 0"`
	EndOffset   int64     `gorm:"type:bigint; not null; check:end_offset >= 0; check:end_offset >= start_offset"`
	S3Key       string    `gorm:"size:255; not null"`
}

// Input structure for a single batch commit request within the batch operation
type BatchCommitInput struct {
	TopicName string
	NRecords  int64
	S3Key     string
}

// Output structure containing the result for a single committed batch
type BatchCommitOutput struct {
	InputIndex int    `gorm:"column:input_idx"`
	BaseOffset int64  `gorm:"column:base_offset"` // Match the alias used in the SQL query
	S3Key      string `gorm:"column:s3_key"`      // Change expected column name to s3_key
}
