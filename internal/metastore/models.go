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

//
// V2 Models
//

type TopicV2 struct {
	BaseModel
	Name        string `gorm:"size:255; not null; index"`
	NPartitions int64  `gorm:"type:bigint; not null; check:n_partitions >= 0"`
	// Bounds are inclusive exclusive i.e. [StartOffset, EndOffset)
	StartOffset int64 `gorm:"type:bigint; not null; check:start_offset >= 0"`
	EndOffset   int64 `gorm:"type:bigint; not null; check:end_offset >= 0; check:end_offset >= start_offset"`
}

func (t *TopicV2) GetSize() int64 {
	return t.EndOffset - t.StartOffset
}

type TopicPartition struct {
	BaseModel
	TopicID        uuid.UUID `gorm:"type:uuid;index:idx_topic_partition,unique"`
	Topic          TopicV2   `gorm:"foreignKey:TopicID"`
	PartitionIndex int32     `gorm:"type:bigint;not null;index:idx_topic_partition,unique"`
	StartOffset    int64     `gorm:"type:bigint;not null;check:start_offset >= 0"`
	EndOffset      int64     `gorm:"type:bigint;not null;check:end_offset >= 0;check:end_offset >= start_offset"`
}

type RecordBatchEvent struct {
	BaseModel
	TopicID   uuid.UUID `gorm:"type:uuid; index"`
	Topic     TopicV2   `gorm:"foreignKey:TopicID"`
	Partition int64     `gorm:"type:bigint; not null;"`
	NRecords  int64     `gorm:"type:bigint; not null;"`
	S3Key     string    `gorm:"size:255; not null"`
	Processed bool      `gorm:"default:false"`
}

type RecordBatchV2 struct {
	BaseModel
	TopicID        uuid.UUID `gorm:"type:uuid; index"`
	Topic          TopicV2   `gorm:"foreignKey:TopicID"`
	PartitionIndex int32     `gorm:"type:bigint; not null;"`
	StartOffset    int64     `gorm:"type:bigint; not null; check:start_offset >= 0"`
	EndOffset      int64     `gorm:"type:bigint; not null; check:end_offset >= 0; check:end_offset > start_offset"`
	S3Key          string    `gorm:"size:255; not null"`
}

func (r *RecordBatchV2) GetSize() int64 {
	return r.EndOffset - r.StartOffset
}
