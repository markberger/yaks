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

// Output structure containing the result for a single committed batch in V2 system
type BatchCommitOutputV2 struct {
	InputIndex int    `gorm:"column:input_idx"`
	BaseOffset int64  `gorm:"column:base_offset"`
	S3Key      string `gorm:"column:s3_key"`
	Partition  int32  `gorm:"column:partition"`
}

//
// V2 Models
//

type TopicV2 struct {
	BaseModel
	Name        string `gorm:"size:255; not null; index"`
	NPartitions int32  `gorm:"type:integer; not null; check:n_partitions >= 0"`
}

type TopicPartition struct {
	BaseModel
	TopicID   uuid.UUID `gorm:"type:uuid;index:idx_topic_partition,unique;index:idx_topic_partition_lookup"`
	Topic     TopicV2   `gorm:"foreignKey:TopicID"`
	Partition int32     `gorm:"type:integer;not null;index:idx_topic_partition,unique;index:idx_topic_partition_lookup"`
	// Bounds are inclusive exclusive i.e. [StartOffset, EndOffset)
	StartOffset int64 `gorm:"type:bigint;not null;check:start_offset >= 0"`
	EndOffset   int64 `gorm:"type:bigint;not null;check:end_offset >= 0;check:end_offset >= start_offset"`
}

type RecordBatchEvent struct {
	BaseModel
	TopicID    uuid.UUID `gorm:"type:uuid; not null; index; index:idx_rbe_unprocessed_ordered"`
	Topic      TopicV2   `gorm:"foreignKey:TopicID"`
	Partition  int32     `gorm:"type:integer; not null; index:idx_rbe_unprocessed_ordered"`
	NRecords   int64     `gorm:"type:bigint; not null;"`
	S3Key      string    `gorm:"size:255; not null"`
	ByteOffset int64     `gorm:"type:bigint; not null; default:0"`
	ByteLength int64     `gorm:"type:bigint; not null; default:0"`
	Processed  bool      `gorm:"default:false; index:idx_rbe_unprocessed_ordered"`
}

type ConsumerGroupOffset struct {
	BaseModel
	GroupID   string    `gorm:"size:255;not null;uniqueIndex:idx_consumer_group_offset"`
	TopicID   uuid.UUID `gorm:"type:uuid;not null;uniqueIndex:idx_consumer_group_offset"`
	Partition int32     `gorm:"not null;uniqueIndex:idx_consumer_group_offset"`
	Offset    int64     `gorm:"type:bigint;not null"`
	Metadata  string    `gorm:"size:255"`
}

type GroupcachePeer struct {
	NodeID         int32     `gorm:"primaryKey;autoIncrement:false"`
	PeerURL        string    `gorm:"size:255;not null"`
	LeaseExpiresAt time.Time `gorm:"not null;index"`
}

type RecordBatchV2 struct {
	BaseModel
	TopicID   uuid.UUID `gorm:"type:uuid; index"`
	Topic     TopicV2   `gorm:"foreignKey:TopicID"`
	Partition int32     `gorm:"type:integer; not null;"`
	// StartOffset is the first offset in this batch (inclusive)
	StartOffset int64  `gorm:"type:bigint; not null; check:start_offset >= 0"`
	NRecords    int64  `gorm:"type:bigint; not null; check:n_records >= 0"`
	S3Key       string `gorm:"size:255; not null"`
	ByteOffset  int64  `gorm:"type:bigint; not null; default:0"`
	ByteLength  int64  `gorm:"type:bigint; not null; default:0"`
}
