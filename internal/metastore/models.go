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
	Name       string `gorm:"size:255; not null"`
	S3BasePath string `gorm:"size:255; not null"`
	MinOffset  int64  `gorm:"type:bigint; not null; check:min_offset >= 0"`
	MaxOffset  int64  `gorm:"type:bigint; not null; check:max_offset >= 0; check:max_offset >= min_offset"`
}

// A record batch represents a contiguous group of kafka messages
type RecordBatch struct {
	BaseModel
	TopicID     uuid.UUID `gorm:"type:uuid; index"`
	Topic       Topic     `gorm:"foreignKey:TopicID"`
	StartOffset int64     `gorm:"type:bigint; not null; check:start_offset >= 0"`
	EndOffset   int64     `gorm:"type:bigint; not null; check:end_offset >= 0; check:end_offset > start_offset"`
	S3Path      string    `gorm:"size:255; not null"`
}
