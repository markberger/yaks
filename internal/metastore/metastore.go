package metastore

import (
	"fmt"

	"gorm.io/gorm"
)

type Metastore interface {
	CreateTopic(name string, s3Path string) error
	GetTopics() ([]Topic, error)
	ApplyMigrations() error
}

// Struct responsible for logic between server and postgres
type GormMetastore struct {
	db *gorm.DB
}

func NewGormMetastore(db *gorm.DB) *GormMetastore {
	return &GormMetastore{db}
}

func (m *GormMetastore) ApplyMigrations() error {
	err := m.db.AutoMigrate(&Topic{}, &RecordBatch{})
	if err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}
	return nil
}

func (m *GormMetastore) CreateTopic(name string, s3Path string) error {
	topic := &Topic{
		Name:       name,
		S3BasePath: s3Path,
		MinOffset:  0,
		MaxOffset:  0,
	}
	result := m.db.Create(topic)
	return result.Error
}

func (m *GormMetastore) GetTopics() ([]Topic, error) {
	var topics []Topic
	err := m.db.Raw("select * from topics").Scan(&topics).Error
	if err != nil {
		return nil, err
	}

	return topics, nil
}
