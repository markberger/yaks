package metastore

import "gorm.io/gorm"

type Metastore interface {
	CreateTopic(name string, s3Path string) error
	GetTopics() ([]Topic, error)
}

// Struct responsible for logic between server and postgres
type GormMetastore struct {
	db *gorm.DB
}

func NewGormMetastore(db *gorm.DB) *GormMetastore {
	return &GormMetastore{db}
}

func (m *GormMetastore) CreateTopic(name string, s3Path string) error {
	topic := &Topic{
		Name:       name,
		S3BasePath: s3Path,
		MinOffset:  0,
		MaxOffset:  0,
	}
	return m.db.Create(topic).Error
}

func (m *GormMetastore) GetTopics() ([]Topic, error) {
	var topics []Topic
	err := m.db.Raw("select * from topics").Scan(&topics).Error
	if err != nil {
		return nil, err
	}

	return topics, nil
}
