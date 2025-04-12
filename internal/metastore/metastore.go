package metastore

import "gorm.io/gorm"

// Struct responsible for logic between server and postgres
type Metastore struct {
	db *gorm.DB
}

func NewMetastore(db *gorm.DB) *Metastore {
	return &Metastore{db}
}

func (m *Metastore) CreateTopic(name string, s3Path string) error {
	topic := &Topic{
		Name:       name,
		S3BasePath: s3Path,
		MinOffset:  0,
		MaxOffset:  0,
	}
	return m.db.Create(topic).Error
}

func (m *Metastore) GetTopics() ([]Topic, error) {
	var topics []Topic
	err := m.db.Raw("select * from topics").Scan(&topics).Error
	if err != nil {
		return nil, err
	}

	return topics, nil
}
