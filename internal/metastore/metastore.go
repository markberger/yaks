package metastore

import (
	"fmt"

	"gorm.io/gorm"
)

type Metastore interface {
	ApplyMigrations() error
	CreateTopic(name string, s3Path string) error
	GetTopics() ([]Topic, error)
	GetRecordBatches(topicName string) ([]RecordBatch, error)
	CommitRecordBatch(topicName string, nRecords int64, s3Path string) error
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

func (m *GormMetastore) GetRecordBatches(topicName string) ([]RecordBatch, error) {
	var recordBatches []RecordBatch
	err := m.db.Raw("select * from record_batches").Scan(&recordBatches).Error
	if err != nil {
		return nil, err
	}

	return recordBatches, nil
}

func (m *GormMetastore) CommitRecordBatch(topicName string, nRecords int64, s3Path string) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		sql := `
			with topic_lookup as (
				select id, min_offset, max_offset
				from topics
				where name = @topicName
				limit 1
				for update
			),

			new_batch as (
				insert into record_batches (
					id,
					created_at,
					updated_at,
					topic_id,
					start_offset,
					end_offset,
					s3_path
				) select
					gen_random_uuid(),
					NOW(),
					NOW(),
					tl.id,
					case
						when min_offset = 0 and max_offset = 0
						then 0
						else max_offset + 1
					end,
					case
						when min_offset = 0 and max_offset = 0
						then @nRecords - 1
						else max_offset + @nRecords
					end,
					@s3Path

				from topic_lookup tl
			)

			update topics t
			set
				max_offset = case
					when tl.min_offset = 0 and tl.max_offset = 0
					then @nRecords - 1
					else tl.max_offset + @nRecords
				end,
				updated_at = NOW()
			from topic_lookup tl
			where t.id = tl.id;
		`
		namedArgs := map[string]interface{}{
			"topicName": topicName,
			"nRecords":  nRecords,
			"s3Path":    s3Path,
		}
		result := tx.Exec(sql, namedArgs)
		return result.Error
	})
}
