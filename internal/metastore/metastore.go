package metastore

import (
	"fmt"

	"github.com/lib/pq" // Import for pq.Array
	"gorm.io/gorm"
)

type Metastore interface {
	ApplyMigrations() error
	CreateTopic(name string, nPartitions int32) error
	CreateTopicV2(name string, nPartitions int32) error
	GetTopics() ([]Topic, error)
	GetTopicByName(name string) *TopicV2
	GetRecordBatches(topicName string) ([]RecordBatch, error)
	CommitRecordBatchEvents(recordBatchEvents []RecordBatchEvent) error
	MaterializeRecordBatchEvents(nRecords int32) error
	CommitRecordBatchesV2(batches []RecordBatchV2) ([]BatchCommitOutputV2, error)
	GetRecordBatchesV2(topicName string, startOffset int64) ([]RecordBatchV2, error)
	GetTopicPartitions(topicName string) ([]TopicPartition, error)
	GetRecordBatchEvents(topicName string) ([]RecordBatchEvent, error)
}

// Struct responsible for logic between server and postgres
type GormMetastore struct {
	db *gorm.DB
}

func NewGormMetastore(db *gorm.DB) *GormMetastore {
	return &GormMetastore{db}
}

// GetDB returns the underlying gorm.DB instance
func (m *GormMetastore) GetDB() *gorm.DB {
	return m.db
}

func (m *GormMetastore) ApplyMigrations() error {
	err := m.db.AutoMigrate(
		&Topic{},
		&RecordBatch{},
		&TopicV2{},
		&TopicPartition{},
		&RecordBatchEvent{},
		&RecordBatchV2{},
	)
	if err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}
	return nil
}

func (m *GormMetastore) CreateTopic(name string, nPartitions int32) error {
	topic := &Topic{
		Name:      name,
		MinOffset: 0,
		MaxOffset: 0,
	}
	result := m.db.Create(topic)
	return result.Error
}

func (m *GormMetastore) CreateTopicV2(name string, nPartitions int32) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		// Create the topic
		topicV2 := &TopicV2{
			Name:        name,
			NPartitions: nPartitions,
		}
		if err := tx.Create(topicV2).Error; err != nil {
			return err
		}

		// Create partitions for the topic
		for i := int32(0); i < nPartitions; i++ {
			partition := &TopicPartition{
				TopicID:     topicV2.ID,
				Partition:   i,
				StartOffset: 0,
				EndOffset:   0,
			}
			if err := tx.Create(partition).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (m *GormMetastore) GetTopics() ([]Topic, error) {
	var topics []Topic
	err := m.db.Raw("select * from topics").Scan(&topics).Error
	if err != nil {
		return nil, err
	}

	return topics, nil
}

func (m *GormMetastore) GetTopicsV2() ([]TopicV2, error) {
	var topics []TopicV2
	results := m.db.Find(&topics)
	if err := results.Error; err != nil {
		return nil, err
	}

	return topics, nil
}

func (m *GormMetastore) GetTopicByName(name string) *TopicV2 {
	var topic TopicV2
	result := m.db.Where("name = ?", name).First(&topic)
	if result.Error != nil {
		return nil
	}

	return &topic
}

func (m *GormMetastore) GetRecordBatches(topicName string) ([]RecordBatch, error) {
	var recordBatches []RecordBatch

	err := m.db.
		Joins("JOIN topics ON topics.id = record_batches.topic_id").
		Where("topics.name = ?", topicName).
		Preload("Topic"). // Preload still works after explicit join
		Order("record_batches.start_offset asc").
		Find(&recordBatches).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get record batches for topic %s: %w", topicName, err)
	}

	return recordBatches, nil
}

func (m *GormMetastore) CommitRecordBatchEvents(recordBatchEvents []RecordBatchEvent) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Create(recordBatchEvents)
		return result.Error
	})
}

func (m *GormMetastore) CommitRecordBatchesV2(batches []RecordBatchV2) ([]BatchCommitOutputV2, error) {
	if len(batches) == 0 {
		return []BatchCommitOutputV2{}, nil
	}

	// Extract data into parallel slices
	topicIDs := make([]string, len(batches))
	partitions := make([]int32, len(batches))
	nRecords := make([]int64, len(batches))
	s3Keys := make([]string, len(batches))
	for i, batch := range batches {
		topicIDs[i] = batch.TopicID.String()
		partitions[i] = batch.Partition
		nRecords[i] = batch.NRecords
		s3Keys[i] = batch.S3Key
	}

	var results []BatchCommitOutputV2
	err := m.db.Transaction(func(tx *gorm.DB) error {
		rawSQL := `
		-- Create input data from the provided arrays
		with input_data as (
			select
				idx - 1 as input_idx, -- 0-based index matching original slice
				topic_id::uuid,
				partition,
				n_record,
				s3_key
			from unnest($1::text[], $2::integer[], $3::bigint[], $4::text[])
				with ordinality as t(topic_id, partition, n_record, s3_key, idx)
		),

		-- lock each partition that we're going to update
		locked_partitions as (
			select tp.*
			from topic_partitions tp
			where (tp.topic_id, tp.partition) in (
				select distinct topic_id, partition
				from input_data
			)
			-- acquire locks in deterministic order to prevent deadlocks
			order by tp.topic_id, tp.partition
			for update of tp
		),

		-- compute the offsets for each batch
		offsets as (
			select
				inp.input_idx,
				inp.topic_id,
				inp.partition,
				inp.n_record,
				tp.end_offset + sum(inp.n_record) over (partition by inp.topic_id, inp.partition order by inp.input_idx) - inp.n_record as start_offset,
				tp.end_offset + sum(inp.n_record) over (partition by inp.topic_id, inp.partition order by inp.input_idx) as end_offset,
				inp.s3_key
			from input_data inp
			join locked_partitions tp
				on tp.topic_id = inp.topic_id
				and tp.partition = inp.partition
		),

		insert_batches as (
			insert into record_batch_v2(topic_id, partition, start_offset, n_records, s3_key)
			select topic_id, partition, start_offset, n_record, s3_key
			from offsets
			returning topic_id, partition, start_offset, s3_key
		),

		update_partitions as (
			update topic_partitions tp
			set end_offset = sub.max_end_offset
			from (
				select topic_id, partition, max(start_offset + n_record) as max_end_offset
				from offsets
				group by topic_id, partition
			) sub
			where tp.topic_id = sub.topic_id
				and tp.partition = sub.partition
			returning tp.topic_id
		)

		-- Return the results for each input batch
		select
			o.input_idx as input_idx,
			o.start_offset as base_offset,
			o.s3_key as s3_key,
			o.partition as partition
		from offsets o
		order by o.input_idx;
		`

		err := tx.Raw(rawSQL,
			pq.Array(topicIDs),
			pq.Array(partitions),
			pq.Array(nRecords),
			pq.Array(s3Keys),
		).Scan(&results).Error

		return err
	})

	if err != nil {
		return nil, fmt.Errorf("failed to commit record batches v2: %w", err)
	}

	return results, nil
}

func (m *GormMetastore) MaterializeRecordBatchEvents(nRecords int32) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		rawSQL := `
		-- lock the events we are going to materialize
		with locked_events as (

			select *
			from record_batch_events
			where processed = false
			order by created_at
			limit ?
			for update skip locked

		),

		-- lock each event's corresponding partition
		locked_partitions as (

			select tp.*
			from topic_partitions tp
			where (tp.topic_id, tp.partition) in (

				select distinct e.topic_id, e.partition
				from locked_events e

			)
			-- acquire locks in deterministic order to prevent deadlocks
			order by tp.topic_id, tp.partition
			for update of tp
		),

		-- compute the offsets for each event
		offsets as (

			select
				e.id,
				e.topic_id,
				e.partition,
				e.n_records,
				tp.end_offset + sum(e.n_records) over (partition by e.topic_id, e.partition order by e.id) - e.n_records as start_offset,
				tp.end_offset + sum(e.n_records) over (partition by e.topic_id, e.partition order by e.id) as end_offset,
				e.s3_key
			from locked_events e
			join locked_partitions tp
				on tp.topic_id = e.topic_id
				and tp.partition = e.partition
		),

		insert_batches as (
			insert into record_batch_v2(topic_id, partition, start_offset, n_records, s3_key)
			select topic_id, partition, start_offset, n_records, s3_key
			from offsets
			returning topic_id, partition
		),

		update_partitions as (
			update topic_partitions tp
			set end_offset = sub.max_end_offset
			from (
				select topic_id, partition, max(start_offset + n_records) as max_end_offset
				from offsets
				group by topic_id, partition
			) sub
			where tp.topic_id = sub.topic_id
				and tp.partition = sub.partition
			returning tp.topic_id
		)

		update record_batch_events
		set processed = true
		where id in (select id from locked_events);
		`

		if err := tx.Exec(rawSQL, nRecords).Error; err != nil {
			return err
		}

		return nil
	})
}

func (m *GormMetastore) GetRecordBatchesV2(topicName string, startOffset int64) ([]RecordBatchV2, error) {
	var recordBatches []RecordBatchV2

	rawSQL := `
		select rb.*
		from record_batch_v2 rb
		join topic_v2 t
			on rb.topic_id = t.id
		where t.name = ?
	`
	result := m.db.Raw(rawSQL, topicName).Scan(&recordBatches)
	if result.Error != nil {
		return nil, result.Error
	}

	return recordBatches, nil
}

func (m *GormMetastore) GetTopicPartitions(topicName string) ([]TopicPartition, error) {
	var topicPartitions []TopicPartition
	rawSQL := `
		select tp.*
		from topic_partitions tp
		join topic_v2 t
			on tp.topic_id = t.id
		where t.name = ?
	`
	result := m.db.Raw(rawSQL, topicName).Scan(&topicPartitions)
	if result.Error != nil {
		return nil, result.Error
	}

	return topicPartitions, nil
}

func (m *GormMetastore) GetRecordBatchEvents(topicName string) ([]RecordBatchEvent, error) {
	var events []RecordBatchEvent
	rawSQL := `
		select e.*
		from record_batch_events e
		join topic_v2 t
			on e.topic_id = t.id
		where t.name = ?
	`
	result := m.db.Raw(rawSQL, topicName).Scan(&events)
	if result.Error != nil {
		return nil, result.Error
	}

	return events, nil
}
