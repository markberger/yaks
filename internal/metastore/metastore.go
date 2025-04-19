package metastore

import (
	"fmt"

	"github.com/lib/pq" // Import for pq.Array
	"gorm.io/gorm"
)

type Metastore interface {
	ApplyMigrations() error
	CreateTopic(name string, s3Path string) error
	GetTopics() ([]Topic, error)
	GetRecordBatches(topicName string) ([]RecordBatch, error)
	CommitRecordBatch(topicName string, nRecords int64, s3Path string) error
	CommitRecordBatches(batches []BatchCommitInput) ([]BatchCommitOutput, error)
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

func (m *GormMetastore) CommitRecordBatches(batches []BatchCommitInput) ([]BatchCommitOutput, error) {
	if len(batches) == 0 {
		return []BatchCommitOutput{}, nil
	}

	// Extract data into parallel slices
	topicNames := make([]string, len(batches))
	nRecords := make([]int64, len(batches))
	s3Paths := make([]string, len(batches))
	for i, batch := range batches {
		topicNames[i] = batch.TopicName
		nRecords[i] = batch.NRecords
		s3Paths[i] = batch.S3Path
	}

	var results []BatchCommitOutput
	err := m.db.Transaction(func(tx *gorm.DB) error {
		sqlQuery := `
			WITH input_data AS (
				-- Use unnest to create rows from input arrays, preserving order with ORDINALITY
				SELECT
					idx - 1 AS input_idx, -- 0-based index matching original slice
					topic_name,
					n_record,
					s3_path
				FROM unnest($1::text[], $2::bigint[], $3::text[])
					 WITH ORDINALITY AS t(topic_name, n_record, s3_path, idx)
			),
			topic_lookup AS (
				-- Get current state of all relevant topics and lock them
				SELECT
					t.id,
					t.name,
					t.min_offset,
					t.max_offset
				FROM topics t
				JOIN (SELECT DISTINCT topic_name FROM input_data) AS needed_topics
				  ON t.name = needed_topics.topic_name
				FOR UPDATE OF t
			),
			offset_calculations AS (
				-- Calculate offsets for each batch, considering preceding batches in this transaction
				SELECT
					inp.input_idx,
					inp.topic_name,
					inp.n_record AS n_records, -- Alias for clarity
					inp.s3_path,
					tl.id AS topic_id,
					-- Sum records for the same topic from preceding input rows (ordered by input index)
					COALESCE(SUM(inp.n_record) OVER (PARTITION BY tl.id ORDER BY inp.input_idx ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS preceding_records_in_batch,
					tl.max_offset AS initial_topic_max_offset,
					tl.min_offset AS initial_topic_min_offset
				FROM input_data inp
				JOIN topic_lookup tl ON inp.topic_name = tl.name
			),
			new_batches_insert AS (
				-- Insert the new batches with calculated offsets
				INSERT INTO record_batches (
					id, created_at, updated_at, topic_id, start_offset, end_offset, s3_path
				)
				SELECT
					gen_random_uuid(),
					NOW(),
					NOW(),
					oc.topic_id,
					-- Calculate start_offset (handle initial state)
					CASE
						WHEN oc.initial_topic_min_offset = 0 AND oc.initial_topic_max_offset = 0
						THEN oc.preceding_records_in_batch
						ELSE oc.initial_topic_max_offset + 1 + oc.preceding_records_in_batch
					END AS start_offset,
					-- Calculate end_offset (handle initial state)
					CASE
						WHEN oc.initial_topic_min_offset = 0 AND oc.initial_topic_max_offset = 0
						THEN oc.preceding_records_in_batch + oc.n_records - 1
						ELSE oc.initial_topic_max_offset + 1 + oc.preceding_records_in_batch + oc.n_records - 1
					END AS end_offset,
					oc.s3_path
				FROM offset_calculations oc
				-- ORDER BY oc.input_idx -- Optional: Ensures insert order matches input if needed
				RETURNING id, topic_id, start_offset, end_offset, s3_path -- Also return end_offset
			),
			topic_final_offsets AS (
				-- Find the maximum end_offset achieved for each topic in this batch insert
				SELECT
					topic_id,
					MAX(end_offset) AS final_end_offset
				FROM new_batches_insert
				GROUP BY topic_id
			),
			final_topic_update AS (
				-- Update the topics table using the calculated final end_offset
				UPDATE topics t
				SET
					max_offset = tfo.final_end_offset, -- Set to the highest end_offset reached
					updated_at = NOW()
				FROM topic_final_offsets tfo
				WHERE t.id = tfo.topic_id
			)
			-- Final SELECT to retrieve the required output, joining inserted data back to calculations
			SELECT
				oc.input_idx,
				nb.start_offset,
				nb.s3_path
			FROM new_batches_insert nb
			JOIN offset_calculations oc ON nb.topic_id = oc.topic_id AND nb.start_offset = (
				 -- Re-calculate start offset exactly as done in the INSERT to ensure a correct join key
				 CASE
					WHEN oc.initial_topic_min_offset = 0 AND oc.initial_topic_max_offset = 0 THEN oc.preceding_records_in_batch
					ELSE oc.initial_topic_max_offset + 1 + oc.preceding_records_in_batch
				 END
			) AND nb.s3_path = oc.s3_path -- Assumes this combination is unique enough within the batch for joining
			ORDER BY oc.input_idx; -- Return results in the original input order
		`

		err := tx.Raw(sqlQuery,
			pq.Array(topicNames),
			pq.Array(nRecords),
			pq.Array(s3Paths),
		).Scan(&results).Error

		return err
	})

	if err != nil {
		return nil, fmt.Errorf("failed to commit record batches: %w", err)
	}

	return results, nil
}
