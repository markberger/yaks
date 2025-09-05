# Query Performance Optimizations for MaterializeRecordBatchEvents

## Overview

This document outlines performance optimization strategies for the
`MaterializeRecordBatchEvents` function in the metastore package. The
optimizations respect the critical constraint that record batch offsets must be
contiguous within each topic/partition.

## Current Implementation Analysis

The `MaterializeRecordBatchEvents` function performs a complex operation:

1. Locks unprocessed `RecordBatchEvent` records (with `SKIP LOCKED`)
2. Locks corresponding `TopicPartition` records
3. Calculates running offsets using window functions
4. Inserts materialized `RecordBatchV2` records
5. Updates partition end offsets
6. Marks events as processed

### Performance Bottlenecks Identified

1. **Window Function Overhead**:
   `sum(e.n_records) over (partition by e.topic_id, e.partition order by e.id)`
   can be expensive for large batches
2. **Missing Indexes**: No optimal indexes for the query patterns used
3. **Multiple Table Locks**: Current approach locks both events and partitions,
   creating potential contention
4. **Large Transaction Scope**: Processing many events in a single transaction

## Optimization Strategies

### 1. Index Optimizations (High Impact, Low Risk)

Add composite indexes to improve query performance:

```go
// In models.go - RecordBatchEvent
type RecordBatchEvent struct {
    BaseModel
    TopicID   uuid.UUID `gorm:"type:uuid; not null; index:idx_rbe_unprocessed_ordered"`
    Topic     TopicV2   `gorm:"foreignKey:TopicID"`
    Partition int32     `gorm:"type:integer; not null; index:idx_rbe_unprocessed_ordered"`
    NRecords  int64     `gorm:"type:bigint; not null;"`
    S3Key     string    `gorm:"size:255; not null"`
    Processed bool      `gorm:"default:false; index:idx_rbe_unprocessed_ordered"`
    CreatedAt time.Time `gorm:"autoCreateTime; index:idx_rbe_unprocessed_ordered"`
}

// Separate index for partition lookups
type TopicPartition struct {
    BaseModel
    TopicID   uuid.UUID `gorm:"type:uuid;index:idx_topic_partition_lookup"`
    Topic     TopicV2   `gorm:"foreignKey:TopicID"`
    Partition int32     `gorm:"type:integer;not null;index:idx_topic_partition_lookup"`
    StartOffset int64 `gorm:"type:bigint;not null;check:start_offset >= 0"`
    EndOffset   int64 `gorm:"type:bigint;not null;check:end_offset >= 0;check:end_offset >= start_offset"`
}
```

**Expected Impact**: 30-50% improvement in query planning and execution

### 2. Partition-Aware Batching

Process events in smaller, topic/partition-specific batches to reduce window
function scope:

```sql
-- Process events partition by partition to reduce window function scope
WITH partition_batches AS (
    SELECT DISTINCT topic_id, partition
    FROM record_batch_events
    WHERE processed = false
    ORDER BY topic_id, partition
    LIMIT 10  -- Limit number of partitions processed at once
),
locked_events AS (
    SELECT e.*
    FROM record_batch_events e
    JOIN partition_batches pb ON e.topic_id = pb.topic_id AND e.partition = pb.partition
    WHERE e.processed = false
    ORDER BY e.topic_id, e.partition, e.created_at
    LIMIT ?
    FOR UPDATE SKIP LOCKED
),
-- Rest of the query remains similar but operates on smaller dataset
locked_partitions AS (
    SELECT tp.*
    FROM topic_partitions tp
    WHERE (tp.topic_id, tp.partition) IN (
        SELECT DISTINCT e.topic_id, e.partition
        FROM locked_events e
    )
    FOR UPDATE OF tp
),
-- Continue with existing offset calculation logic...
```

**Expected Impact**: 20-40% reduction in lock contention, better concurrency

### 3. Incremental Offset Calculation

Replace window functions with incremental calculation using recursive CTEs:

```sql
WITH RECURSIVE offset_calc AS (
    -- Base case: first event per partition
    SELECT
        id, topic_id, partition, n_records, s3_key,
        ROW_NUMBER() OVER (PARTITION BY topic_id, partition ORDER BY created_at) as rn,
        tp.end_offset as start_offset,
        tp.end_offset + n_records as end_offset
    FROM locked_events e
    JOIN locked_partitions tp ON e.topic_id = tp.topic_id AND e.partition = tp.partition
    WHERE ROW_NUMBER() OVER (PARTITION BY topic_id, partition ORDER BY created_at) = 1

    UNION ALL

    -- Recursive case: subsequent events
    SELECT
        e.id, e.topic_id, e.partition, e.n_records, e.s3_key,
        ROW_NUMBER() OVER (PARTITION BY e.topic_id, e.partition ORDER BY e.created_at),
        oc.end_offset as start_offset,
        oc.end_offset + e.n_records as end_offset
    FROM locked_events e
    JOIN offset_calc oc ON e.topic_id = oc.topic_id AND e.partition = oc.partition
    WHERE ROW_NUMBER() OVER (PARTITION BY e.topic_id, e.partition ORDER BY e.created_at) = oc.rn + 1
)
SELECT * FROM offset_calc;
```

**Expected Impact**: 40-60% reduction in computation overhead for large batches

### 4. Optimized Batch Size Strategy

Instead of processing arbitrary `nRecords`, optimize batching strategy:

```go
// Option A: Partition-complete batching
func (m *GormMetastore) MaterializeRecordBatchEventsByPartition(maxPartitions int32) error {
    // Process complete partitions rather than arbitrary record counts
    // Ensures better cache locality and reduces fragmentation
}

// Option B: Time-based batching
func (m *GormMetastore) MaterializeRecordBatchEventsByTime(timeWindow time.Duration) error {
    // Process events within time windows
    // Better for real-time processing scenarios
}

// Option C: Adaptive batching
func (m *GormMetastore) MaterializeRecordBatchEventsAdaptive() error {
    // Adjust batch size based on partition activity and system load
    // Monitor processing times and adjust accordingly
}
```

### 5. Hot and Cold Path Separation

Split materialization into two phases:

```go
// Hot path: Quick insertion of events with minimal validation
func (m *GormMetastore) CommitRecordBatchEvents(events []RecordBatchEvent) error {
    // Simple bulk insert, no offset calculation
    // Focus on high throughput for event ingestion
    return m.db.Create(events).Error
}

// Cold path: Optimized materialization with batching
func (m *GormMetastore) MaterializeRecordBatchEventsOptimized(partitionLimit int32) error {
    // Process limited number of partitions with optimized query
    // Can be run asynchronously or on schedule
    return m.db.Transaction(func(tx *gorm.DB) error {
        // Optimized materialization logic here
    })
}
```

### 6. Query Plan Optimization

Add PostgreSQL-specific optimizations:

```sql
-- Force specific join order and index usage
/*+ USE_INDEX(record_batch_events, idx_rbe_unprocessed_ordered) */
/*+ LEADING(record_batch_events topic_partitions) */

-- Alternative: Use explicit join hints
SELECT /*+ USE_NL(e tp) */ e.*, tp.*
FROM record_batch_events e
INNER JOIN topic_partitions tp ON e.topic_id = tp.topic_id AND e.partition = tp.partition
WHERE e.processed = false;
```

### 7. Database Configuration Optimizations

Optimize PostgreSQL settings for the workload:

```sql
-- Increase work_mem for complex queries
SET work_mem = '256MB';

-- Optimize for batch processing
SET effective_cache_size = '4GB';
SET shared_buffers = '1GB';

-- Reduce checkpoint frequency for bulk operations
SET checkpoint_segments = 32;
SET checkpoint_completion_target = 0.9;
```

### 8. Connection Pool and Transaction Management

```go
// Optimize connection pool settings
func configureDB(db *gorm.DB) {
    sqlDB, _ := db.DB()

    // Optimize for concurrent materialization
    sqlDB.SetMaxOpenConns(25)
    sqlDB.SetMaxIdleConns(10)
    sqlDB.SetConnMaxLifetime(time.Hour)

    // Use shorter transactions where possible
    db.Config.PrepareStmt = true
    db.Config.Logger = logger.Default.LogMode(logger.Silent) // Reduce logging overhead
}

// Implement transaction retry logic
func (m *GormMetastore) MaterializeWithRetry(nRecords int32, maxRetries int) error {
    for i := 0; i < maxRetries; i++ {
        err := m.MaterializeRecordBatchEvents(nRecords)
        if err == nil {
            return nil
        }

        // Check if error is retryable (deadlock, timeout, etc.)
        if isRetryableError(err) {
            time.Sleep(time.Duration(i*100) * time.Millisecond) // Exponential backoff
            continue
        }

        return err
    }
    return fmt.Errorf("materialization failed after %d retries", maxRetries)
}
```

## Implementation Priority

### Phase 1: Low-Risk, High-Impact (Immediate)

1. **Add composite indexes** - Can be done without code changes
2. **Optimize batch size** - Simple parameter tuning
3. **Database configuration** - Infrastructure-level improvements

### Phase 2: Medium-Risk, High-Impact (Short-term)

1. **Partition-aware batching** - Requires query modification
2. **Connection pool optimization** - Application-level changes
3. **Transaction retry logic** - Error handling improvements

### Phase 3: Higher-Risk, High-Impact (Long-term)

1. **Incremental offset calculation** - Significant query rewrite
2. **Hot/cold path separation** - Architecture changes
3. **Query plan optimization** - Database-specific tuning

## Expected Performance Improvements

| Optimization             | Expected Improvement | Risk Level | Implementation Effort |
| ------------------------ | -------------------- | ---------- | --------------------- |
| Index Optimizations      | 30-50%               | Low        | Low                   |
| Partition-Aware Batching | 20-40%               | Medium     | Medium                |
| Incremental Calculation  | 40-60%               | Medium     | High                  |
| Hot/Cold Path Separation | 25-35%               | Medium     | High                  |
| Database Configuration   | 15-25%               | Low        | Low                   |
| Connection Pool Tuning   | 10-20%               | Low        | Low                   |

## Monitoring and Metrics

Track these metrics to measure optimization effectiveness:

```go
type MaterializationMetrics struct {
    ProcessingTimeMs     int64
    EventsProcessed      int64
    PartitionsProcessed  int32
    LockWaitTimeMs       int64
    TransactionSizeBytes int64
    ErrorRate            float64
}

// Add monitoring to materialization function
func (m *GormMetastore) MaterializeRecordBatchEventsWithMetrics(nRecords int32) (*MaterializationMetrics, error) {
    start := time.Now()

    // Execute materialization
    err := m.MaterializeRecordBatchEvents(nRecords)

    metrics := &MaterializationMetrics{
        ProcessingTimeMs: time.Since(start).Milliseconds(),
        // ... collect other metrics
    }

    return metrics, err
}
```

## Testing Strategy

1. **Benchmark existing performance** before implementing changes
2. **Load testing** with realistic data volumes and concurrency
3. **A/B testing** between old and new implementations
4. **Monitoring** in production with gradual rollout

## Conclusion

These optimizations focus on improving the existing materialization approach
while respecting the contiguous offset requirement. The key is to work with the
constraint rather than around it, making the current approach more efficient
through better indexing, smarter batching, and optimized query execution.

Start with low-risk, high-impact optimizations (indexes and configuration) and
gradually implement more complex changes based on measured performance
improvements.
