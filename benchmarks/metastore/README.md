# Metastore Benchmark Suite

This benchmark suite provides comprehensive performance testing for critical
metastore functions to support query optimization efforts. It includes support
for testing concurrent operations to measure scalability and contention
characteristics.

## Functions Tested

### MaterializeRecordBatchEvents

Tests the materialization of record batch events into the V2 system with
concurrent indexer support.

### CommitRecordBatchesV2

Tests the direct commit of record batches with multiple concurrent writers to
measure lock contention and throughput characteristics.

## Overview

The benchmark suite tests various scenarios and configurations to help identify
performance bottlenecks and measure the impact of optimizations outlined in
`QUERY_OPTIMIZATIONS.md`. The suite now supports concurrent indexer testing to
evaluate how multiple indexers perform when running simultaneously.

## Benchmark Scenarios

### Core Performance Benchmarks

- **Small**: 2 topics, 2 partitions each, 100 events (baseline testing)
- **Medium**: 5 topics, 4 partitions each, 1,000 events (typical production
  load)
- **Large**: 10 topics, 8 partitions each, 5,000 events (stress testing)

### Specialized Scenarios

- **HighContention**: Many small events (1-10 records each) to stress window
  functions
- **ExistingOffsets**: Tests performance with pre-existing partition data
- **MultiPartition**: 16 partitions per topic to test partition scaling
- **WindowFunctionStress**: 5,000 tiny events to stress window function
  performance

### Batch Size Optimization

Tests different `nRecords` parameter values:

- BatchSize100: Process 100 events at a time
- BatchSize500: Process 500 events at a time
- BatchSize1000: Process 1,000 events at a time
- BatchSize2000: Process 2,000 events at a time

### Concurrent Indexer Benchmarks

Tests multiple indexers running simultaneously to measure scalability and
contention:

- **Concurrent2Indexers**: 2 indexers processing events concurrently
- **Concurrent4Indexers**: 4 indexers processing events concurrently
- **Concurrent8Indexers**: 8 indexers processing events concurrently
- **HighContentionConcurrent**: 4 indexers with high contention scenario
- **MultiPartitionConcurrent**: 6 indexers with many partitions

These benchmarks leverage the `FOR UPDATE SKIP LOCKED` mechanism in
`MaterializeRecordBatchEvents` to safely process events concurrently without
conflicts.

## CommitRecordBatchesV2 Benchmark Scenarios

### Core Performance Benchmarks

- **SmallCommit**: 2 topics, 2 partitions each, 10 batches per commit (baseline
  testing)
- **MediumCommit**: 3 topics, 4 partitions each, 50 batches per commit (typical
  production load)
- **LargeCommit**: 5 topics, 8 partitions each, 200 batches per commit (stress
  testing)
- **ExistingOffsetsCommit**: Tests performance with pre-existing partition
  offsets

### Batch Size Optimization

Tests different numbers of batches per commit:

- **BatchSize10**: 10 batches per commit
- **BatchSize25**: 25 batches per commit
- **BatchSize100**: 100 batches per commit
- **BatchSize500**: 500 batches per commit

### Concurrent Writer Benchmarks - Lock Contention Testing

Tests multiple writers running simultaneously to measure lock contention and
throughput:

- **Concurrent2Writers**: 2 writers committing batches concurrently
- **Concurrent4Writers**: 4 writers committing batches concurrently
- **Concurrent8Writers**: 8 writers committing batches concurrently
- **Concurrent16Writers**: 16 writers committing batches concurrently

### Partition Strategy Benchmarks

Tests different partition targeting strategies to measure contention impact:

- **ConcentratedPartitions**: All writers target the same few partitions (high
  contention)
- **DistributedPartitions**: Writers spread across all available partitions (low
  contention)
- **RandomPartitions**: Writers randomly target partitions (mixed contention)

### Contention-Specific Scenarios

- **HighContention**: 4 writers targeting same partitions with small batches
- **LowContention**: 4 writers spread across many partitions
- **HighThroughputStress**: 8 writers with large batches across many partitions
- **SmallBatchHighFrequency**: 6 writers with very small, frequent commits

## Running Benchmarks

### Basic Usage

```bash
# Run all MaterializeRecordBatchEvents benchmarks
go test -bench=BenchmarkMaterializeRecordBatchEvents -benchmem ./benchmarks/metastore

# Run all CommitRecordBatchesV2 benchmarks
go test -bench=BenchmarkCommitRecordBatchesV2 -benchmem ./benchmarks/metastore

# Run specific scenario
go test -bench=BenchmarkMaterializeRecordBatchEvents_Medium -benchmem ./benchmarks/metastore
go test -bench=BenchmarkCommitRecordBatchesV2_Medium -benchmem ./benchmarks/metastore

# Run batch size optimization tests
go test -bench=BenchmarkMaterializeRecordBatchEvents_BatchSize -benchmem ./benchmarks/metastore
go test -bench=BenchmarkCommitRecordBatchesV2_BatchSize -benchmem ./benchmarks/metastore
```

### Performance Comparison

```bash
# Baseline measurement
go test -bench=BenchmarkMaterializeRecordBatchEvents_Medium -benchmem -count=5 ./internal/metastore > baseline.txt

# After implementing optimization
go test -bench=BenchmarkMaterializeRecordBatchEvents_Medium -benchmem -count=5 ./internal/metastore > optimized.txt

# Compare results (requires benchcmp tool)
benchcmp baseline.txt optimized.txt
```

### Concurrent Indexer Testing

```bash
# Run all concurrent indexer benchmarks
go test -bench=BenchmarkMaterializeRecordBatchEvents_Concurrent -benchmem ./benchmarks/metastore

# Test specific concurrency levels
go test -bench=BenchmarkMaterializeRecordBatchEvents_Concurrent2Indexers -benchmem -v ./benchmarks/metastore
go test -bench=BenchmarkMaterializeRecordBatchEvents_Concurrent4Indexers -benchmem -v ./benchmarks/metastore

# Compare single vs concurrent performance
go test -bench="BenchmarkMaterializeRecordBatchEvents_Medium|BenchmarkMaterializeRecordBatchEvents_Concurrent2Indexers" -benchmem ./benchmarks/metastore
```

### CommitRecordBatchesV2 Lock Contention Testing

```bash
# Run all concurrent writer benchmarks
go test -bench=BenchmarkCommitRecordBatchesV2_Concurrent -benchmem ./benchmarks/metastore

# Test specific writer concurrency levels
go test -bench=BenchmarkCommitRecordBatchesV2_Concurrent2Writers -benchmem -v ./benchmarks/metastore
go test -bench=BenchmarkCommitRecordBatchesV2_Concurrent4Writers -benchmem -v ./benchmarks/metastore

# Compare high vs low contention scenarios
go test -bench="BenchmarkCommitRecordBatchesV2_HighContention|BenchmarkCommitRecordBatchesV2_LowContention" -benchmem ./benchmarks/metastore

# Test partition strategy impact
go test -bench=BenchmarkCommitRecordBatchesV2_.*Partitions -benchmem ./benchmarks/metastore

# Stress test with many writers
go test -bench=BenchmarkCommitRecordBatchesV2_Concurrent16Writers -benchmem -v ./benchmarks/metastore
```

### Advanced Options

```bash
# Control benchmark duration
go test -bench=BenchmarkMaterializeRecordBatchEvents_Large -benchtime=10s ./internal/metastore

# Run single iteration for debugging
go test -bench=BenchmarkMaterializeRecordBatchEvents_Small -benchtime=1x ./internal/metastore

# Verbose output with detailed metrics
go test -bench=BenchmarkMaterializeRecordBatchEvents_Medium -benchmem -v ./internal/metastore
```

## Metrics Collected

### Standard Go Benchmark Metrics

- **Execution time** (ns/op): Time per benchmark iteration
- **Memory allocations** (B/op): Bytes allocated per operation
- **Allocation count** (allocs/op): Number of allocations per operation

### Custom Performance Metrics

#### MaterializeRecordBatchEvents Metrics

- **Events Processed**: Number of RecordBatchEvents materialized
- **Partitions Updated**: Number of TopicPartitions with updated end_offset
- **Record Batches Created**: Number of RecordBatchV2 records inserted
- **Execution Time**: Actual MaterializeRecordBatchEvents execution time
- **Events/Second**: Throughput metric for performance comparison

#### CommitRecordBatchesV2 Metrics

- **Total Commits**: Number of commit operations performed
- **Total Batches**: Number of RecordBatchV2 records committed
- **Total Records**: Total number of records across all batches
- **Partitions Updated**: Number of TopicPartitions with updated end_offset
- **Average Commit Latency**: Average time per commit operation (ms)
- **Batches/Second**: Batch commit throughput metric
- **Records/Second**: Record commit throughput metric
- **Concurrent Writers**: Number of writers running simultaneously
- **Partition Strategy**: Distribution strategy used
  (concentrated/distributed/random)

## Sample Output

### Single Indexer Benchmark

```
BenchmarkMaterializeRecordBatchEvents_BatchSize1000-8   	       1	  19305584 ns/op	   13384 B/op	      58 allocs/op
--- BENCH: BenchmarkMaterializeRecordBatchEvents_BatchSize1000-8
    metastore_bench_test.go:313: Scenario: BatchSize1000
    metastore_bench_test.go:314: Concurrent Indexers: 1
    metastore_bench_test.go:315: Events Processed: 1000
    metastore_bench_test.go:316: Partitions Updated: 20
    metastore_bench_test.go:317: Record Batches Created: 1000
    metastore_bench_test.go:318: Execution Time: 19ms
    metastore_bench_test.go:322: Events/Second: 52631.58
```

### Concurrent Indexer Benchmark

```
BenchmarkMaterializeRecordBatchEvents_Concurrent2Indexers-8   	      72	  16898543 ns/op	   52778 B/op	     345 allocs/op
--- BENCH: BenchmarkMaterializeRecordBatchEvents_Concurrent2Indexers-8
    metastore_bench_test.go:313: Scenario: Concurrent2Indexers
    metastore_bench_test.go:314: Concurrent Indexers: 2
    metastore_bench_test.go:315: Events Processed: 1000
    metastore_bench_test.go:316: Partitions Updated: 20
    metastore_bench_test.go:317: Record Batches Created: 1000
    metastore_bench_test.go:318: Execution Time: 16ms
    metastore_bench_test.go:322: Events/Second: 62500.00
```

### CommitRecordBatchesV2 Single Writer Benchmark

```
BenchmarkCommitRecordBatchesV2_Medium-8   	       1	  48269292 ns/op	 4706048 B/op	   32208 allocs/op
--- BENCH: BenchmarkCommitRecordBatchesV2_Medium-8
    metastore_bench_test.go:870: Scenario: MediumCommit
    metastore_bench_test.go:871: Concurrent Writers: 1
    metastore_bench_test.go:872: Partition Strategy: distributed
    metastore_bench_test.go:873: Total Commits: 100
    metastore_bench_test.go:874: Total Batches: 5000
    metastore_bench_test.go:875: Total Records: 1374200
    metastore_bench_test.go:876: Partitions Updated: 12
    metastore_bench_test.go:877: Execution Time: 48ms
    metastore_bench_test.go:878: Average Commit Latency: 0.48ms
    metastore_bench_test.go:881: Batches/Second: 104166.67
    metastore_bench_test.go:884: Records/Second: 28629166.67
```

### CommitRecordBatchesV2 Concurrent Writers Benchmark

```
BenchmarkCommitRecordBatchesV2_Concurrent2Writers-8   	       1	 372976499 ns/op	111289240 B/op	  634749 allocs/op
--- BENCH: BenchmarkCommitRecordBatchesV2_Concurrent2Writers-8
    metastore_bench_test.go:870: Scenario: Concurrent2Writers
    metastore_bench_test.go:871: Concurrent Writers: 2
    metastore_bench_test.go:872: Partition Strategy: distributed
    metastore_bench_test.go:873: Total Commits: 300
    metastore_bench_test.go:874: Total Batches: 15000
    metastore_bench_test.go:875: Total Records: 4116662
    metastore_bench_test.go:876: Partitions Updated: 12
    metastore_bench_test.go:877: Execution Time: 372ms
    metastore_bench_test.go:878: Average Commit Latency: 1.24ms
    metastore_bench_test.go:881: Batches/Second: 40322.58
    metastore_bench_test.go:884: Records/Second: 11066295.70
```

### CommitRecordBatchesV2 High Contention Benchmark

```
BenchmarkCommitRecordBatchesV2_HighContention-8   	       1	 333745500 ns/op	80670224 B/op	  478288 allocs/op
--- BENCH: BenchmarkCommitRecordBatchesV2_HighContention-8
    metastore_bench_test.go:870: Scenario: HighContentionCommit
    metastore_bench_test.go:871: Concurrent Writers: 4
    metastore_bench_test.go:872: Partition Strategy: concentrated
    metastore_bench_test.go:873: Total Commits: 400
    metastore_bench_test.go:874: Total Batches: 10000
    metastore_bench_test.go:875: Total Records: 299837
    metastore_bench_test.go:876: Partitions Updated: 2
    metastore_bench_test.go:877: Execution Time: 333ms
    metastore_bench_test.go:878: Average Commit Latency: 0.83ms
    metastore_bench_test.go:881: Batches/Second: 30030.03
    metastore_bench_test.go:884: Records/Second: 900411.41
```

## Interpreting Results

### Performance Indicators

1. **Events/Second**: Primary throughput metric - higher is better
2. **Execution Time**: Raw performance - lower is better
3. **Memory allocations**: Memory efficiency - lower is better
4. **Partitions Updated**: Indicates concurrency level and lock contention

### Optimization Targets

Based on initial benchmarks, focus optimization efforts on:

1. **Batch Size**: Optimal around 1000 events for throughput
2. **Window Function Performance**: Critical for scenarios with many small
   events
3. **Lock Contention**: Important for multi-partition scenarios
4. **Memory Usage**: Consistent ~13KB per operation regardless of batch size

## Benchmark Configuration

### Test Data Characteristics

- **Deterministic**: Uses fixed random seed (42) for reproducible results
- **Realistic**: Simulates real-world topic/partition distributions
- **Scalable**: Configurable event counts and record sizes
- **Isolated**: Each iteration uses fresh database to avoid interference

### Database Setup

- **PostgreSQL 15**: Uses testcontainers for consistent environment
- **Fresh Schema**: Each iteration gets clean database state
- **Connection Management**: Proper cleanup to prevent connection leaks
- **Transaction Isolation**: Each benchmark runs in isolated transaction

## Extending the Benchmark Suite

### Adding New Scenarios

```go
// Define new benchmark configuration
var CustomScenario = BenchmarkConfig{
    Name:               "Custom",
    NumTopics:          3,
    PartitionsPerTopic: 6,
    NumEvents:          750,
    MinRecordsPerEvent: 50,
    MaxRecordsPerEvent: 200,
    BatchSize:          375,
    WithExistingData:   false,
}

// Create benchmark function
func BenchmarkMaterializeRecordBatchEvents_Custom(b *testing.B) {
    runBenchmarkScenario(b, CustomScenario)
}
```

### Testing Query Optimizations

1. **Create optimization branch**: Implement query changes in separate function
2. **Add benchmark variant**: Create new benchmark that calls optimized function
3. **Compare results**: Run both benchmarks and compare metrics
4. **Validate correctness**: Ensure optimization produces same results

### Adding Concurrent Indexer Scenarios

```go
// Define concurrent benchmark configuration
var ConcurrentScenario = BenchmarkConfig{
    Name:               "ConcurrentCustom",
    NumTopics:          3,
    PartitionsPerTopic: 6,
    NumEvents:          1500,
    MinRecordsPerEvent: 50,
    MaxRecordsPerEvent: 200,
    BatchSize:          250,
    WithExistingData:   false,
    NumIndexers:        3, // Key field for concurrent testing
}

// Create concurrent benchmark function
func BenchmarkMaterializeRecordBatchEvents_ConcurrentCustom(b *testing.B) {
    runBenchmarkScenario(b, ConcurrentScenario)
}
```

### Custom Metrics

Extend `BenchmarkMetrics` struct and `collectBenchmarkMetrics` function to track
additional performance indicators specific to your optimizations.

## Concurrent Indexer Architecture

### How It Works

The concurrent indexer implementation leverages PostgreSQL's
`FOR UPDATE SKIP LOCKED` mechanism to safely distribute work among multiple
indexers:

1. **Work Distribution**: Each indexer calls `MaterializeRecordBatchEvents` with
   the same batch size
2. **Lock Avoidance**: `SKIP LOCKED` ensures indexers don't block each other
3. **Safe Concurrency**: No conflicts or duplicate processing
4. **Automatic Load Balancing**: Work is distributed based on availability

### Configuration Guidelines

- **NumIndexers**: Start with 2-4 indexers, increase based on CPU cores and
  database capacity
- **BatchSize**: May need adjustment for concurrent scenarios - smaller batches
  can improve distribution
- **Event Count**: Ensure sufficient events to benefit from concurrency
  (typically 2-3x the total batch size across all indexers)

### Performance Considerations

- **Database Connections**: Each indexer uses a database connection
- **Lock Contention**: More indexers may increase lock contention on partitions
- **Optimal Concurrency**: Test different indexer counts to find the sweet spot
- **Resource Usage**: Monitor CPU and database load during concurrent execution

## Best Practices

### Benchmark Reliability

1. **Multiple runs**: Use `-count=5` for statistical significance
2. **Stable environment**: Run on dedicated hardware when possible
3. **Consistent load**: Avoid running other intensive processes during
   benchmarks
4. **Warm-up**: First iteration may be slower due to cold caches

### Performance Analysis

1. **Baseline first**: Always establish baseline before optimizing
2. **Incremental changes**: Test one optimization at a time
3. **Regression testing**: Ensure optimizations don't break existing
   functionality
4. **Real-world validation**: Supplement benchmarks with production testing

### Troubleshooting

#### Connection Issues

If you see "too many clients" errors:

```bash
# Run with single iteration
go test -bench=BenchmarkMaterializeRecordBatchEvents_Small -benchtime=1x ./internal/metastore
```

#### Memory Issues

For large scenarios, monitor system memory:

```bash
# Reduce event counts in large scenarios
# Or run benchmarks individually rather than all at once
```

## Integration with CI/CD

Consider adding benchmark regression testing:

```bash
# In CI pipeline
go test -bench=BenchmarkMaterializeRecordBatchEvents_Medium -benchmem -count=3 ./internal/metastore > current.txt
# Compare with stored baseline and fail if performance degrades significantly
```

This benchmark suite provides the foundation for data-driven optimization of the
`MaterializeRecordBatchEvents` function, enabling you to measure the real-world
impact of the optimizations outlined in your query optimization strategy.
