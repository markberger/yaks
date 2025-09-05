# MaterializeRecordBatchEvents Benchmark Suite

This benchmark suite provides comprehensive performance testing for the
`MaterializeRecordBatchEvents` function to support query optimization efforts.

## Overview

The benchmark suite tests various scenarios and configurations to help identify
performance bottlenecks and measure the impact of optimizations outlined in
`QUERY_OPTIMIZATIONS.md`.

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

## Running Benchmarks

### Basic Usage

```bash
# Run all MaterializeRecordBatchEvents benchmarks
go test -bench=BenchmarkMaterializeRecordBatchEvents -benchmem ./internal/metastore

# Run specific scenario
go test -bench=BenchmarkMaterializeRecordBatchEvents_Medium -benchmem ./internal/metastore

# Run batch size optimization tests
go test -bench=BenchmarkMaterializeRecordBatchEvents_BatchSize -benchmem ./internal/metastore
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

- **Events Processed**: Number of RecordBatchEvents materialized
- **Partitions Updated**: Number of TopicPartitions with updated end_offset
- **Record Batches Created**: Number of RecordBatchV2 records inserted
- **Execution Time**: Actual MaterializeRecordBatchEvents execution time
- **Events/Second**: Throughput metric for performance comparison

## Sample Output

```
BenchmarkMaterializeRecordBatchEvents_BatchSize1000-8   	       1	  19305584 ns/op	   13384 B/op	      58 allocs/op
--- BENCH: BenchmarkMaterializeRecordBatchEvents_BatchSize1000-8
    metastore_bench_test.go:265: Scenario: BatchSize1000
    metastore_bench_test.go:266: Events Processed: 1000
    metastore_bench_test.go:267: Partitions Updated: 20
    metastore_bench_test.go:268: Record Batches Created: 1000
    metastore_bench_test.go:269: Execution Time: 19ms
    metastore_bench_test.go:273: Events/Second: 52631.58
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

### Custom Metrics

Extend `BenchmarkMetrics` struct and `collectBenchmarkMetrics` function to track
additional performance indicators specific to your optimizations.

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
