# yaks: Yet Another Kafka on S3

⚠️ yaks is a work-in-progress and not production-tested software. Use at your
own risk. ⚠️

yaks is a diskless Kafka server that stores data directly to S3. It is inspired
by similar platforms like [Warpstream](https://www.warpstream.com/) and
[AutoMQ](https://www.automq.com/).

At the cost of increased end-to-end latency, Kafka on S3 provides:

- **Cost savings**: S3 storage is significantly cheaper than EBS with high IOPS
- **No disk management**: Simply scale up and down the number of agents as load
  on the service changes
- **Pain-free topic retention**: Retaining active topics for 30+ days is natural

Because of these trade-offs, yaks is naturally suited to high throughput and
latency-tolerant applications like logs, event souring, and CDC pipelines.

The system is designed with simplicity in mind. Only a postgres database and S3
bucket are required.

<p align="center">
  <img src="./docs/imgs/overview_diagram.png" />
</p>

See the [System Design](./docs/SYSTEM_DESIGN.md) doc for how yaks works.

## Quick Start

Start a two node deployment on your local machine with postgres and localstack.

Both agents are behind an nginx proxy serving on `localhost:9092`:

```bash
docker compose --profile multi-agent up
```

## Tests

```bash
# docker daemon is required
# hardcoded infra setup requires running tests serially
go test -p=1 ./...
```

## Project Layout

```
.
├── cmd/
│   └── agent        # run yaks-agent
├── integration/     # integration tests with testcontainers
└── internal/
    ├── agent/       # yaks-agent
    ├── api/         # kafka api logic + serialization
    ├── broker/      # generic kafka broker implementation
    ├── buffer/      # buffer to group messages before upload
    ├── config/      # see config env below for settings
    ├── handlers/    # kafka request handlers
    ├── metastore/   # store metadata in postgres
    └── s3_client/   # s3 client helper
```

## Configuration

All settings are configured via environment variables prefixed with `YAKS_`.

### Broker

| Variable                       | Default     | Description                                                        |
| ------------------------------ | ----------- | ------------------------------------------------------------------ |
| `YAKS_BROKER_HOST`             | `0.0.0.0`   | Host address the broker listens on                                 |
| `YAKS_BROKER_PORT`             | `9092`      | Port the broker listens on                                         |
| `YAKS_ADVERTISED_HOST`         | `localhost` | Host advertised to clients in metadata responses                   |
| `YAKS_ADVERTISED_PORT`         | `0`         | Port advertised to clients (falls back to `YAKS_BROKER_PORT` if 0) |
| `YAKS_RUN_MIGRATIONS`          | `false`     | Run database migrations on startup                                 |
| `YAKS_FLUSH_INTERVAL_MS`       | `250`       | Interval (ms) before flushing pending batches to S3                |
| `YAKS_FLUSH_BYTES`             | `5242880`   | Byte threshold before flushing batches to S3                       |
| `YAKS_MATERIALIZE_INTERVAL_MS` | `100`       | Interval (ms) between materializing batches                        |
| `YAKS_MATERIALIZE_BATCH_SIZE`  | `1000`      | Number of records to materialize per batch                         |
| `YAKS_FETCH_MAX_BYTES`         | `52428800`  | Maximum bytes returned in a single fetch response                  |

### PostgreSQL

| Variable           | Default        | Description       |
| ------------------ | -------------- | ----------------- |
| `YAKS_DB_HOST`     | `localhost`    | Database host     |
| `YAKS_DB_PORT`     | `5432`         | Database port     |
| `YAKS_DB_USER`     | `testuser`     | Database user     |
| `YAKS_DB_PASSWORD` | `testpassword` | Database password |
| `YAKS_DB_NAME`     | `testdb`       | Database name     |
| `YAKS_DB_SSLMODE`  | `disable`      | SSL mode          |

### S3

| Variable             | Default                 | Description                                         |
| -------------------- | ----------------------- | --------------------------------------------------- |
| `YAKS_S3_ENDPOINT`   | `http://localhost:4566` | S3 endpoint URL                                     |
| `YAKS_S3_REGION`     | `us-east-1`             | AWS region                                          |
| `YAKS_S3_ACCESS_KEY` | `test`                  | Access key ID                                       |
| `YAKS_S3_SECRET_KEY` | `test`                  | Secret access key                                   |
| `YAKS_S3_PATH_STYLE` | `true`                  | Use path-style URLs instead of virtual-hosted-style |
| `YAKS_S3_BUCKET`     | `test-bucket`           | Bucket name for storing message batches             |

## Supported API Keys

| API Key         | Min Version | Max Version |
| --------------- | ----------- | ----------- |
| ApiVersions     | 0           | 4           |
| Metadata        | 0           | 7           |
| CreateTopics    | 2           | 4           |
| Produce         | 3           | 3           |
| Fetch           | 3           | 4           |
| FindCoordinator | 0           | 2           |
| ListOffsets     | 1           | 4           |
| OffsetCommit    | 2           | 7           |
| OffsetFetch     | 1           | 5           |

## Future Work

yaks is a MVP implementation and requires additional work to be cost-effective
and production ready. Some things I'd like to get around to implementing are:

- Use [`groupcache`](https://github.com/golang/groupcache) to cache S3 objects
  on disk to reduce API calls
- Compact record batches on S3 in the background for efficient whole-topic
  retrieval
- Health checks, statsd metrics, tracing

## Related Resources

This project would not be possible without these great resources:

- [Official Kafka protocol guide](https://kafka.apache.org/42/design/protocol/)

- [franz-go](https://github.com/twmb/franz-go): Go library for api message
  serialization - [_Travis Bischel_](https://github.com/twmb)

- [Kafka protocol practical guide](https://ivanyu.me/blog/2024/09/08/kafka-protocol-practical-guide/) -
  [_Ivan Yurchenko_](https://github.com/ivanyu)

- [Warpstream docs](https://docs.warpstream.com/warpstream/overview/architecture)

- [AutoMQ wiki](https://github.com/AutoMQ/automq/wiki/What-is-automq:-Overview)
