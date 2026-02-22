# yaks: Yet Another Kafka on S3

⚠️ yaks is not production tested software. Use at your own risk. ⚠️

yaks is a diskless Kafka server that stores data directly to S3. It is inspired
by similar platforms like [Warpstream](https://www.warpstream.com/) and
[AutoMQ](https://www.automq.com/).

The system is designed with simplicity in mind. Only a postgres database and S3
bucket are required.

<p align="center">
  <img src="./docs/overview_diagram.png" />
</p>

## Quick Start

Start a two node deployment on your local machine with postgres and localstack.

Both agents are behind an nginx proxy serving on `localhost:9092`:

```bash
docker compose --profile multi-agent up
```

## Configuration

// list every env variable in config/ we can set here in a table

## Support Requests

// add a list of all kafka api types with versions supported

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

## Related Resources

This package would not be possible without these great resources:

- [Official Kafka protocol guide](https://kafka.apache.org/42/design/protocol/)

- [franz-go](https://github.com/twmb/franz-go): Go library for api message
  serialization - [_Travis Bischel_](https://github.com/twmb)

- [Kafka protocol practical guide](https://ivanyu.me/blog/2024/09/08/kafka-protocol-practical-guide/) -
  [_Ivan Yurchenko_](https://github.com/ivanyu)

- [Warpstream docs](https://docs.warpstream.com/warpstream/overview/architecture)

- [AutoMQ wiki](https://github.com/AutoMQ/automq/wiki/What-is-automq:-Overview)
