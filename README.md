# yaks: Yet Another Kafka on S3

yaks is a kafka compatible server which stores message batches in S3. It is
inspired by similar platforms like [Warpstream](https://www.warpstream.com/) and
[Redpanda](https://www.redpanda.com/).

The system is designed with simplicity in mind. Only a postgres database and S3
bucket are required.

<p align="center">
  <img src="./docs/overview_diagram.png" />
</p>

## Tests

```sh
# docker daemon is required
go test ./...
```

## Project Layout

```
internal/
├── api/        # Kafka api logic + serialization
├── broker/     # Generic Kafka broker implementation
├── server/     # yaks-agent
└── metastore/  # Store meta data in postgres
```
