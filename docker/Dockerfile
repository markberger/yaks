FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o yaks ./cmd/agent

FROM alpine:3.21

COPY --from=builder /app/yaks /usr/local/bin/yaks
ENTRYPOINT ["yaks"]
