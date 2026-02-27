package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/markberger/yaks/internal/oracle"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated broker addresses")
	numTopics := flag.Int("topics", 1, "number of topics to create")
	numPartitions := flag.Int("partitions", 1, "number of partitions per topic")
	numProducers := flag.Int("producers", 100, "number of concurrent producers")
	recordsPerProducer := flag.Int("records", 100, "records per producer")
	topicPrefix := flag.String("topic-prefix", "oracle-topic", "prefix for topic names")
	consumeTimeout := flag.Duration("consume-timeout", 60*time.Second, "timeout for consuming all records")
	dataDir := flag.String("data-dir", "", "directory for oracle SQLite DB (temp dir if empty)")
	flag.Parse()

	cfg := oracle.DefaultConfig()
	cfg.Brokers = strings.Split(*brokers, ",")
	cfg.NumTopics = *numTopics
	cfg.NumPartitions = *numPartitions
	cfg.NumProducers = *numProducers
	cfg.RecordsPerProducer = *recordsPerProducer
	cfg.TopicPrefix = *topicPrefix
	cfg.ConsumeTimeout = *consumeTimeout
	cfg.DataDir = *dataDir

	result, err := oracle.Run(context.Background(), cfg)
	if err != nil {
		log.Fatalf("oracle failed: %v", err)
	}

	fmt.Printf("OK: %d records verified across %d partitions from %d producers\n",
		result.TotalRecords, result.NumPartitions, result.NumProducers)
	fmt.Printf("Topics: %v\n", result.TopicNames)
	os.Exit(0)
}
