package oracle

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type PartKey struct {
	Topic     string
	Partition int32
}

type ConsumedRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

type recordPayload struct {
	PID  int    `json:"pid"`
	Seq  int    `json:"seq"`
	Data string `json:"data"`
}

type Config struct {
	Brokers           []string
	NumTopics         int
	NumPartitions     int
	NumProducers      int
	RecordsPerProducer int
	TopicPrefix       string
	ConsumeTimeout    time.Duration
	DataDir           string
	Logger            func(string, ...interface{})
}

type Result struct {
	TotalRecords  int
	NumPartitions int
	NumProducers  int
	TopicNames    []string
}

func DefaultConfig() Config {
	return Config{
		Brokers:           []string{"localhost:9092"},
		NumTopics:         1,
		NumPartitions:     1,
		NumProducers:      100,
		RecordsPerProducer: 100,
		TopicPrefix:       "oracle-topic",
		ConsumeTimeout:    60 * time.Second,
		Logger:            func(format string, args ...interface{}) { fmt.Printf(format+"\n", args...) },
	}
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Logger == nil {
		cfg.Logger = func(string, ...interface{}) {}
	}

	// Generate unique topic names
	suffix := randomHex(4)
	topicNames := make([]string, cfg.NumTopics)
	for i := range topicNames {
		topicNames[i] = fmt.Sprintf("%s-%d-%s", cfg.TopicPrefix, i, suffix)
	}

	// Create topics
	cfg.Logger("[oracle] Creating topics: %v", topicNames)
	if err := createTopics(ctx, cfg, topicNames); err != nil {
		return Result{}, fmt.Errorf("create topics: %w", err)
	}
	cfg.Logger("[oracle] Topics created")

	// Open SQLite oracle
	dataDir := cfg.DataDir
	if dataDir == "" {
		var err error
		dataDir, err = os.MkdirTemp("", "oracle-*")
		if err != nil {
			return Result{}, fmt.Errorf("create temp dir: %w", err)
		}
		defer os.RemoveAll(dataDir)
	}

	oracleDB, err := openOracle(dataDir)
	if err != nil {
		return Result{}, fmt.Errorf("open oracle db: %w", err)
	}
	defer oracleDB.Close()

	// Concurrent produce
	cfg.Logger("[oracle] Launching %d producers, %d records each", cfg.NumProducers, cfg.RecordsPerProducer)
	expectedCounts, err := produce(ctx, cfg, topicNames, oracleDB)
	if err != nil {
		return Result{}, fmt.Errorf("produce: %w", err)
	}
	cfg.Logger("[oracle] All producers finished. Expected counts: %v", expectedCounts)

	// Consume all partitions
	cfg.Logger("[oracle] Consuming all partitions")
	allConsumed, err := consumeAll(ctx, cfg, expectedCounts)
	if err != nil {
		return Result{}, fmt.Errorf("consume: %w", err)
	}

	// Verify
	if err := verify(oracleDB, expectedCounts, allConsumed, cfg.Logger); err != nil {
		return Result{}, err
	}

	totalRecords := 0
	for _, records := range allConsumed {
		totalRecords += len(records)
	}

	cfg.Logger("[oracle] All 6 checks passed!")
	cfg.Logger("[oracle] Total records verified: %d across %d partitions from %d producers",
		totalRecords, len(expectedCounts), cfg.NumProducers)

	return Result{
		TotalRecords:  totalRecords,
		NumPartitions: len(expectedCounts),
		NumProducers:  cfg.NumProducers,
		TopicNames:    topicNames,
	}, nil
}

func openOracle(dir string) (*sql.DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}
	dbPath := filepath.Join(dir, "oracle.db")
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s", dbPath))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE produced_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		producer_id INTEGER NOT NULL,
		topic TEXT NOT NULL,
		partition INTEGER NOT NULL,
		sequence INTEGER NOT NULL,
		key BLOB,
		value BLOB NOT NULL
	)`)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func generateRecordValue(producerID, sequence int) []byte {
	nBytes := 32 + (sequence % 97)
	randBytes := make([]byte, nBytes)
	_, _ = rand.Read(randBytes)

	payload := recordPayload{
		PID:  producerID,
		Seq:  sequence,
		Data: hex.EncodeToString(randBytes),
	}
	b, _ := json.Marshal(payload)
	return b
}

func insertOracleRecord(db *sql.DB, mu *sync.Mutex, producerID int, topic string, partition int32, seq int, key, value []byte) error {
	mu.Lock()
	defer mu.Unlock()
	_, err := db.Exec(
		"INSERT INTO produced_records (producer_id, topic, partition, sequence, key, value) VALUES (?, ?, ?, ?, ?, ?)",
		producerID, topic, partition, seq, key, value,
	)
	return err
}
