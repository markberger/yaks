package integration

import (
	"context"
	"crypto/rand"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/markberger/yaks/integration/container"
	"github.com/markberger/yaks/internal/agent"
	"github.com/markberger/yaks/internal/metastore"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestMain(m *testing.M) {
	_ = container.GetTestContainer()

	code := m.Run()

	container.TerminateTestContainer()
	os.Exit(code)
}

func NewAgent(t *testing.T) *agent.Agent {
	dbName, err := container.GetNewDatabase()
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	port, err := container.GetContainerPort()
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}
	config := metastore.Config{
		Host:     "localhost",
		Port:     port,
		User:     "testuser",
		Password: "testpassword",
		DBName:   dbName,
		SSLMode:  "disable",
	}
	agent, err := agent.NewAgent(config, "localhost", 9092)
	agent.ApplyMigrations()
	agent.AddHandlers()
	if err != nil {
		t.Fatalf("failed to create agent: %v", err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	go agent.ListenAndServe(ctx)
	t.Cleanup(func() {
		cancelFn()
		time.Sleep(500 * time.Millisecond)
	})

	return agent
}

func NewConfluentAdminClient(t *testing.T) *ckafka.AdminClient {
	config := &ckafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	adminClient, err := ckafka.NewAdminClient(config)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	return adminClient
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(n int) (string, error) {
	result := make([]byte, n)
	for i := range result {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		result[i] = letters[num.Int64()]
	}
	return string(result), nil
}
