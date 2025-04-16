package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/markberger/yaks/integration/container"
	"github.com/markberger/yaks/internal/agent"
	"github.com/markberger/yaks/internal/metastore"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestMain(m *testing.M) {
	container.GetTestContainer() // init singleton
	code := m.Run()
	container.TerminateTestContainer()
	os.Exit(code)
}

func NewAgent(t *testing.T) *agent.Agent {
	// Set up postgres database
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
	db, err := metastore.Connect(config)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	// Create agent and start serving
	agent := agent.NewAgent(db, "localhost", 9092)
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
