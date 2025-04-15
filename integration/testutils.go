package integration

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/markberger/yaks/internal/agent"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

var dbContainer testcontainers.Container

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start the container once for all tests
	var err error
	dbContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:latest",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_PASSWORD": "password",
				"POSTGRES_DB":       "testdb",
			},
			WaitingFor: wait.ForListeningPort("5432/tcp"),
		},
		Started: true,
	})
	if err != nil {
		log.Fatalf("failed to start postgres container: %v", err)
	}
	time.Sleep(2 * time.Second)

	code := m.Run()

	dbContainer.Terminate(ctx)
	os.Exit(code)
}

func NewAgent(t *testing.T) *agent.Agent {
	suffix, err := randomString(8)
	if err != nil {
		t.Fatalf("failed to generate random db name: %v", err)
	}

	ctx := context.Background()
	mappedPort, err := dbContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	dbName := fmt.Sprintf("testdb-%s", suffix)
	config := metastore.Config{
		Host:     "localhost",
		Port:     mappedPort.Int(),
		User:     "testuser",
		Password: "testpassword",
		DBName:   dbName,
		SSLMode:  "false",
	}
	agent, err := agent.NewAgent(config, "localhost", 9092)
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
