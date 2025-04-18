package integration

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/markberger/yaks/internal/agent"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/suite"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type IntegrationTestsSuite struct {
	suite.Suite
	*metastore.TestDB
}

func (s *IntegrationTestsSuite) SetupSuite() {
	s.TestDB = metastore.NewTestDB()
}

func (s *IntegrationTestsSuite) TearDownSuite() {
	s.TestDB.Close()
}

func TestIntegrationsTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestsSuite))
}

func (s *IntegrationTestsSuite) NewAgent() *agent.Agent {
	db := s.TestDB.InitDB()
	metastore := metastore.NewGormMetastore(db)
	err := metastore.ApplyMigrations()
	if err != nil {
		log.Fatalf("failed to apply migrations: %v", err)
	}

	// Create agent and start serving
	agent := agent.NewAgent(db, "localhost", 9092)
	agent.AddHandlers()

	ctx, cancelFn := context.WithCancel(context.Background())
	go agent.ListenAndServe(ctx)
	s.T().Cleanup(func() {
		cancelFn()
		time.Sleep(200 * time.Millisecond)
	})

	return agent
}

func NewConfluentAdminClient() *ckafka.AdminClient {
	config := &ckafka.ConfigMap{"bootstrap.servers": "localhost:9092"}
	adminClient, err := ckafka.NewAdminClient(config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}

	return adminClient
}
