package integration

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
	"github.com/markberger/yaks/internal/agent"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type IntegrationTestsSuite struct {
	suite.Suite
	*metastore.TestDB
	*localstack.LocalStackContainer
}

func (s *IntegrationTestsSuite) SetupSuite() {
	ctx := context.Background()
	s.TestDB = metastore.NewTestDB()

	// Get init script path to create test-bucket
	scriptPath, err := filepath.Abs("init-aws.sh")
	if err != nil {
		log.Fatalf("failed to get absolute path for init script: %v", err)
	}
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		log.Fatalf("init script not found at %s", scriptPath)
	}

	// Start LocalStack with S3 and hardcoded to port 4566
	l, err := localstack.Run(
		ctx,
		"localstack/localstack:latest",
		testcontainers.WithEnv(map[string]string{
			"SERVICES": "s3",
		}),
		testcontainers.WithHostConfigModifier(
			func(hostConfig *container.HostConfig) {
				hostConfig.PortBindings = nat.PortMap{
					"4566/tcp": []nat.PortBinding{{
						HostIP:   "0.0.0.0",
						HostPort: "4566",
					}},
				}

				hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{
					Type:     mount.TypeBind,
					Source:   scriptPath,
					Target:   "/etc/localstack/init/ready.d/init-aws.sh",
					ReadOnly: false,
				})
			},
		),
	)
	if err != nil {
		log.Fatalf("failed to start LocalStack container: %v", err)
	}
	s.LocalStackContainer = l
	time.Sleep(2 * time.Second)
}

func (s *IntegrationTestsSuite) TearDownSuite() {
	s.TestDB.Close()

	ctx := context.Background()
	if err := s.LocalStackContainer.Terminate(ctx); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	}
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
