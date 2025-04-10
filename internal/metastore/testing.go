package metastore

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
)

// TestDB wraps the test database container and configuration
type TestDB struct {
	Container testcontainers.Container
	Config    Config
	DB        *gorm.DB
}

// NewTestDB creates a new PostgreSQL container and returns a configured test database
func NewTestDB(t *testing.T) *TestDB {
	ctx := context.Background()

	// Start PostgreSQL container
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine", // Using alpine for smaller image
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_DB":       "test",
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	// Get host and mapped port
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %s", err)
	}

	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %s", err)
	}

	// Create config for test database
	cfg := Config{
		Host:     host,
		Port:     mappedPort.Int(),
		User:     "test",
		Password: "test",
		DBName:   "test",
		SSLMode:  "disable",
	}

	// Wait a bit to ensure PostgreSQL is fully ready
	time.Sleep(time.Second)

	// Connect to database
	db, err := Connect(cfg)
	if err != nil {
		t.Fatalf("failed to connect to test database: %s", err)
	}

	// Auto-migrate the schema
	if err := db.AutoMigrate(&Topic{}, &RecordBatch{}); err != nil {
		t.Fatalf("failed to migrate test database: %s", err)
	}

	return &TestDB{
		Container: container,
		Config:    cfg,
		DB:        db,
	}
}

// Close terminates the test database container
func (tdb *TestDB) Close(t *testing.T) {
	ctx := context.Background()
	if err := tdb.Container.Terminate(ctx); err != nil {
		t.Errorf("failed to terminate container: %s", err)
	}
}
