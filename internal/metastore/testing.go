package metastore

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
)

const (
	TESTDB_USER = "testuser"
	TESTDB_PASS = "testpass"
	TESTDB_NAME = "testdb"
)

// TestDB wraps the test database container and configuration
type TestDB struct {
	Container testcontainers.Container
	Config    Config
	DB        *gorm.DB
}

// NewTestDB creates a new PostgreSQL container and returns a configured test database
func NewTestDB() *TestDB {
	ctx := context.Background()

	// Start PostgreSQL container
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine", // Using alpine for smaller image
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     TESTDB_USER,
			"POSTGRES_PASSWORD": TESTDB_PASS,
			"POSTGRES_DB":       TESTDB_NAME,
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}

	// Get host and mapped port
	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get container host: %s", err)
	}

	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		log.Fatalf("failed to get container port: %s", err)
	}

	// Create config for test database
	cfg := Config{
		Host:     host,
		Port:     mappedPort.Int(),
		User:     TESTDB_USER,
		Password: TESTDB_PASS,
		DBName:   TESTDB_NAME,
		SSLMode:  "disable",
	}

	// Wait a bit to ensure PostgreSQL is fully ready
	time.Sleep(500 * time.Millisecond)

	// Connect to database
	db, err := Connect(cfg)
	if err != nil {
		log.Fatalf("failed to connect to test database: %s", err)
	}

	return &TestDB{
		Container: container,
		Config:    cfg,
		DB:        db,
	}
}

// Create a new database in postgres and hand out the gorm connection
func (tdb *TestDB) InitDB() *gorm.DB {
	// Create db with random name
	dbName := fmt.Sprintf("testdb_%s", randomString(8))
	sqlCommand := fmt.Sprintf(`CREATE DATABASE "%s";`, dbName)
	tx := tdb.DB.Exec(sqlCommand)
	if tx.Error != nil {
		log.Fatalf("failed to initdb: %v", tx.Error)
	}

	// Connect to the testdb and return it to the test
	cfg := Config{
		Host:     tdb.Config.Host,
		Port:     tdb.Config.Port,
		User:     TESTDB_USER,
		Password: TESTDB_PASS,
		DBName:   dbName,
		SSLMode:  "disable",
	}
	db, err := Connect(cfg)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	return db
}

// Helper function for creating a Metastore out of the connection
// returned by InitDB()
func (tdb *TestDB) InitMetastore() *GormMetastore {
	testdb := tdb.InitDB()
	metastore := NewGormMetastore(testdb)
	err := metastore.ApplyMigrations()
	if err != nil {
		log.Fatalf("failed to apply migrations: %v", err)
	}

	return metastore
}

// Close terminates the test database container
func (tdb *TestDB) Close() {
	ctx := context.Background()
	if err := tdb.Container.Terminate(ctx); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	}
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(n int) string {
	result := make([]byte, n)
	for i := range result {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			log.Fatalf("could not generate random int: %v", err)
		}
		result[i] = letters[num.Int64()]
	}
	return string(result)
}
