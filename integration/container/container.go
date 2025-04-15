package container

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"

	//nolint:revive // Driver registration
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Establish a singleton for our postgres database
var dbContainer testcontainers.Container
var once sync.Once

func GetTestContainer() testcontainers.Container {
	// Start the container once for all tests
	once.Do(func() {
		ctx := context.Background()

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "postgres:17-alpine",
				ExposedPorts: []string{"5432/tcp"},
				Env: map[string]string{
					"POSTGRES_USER":     "testuser",
					"POSTGRES_PASSWORD": "testpassword",
					"POSTGRES_DB":       "testdb",
				},
				WaitingFor: wait.ForListeningPort("5432/tcp"),
			},
			Started: true,
		})
		if err != nil {
			log.Fatalf("failed to start postgres container: %v", err)
		}

		dbContainer = container
	})
	return dbContainer
}

func GetContainerHost() (string, error) {
	_ = GetTestContainer() // Ensure container is initialized
	ctx := context.Background()
	if dbContainer == nil {
		return "", fmt.Errorf("test container is nil after initialization attempt")
	}
	host, err := dbContainer.Host(ctx)
	if err != nil {
		return "", err
	}
	return host, nil
}

func GetContainerPort() (int, error) {
	_ = GetTestContainer() // Ensure container is initialized
	ctx := context.Background()
	if dbContainer == nil {
		return 0, fmt.Errorf("test container is nil after initialization attempt")
	}
	mappedPort, err := dbContainer.MappedPort(ctx, "5432")
	if err != nil {
		return 0, err
	}
	return mappedPort.Int(), nil
}

// Hand out a new database in the same postgres container. This way we only have to do
// setup and teardown once.
func GetNewDatabase() (string, error) {
	host, err := GetContainerHost()
	if err != nil {
		return "", fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := GetContainerPort()
	if err != nil {
		return "", fmt.Errorf("failed to get container port: %w", err)
	}

	suffix, err := randomString(8)
	if err != nil {
		return "", fmt.Errorf("failed to generate random string: %w", err)
	}
	dbName := fmt.Sprintf("testdb_%s", suffix)

	dsn := fmt.Sprintf("host=%s port=%d user=testuser password=testpassword dbname=testdb sslmode=disable", host, mappedPort)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return "", fmt.Errorf("sql.Open failed: %w", err)
	}
	defer db.Close()

	createDbQuery := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	_, err = db.Exec(createDbQuery)
	if err != nil {
		return "", fmt.Errorf("failed to execute CREATE DATABASE for '%s': %w", dbName, err)
	}

	return dbName, nil
}

func TerminateTestContainer() error {
	if dbContainer != nil {
		err := dbContainer.Terminate(context.Background())
		if err != nil {
			log.Printf("Error terminating container: %v", err)
			return err
		}
		return nil
	}
	return nil
}
