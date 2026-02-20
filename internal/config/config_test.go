package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0", cfg.BrokerHost)
	assert.Equal(t, int32(9092), cfg.BrokerPort)

	assert.Equal(t, "localhost", cfg.DB.Host)
	assert.Equal(t, 5432, cfg.DB.Port)
	assert.Equal(t, "testuser", cfg.DB.User)
	assert.Equal(t, "testpassword", cfg.DB.Password)
	assert.Equal(t, "testdb", cfg.DB.DBName)
	assert.Equal(t, "disable", cfg.DB.SSLMode)

	assert.Equal(t, "http://localhost:4566", cfg.S3.Endpoint)
	assert.Equal(t, "us-east-1", cfg.S3.Region)
	assert.Equal(t, "test", cfg.S3.AccessKeyID)
	assert.Equal(t, "test", cfg.S3.SecretAccessKey)
	assert.True(t, cfg.S3.UsePathStyle)
	assert.Equal(t, "test-bucket", cfg.S3.Bucket)
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("YAKS_BROKER_HOST", "10.0.0.1")
	t.Setenv("YAKS_BROKER_PORT", "19092")
	t.Setenv("YAKS_DB_HOST", "db.prod")
	t.Setenv("YAKS_DB_PORT", "5433")
	t.Setenv("YAKS_S3_BUCKET", "prod-bucket")
	t.Setenv("YAKS_S3_PATH_STYLE", "false")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "10.0.0.1", cfg.BrokerHost)
	assert.Equal(t, int32(19092), cfg.BrokerPort)
	assert.Equal(t, "db.prod", cfg.DB.Host)
	assert.Equal(t, 5433, cfg.DB.Port)
	assert.Equal(t, "prod-bucket", cfg.S3.Bucket)
	assert.False(t, cfg.S3.UsePathStyle)

	// Unset fields still get defaults
	assert.Equal(t, "testuser", cfg.DB.User)
	assert.Equal(t, "us-east-1", cfg.S3.Region)
}
