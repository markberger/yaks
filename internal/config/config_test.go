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
	assert.Equal(t, "localhost", cfg.AdvertisedHost)
	assert.Equal(t, int32(0), cfg.AdvertisedPort)
	assert.Equal(t, int32(9092), cfg.GetAdvertisedPort())

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

	// Groupcache defaults
	assert.False(t, cfg.Groupcache.Enabled)
	assert.Equal(t, int32(9080), cfg.Groupcache.Port)
	assert.Equal(t, "", cfg.Groupcache.AdvertisedHost)
	assert.Equal(t, int64(512), cfg.Groupcache.CacheSizeMB)
	assert.Equal(t, 5000, cfg.Groupcache.HeartbeatInterval)
	assert.Equal(t, 10000, cfg.Groupcache.PollInterval)
	assert.Equal(t, 15000, cfg.Groupcache.LeaseTTL)
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("YAKS_BROKER_HOST", "10.0.0.1")
	t.Setenv("YAKS_BROKER_PORT", "19092")
	t.Setenv("YAKS_ADVERTISED_HOST", "public.example.com")
	t.Setenv("YAKS_ADVERTISED_PORT", "19092")
	t.Setenv("YAKS_DB_HOST", "db.prod")
	t.Setenv("YAKS_DB_PORT", "5433")
	t.Setenv("YAKS_S3_BUCKET", "prod-bucket")
	t.Setenv("YAKS_S3_PATH_STYLE", "false")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "10.0.0.1", cfg.BrokerHost)
	assert.Equal(t, int32(19092), cfg.BrokerPort)
	assert.Equal(t, "public.example.com", cfg.AdvertisedHost)
	assert.Equal(t, int32(19092), cfg.AdvertisedPort)
	assert.Equal(t, int32(19092), cfg.GetAdvertisedPort())
	assert.Equal(t, "db.prod", cfg.DB.Host)
	assert.Equal(t, 5433, cfg.DB.Port)
	assert.Equal(t, "prod-bucket", cfg.S3.Bucket)
	assert.False(t, cfg.S3.UsePathStyle)

	// Unset fields still get defaults
	assert.Equal(t, "testuser", cfg.DB.User)
	assert.Equal(t, "us-east-1", cfg.S3.Region)
}

func TestGroupcacheConfigEnvOverrides(t *testing.T) {
	t.Setenv("YAKS_GROUPCACHE_ENABLED", "true")
	t.Setenv("YAKS_GROUPCACHE_PORT", "9081")
	t.Setenv("YAKS_GROUPCACHE_ADVERTISED_HOST", "cache-node-1")
	t.Setenv("YAKS_GROUPCACHE_CACHE_SIZE_MB", "1024")
	t.Setenv("YAKS_GROUPCACHE_HEARTBEAT_MS", "3000")
	t.Setenv("YAKS_GROUPCACHE_POLL_MS", "8000")
	t.Setenv("YAKS_GROUPCACHE_LEASE_TTL_MS", "20000")

	cfg, err := Load()
	require.NoError(t, err)

	assert.True(t, cfg.Groupcache.Enabled)
	assert.Equal(t, int32(9081), cfg.Groupcache.Port)
	assert.Equal(t, "cache-node-1", cfg.Groupcache.AdvertisedHost)
	assert.Equal(t, int64(1024), cfg.Groupcache.CacheSizeMB)
	assert.Equal(t, 3000, cfg.Groupcache.HeartbeatInterval)
	assert.Equal(t, 8000, cfg.Groupcache.PollInterval)
	assert.Equal(t, 20000, cfg.Groupcache.LeaseTTL)
}
