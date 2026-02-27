package cache

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/markberger/yaks/internal/s3_client"
)

// CachedS3Client wraps an S3Client and caches GetObject calls via groupcache.
// PutObject calls pass through to the underlying client unchanged.
type CachedS3Client struct {
	inner s3_client.S3Client
	cache *Cache
}

func NewCachedS3Client(inner s3_client.S3Client, cache *Cache) *CachedS3Client {
	return &CachedS3Client{inner: inner, cache: cache}
}

func (c *CachedS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return c.inner.PutObject(ctx, params, optFns...)
}

func (c *CachedS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	data, err := c.cache.Get(ctx, *params.Key)
	if err != nil {
		return nil, err
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}
