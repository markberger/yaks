package s3_client

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client interface abstracts S3 operations for testing
type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// RealS3Client wraps the actual AWS S3 client
type RealS3Client struct {
	client *s3.Client
}

func (r *RealS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return r.client.PutObject(ctx, params, optFns...)
}

func (r *RealS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return r.client.GetObject(ctx, params, optFns...)
}

// NewRealS3Client creates a new RealS3Client
func NewRealS3Client(client *s3.Client) *RealS3Client {
	return &RealS3Client{client: client}
}
