package handlers

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
)

// S3ClientConfig holds configuration for S3 client creation
type S3ClientConfig struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	UsePathStyle    bool
}

// DefaultS3Config returns the default LocalStack configuration
func DefaultS3Config() S3ClientConfig {
	return S3ClientConfig{
		Endpoint:        "http://localhost:4566",
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		UsePathStyle:    true,
	}
}

// CreateS3Client creates an S3 client with the given configuration
func CreateS3Client(cfg S3ClientConfig) S3Client {
	ctx := context.Background()

	staticCredentialsProvider := credentials.NewStaticCredentialsProvider(
		cfg.AccessKeyID,
		cfg.SecretAccessKey,
		"",
	)

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(staticCredentialsProvider),
	)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.UsePathStyle
	})

	return NewRealS3Client(client)
}
