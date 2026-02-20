package s3_client

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
	Endpoint        string `env:"YAKS_S3_ENDPOINT"   envDefault:"http://localhost:4566"`
	Region          string `env:"YAKS_S3_REGION"     envDefault:"us-east-1"`
	AccessKeyID     string `env:"YAKS_S3_ACCESS_KEY" envDefault:"test"`
	SecretAccessKey string `env:"YAKS_S3_SECRET_KEY" envDefault:"test"`
	UsePathStyle    bool   `env:"YAKS_S3_PATH_STYLE" envDefault:"true"`
	Bucket          string `env:"YAKS_S3_BUCKET"     envDefault:"test-bucket"`
}

// CreateRawS3Client creates a raw *s3.Client from the given configuration
func CreateRawS3Client(cfg S3ClientConfig) *s3.Client {
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

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.UsePathStyle
	})
}

// CreateS3Client creates an S3 client with the given configuration
func CreateS3Client(cfg S3ClientConfig) S3Client {
	return NewRealS3Client(CreateRawS3Client(cfg))
}
