package handlers

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/markberger/yaks/internal/metastore"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type FetchRequestHandler struct {
	metastore  metastore.Metastore
	s3Client   *s3.Client
	bucketName *string
}

func NewFetchRequestHandler(metastore metastore.Metastore) *FetchRequestHandler {
	ctx := context.Background()
	localstackEndpoint := "http://localhost:4566"

	awsAccessKeyID := "test"
	awsSecretAccessKey := "test"
	staticCredentialsProvider := credentials.NewStaticCredentialsProvider(awsAccessKeyID, awsSecretAccessKey, "")

	// Load config with handcoded creds
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(staticCredentialsProvider),
	)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Create s3Client. Override endpoint and force path style for Localstack
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(localstackEndpoint)
		o.UsePathStyle = true
	})
	bucketName := "test-bucket"
	return &FetchRequestHandler{metastore, client, &bucketName}
}

func (h *FetchRequestHandler) Key() kmsg.Key     { return kmsg.Fetch }
func (h *FetchRequestHandler) MinVersion() int16 { return 3 }
func (h *FetchRequestHandler) MaxVersion() int16 { return 3 }
func (h *FetchRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.FetchRequest)

	response := kmsg.NewFetchResponse()
	response.SetVersion(request.GetVersion())
	response.ThrottleMillis = 0

	// Identify all S3 files to download
	// TODO: respect byte size cuttoff when returning responses
	var batchesMetadata []metastore.RecordBatchV2
	for _, t := range request.Topics {
		for _, p := range t.Partitions {
			recordBatches, err := h.metastore.GetRecordBatchesV2(t.Topic, p.FetchOffset)
			if err != nil {
				return nil, err
			}

			batchesMetadata = append(batchesMetadata, recordBatches...)
		}

		// TODO: support multiple topics
		break
	}

	// TODO: support multiple topics
	responseTopic := kmsg.NewFetchResponseTopic()
	responseTopic.Topic = request.Topics[0].Topic
	responsePartition := kmsg.NewFetchResponseTopicPartition()
	responsePartition.Partition = 0
	responsePartition.ErrorCode = 0

	var recordBatches []byte
	for _, metadata := range batchesMetadata {
		// Download the object and read into memory
		resp, err := h.s3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: h.bucketName,
			Key:    &metadata.S3Key,
		})
		if err != nil {
			log.Fatalf("failed to GetObject, %v", err)
		}
		defer resp.Body.Close()

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("failed to read object body, %v", err)
		}

		// Set RecordBatch.FirstOffset for consumer
		binary.BigEndian.PutUint64(data, uint64((metadata.StartOffset)))
		recordBatches = append(recordBatches, data...)
	}
	responsePartition.RecordBatches = recordBatches
	responseTopic.Partitions = append(responseTopic.Partitions, responsePartition)
	response.Topics = append(response.Topics, responseTopic)
	return &response, nil
}
