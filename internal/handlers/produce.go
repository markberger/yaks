package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/markberger/yaks/internal/metastore"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ProduceRequestHandler struct {
	metastore  metastore.Metastore
	bucketName string
	s3Client   *s3.Client
}

func NewProduceRequestHandler(m metastore.Metastore) *ProduceRequestHandler {
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

	return &ProduceRequestHandler{m, "test-bucket", client}
}
func (h *ProduceRequestHandler) Key() kmsg.Key     { return kmsg.Produce }
func (h *ProduceRequestHandler) MinVersion() int16 { return 3 }
func (h *ProduceRequestHandler) MaxVersion() int16 { return 3 }
func (h *ProduceRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.ProduceRequest)
	if request.TransactionID != nil {
		return nil, errors.New("ProduceRequestHandler does not support transactional producers")
	}

	response := kmsg.NewProduceResponse()
	response.SetVersion(request.GetVersion())
	response.ThrottleMillis = 0

	for _, topic := range request.Topics {
		var topicResponse kmsg.ProduceResponseTopic
		topicResponse.Topic = topic.Topic

		metaTopic := h.metastore.GetTopicByName(topic.Topic)

		for _, partition := range topic.Partitions {
			partitionResponse := kmsg.NewProduceResponseTopicPartition()
			partitionResponse.Partition = partition.Partition

			// Error response if (1) topic is missing or (2) partition is out of range
			if metaTopic == nil || partition.Partition < 0 || partition.Partition >= metaTopic.NPartitions {
				log.WithFields(log.Fields{"topic": topic.Topic, "partition": partition.Partition, "metaTopic": metaTopic}).Info("Invalid TopicPartition")
				partitionResponse.ErrorCode = kerr.UnknownTopicOrPartition.Code
				topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
				continue
			}

			// Parse RecordBatch
			var recordBatch kmsg.RecordBatch
			if err := recordBatch.ReadFrom(partition.Records); err != nil {
				partitionResponse.ErrorCode = kerr.CorruptMessage.Code
				topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
				continue
			}

			// Push batch to s3
			// TODO: add date partition
			key := fmt.Sprintf("topics/%s/%s.batch", topic.Topic, uuid.New().String())
			size := int64(len(partition.Records))
			reader := bytes.NewReader(partition.Records)
			input := &s3.PutObjectInput{
				Bucket:        aws.String(h.bucketName), // Use the bucket name directly
				Key:           aws.String(key),          // object key/path
				Body:          reader,                   // io.Reader
				ContentLength: &size,                    // must match reader size
				ContentType:   aws.String("application/octet-stream"),
			}
			_, err := h.s3Client.PutObject(context.Background(), input)
			if err != nil {
				return nil, err
			}

			// Register RecordBatches with metastore
			batchCommitInputs := []metastore.RecordBatchV2{
				{
					TopicID:   metaTopic.ID,
					Partition: partition.Partition,
					NRecords:  int64(recordBatch.NumRecords),
					S3Key:     key,
				},
			}
			batchCommitOutputs, err := h.metastore.CommitRecordBatchesV2(batchCommitInputs)
			if err != nil || len(batchCommitOutputs) != 1 {
				return nil, fmt.Errorf("record commit failed: %v", err)
			}

			// Add response details
			log.Infof("record batch has %d records", recordBatch.NumRecords)
			partitionResponse.ErrorCode = 0
			partitionResponse.BaseOffset = batchCommitOutputs[0].BaseOffset
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		response.Topics = append(response.Topics, topicResponse)
	}

	return &response, nil
}
