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
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ProduceRequestHandler struct {
	metastore  metastore.Metastore
	bucketName string
	s3Client   S3Client
}

func NewProduceRequestHandler(m metastore.Metastore) *ProduceRequestHandler {
	s3Client := CreateS3Client(DefaultS3Config())
	return NewProduceRequestHandlerWithS3(m, "test-bucket", s3Client)
}

// NewProduceRequestHandlerWithS3 creates a handler with injectable S3 client for testing
func NewProduceRequestHandlerWithS3(m metastore.Metastore, bucketName string, s3Client S3Client) *ProduceRequestHandler {
	return &ProduceRequestHandler{
		metastore:  m,
		bucketName: bucketName,
		s3Client:   s3Client,
	}
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

	// Structure to track batch information for mapping results back to responses
	type batchInfo struct {
		batch          metastore.RecordBatchV2
		topicIndex     int
		partitionIndex int
		numRecords     int32
	}

	var allBatches []metastore.RecordBatchV2
	var batchInfos []batchInfo

	// Phase 1: Process all topics/partitions, validate, upload to S3, and collect batches
	for topicIndex, topic := range request.Topics {
		var topicResponse kmsg.ProduceResponseTopic
		topicResponse.Topic = topic.Topic

		metaTopic := h.metastore.GetTopicByName(topic.Topic)

		for partitionIndex, partition := range topic.Partitions {
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

			// Collect batch for later database commit
			batch := metastore.RecordBatchV2{
				TopicID:   metaTopic.ID,
				Partition: partition.Partition,
				NRecords:  int64(recordBatch.NumRecords),
				S3Key:     key,
			}
			allBatches = append(allBatches, batch)
			batchInfos = append(batchInfos, batchInfo{
				batch:          batch,
				topicIndex:     topicIndex,
				partitionIndex: partitionIndex,
				numRecords:     recordBatch.NumRecords,
			})

			// Add successful partition response (will be updated with base offset later)
			partitionResponse.ErrorCode = 0
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		response.Topics = append(response.Topics, topicResponse)
	}

	// Phase 2: Commit all batches to database in a single transaction (if we have any successful batches)
	if len(allBatches) > 0 {
		batchCommitOutputs, err := h.metastore.CommitRecordBatchesV2(allBatches)
		if err != nil {
			return nil, fmt.Errorf("record commit failed: %v", err)
		}

		if len(batchCommitOutputs) != len(allBatches) {
			return nil, fmt.Errorf("record commit returned unexpected number of results: expected %d, got %d", len(allBatches), len(batchCommitOutputs))
		}

		// Phase 3: Map database results back to partition responses
		for i, output := range batchCommitOutputs {
			batchInfo := batchInfos[i]

			// Find the corresponding partition response and update it with the base offset
			topicResponse := &response.Topics[batchInfo.topicIndex]
			partitionResponse := &topicResponse.Partitions[batchInfo.partitionIndex]
			partitionResponse.BaseOffset = output.BaseOffset
		}
	}

	return &response, nil
}
