package handlers

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type FetchRequestHandler struct {
	metastore  metastore.Metastore
	s3Client   s3_client.S3Client
	bucketName string
	maxBytes   int32
}

func NewFetchRequestHandler(metastore metastore.Metastore, s3Client s3_client.S3Client, bucketName string, maxBytes int32) *FetchRequestHandler {
	return &FetchRequestHandler{metastore, s3Client, bucketName, maxBytes}
}

func (h *FetchRequestHandler) Key() kmsg.Key     { return kmsg.Fetch }
func (h *FetchRequestHandler) MinVersion() int16 { return 3 }
func (h *FetchRequestHandler) MaxVersion() int16 { return 3 }
func (h *FetchRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.FetchRequest)

	response := kmsg.NewFetchResponse()
	response.SetVersion(request.GetVersion())
	response.ThrottleMillis = 0

	effectiveMaxBytes := h.maxBytes
	if request.MaxBytes > 0 && request.MaxBytes < effectiveMaxBytes {
		effectiveMaxBytes = request.MaxBytes
	}

	var totalResponseBytes int32

	for _, t := range request.Topics {
		responseTopic := kmsg.NewFetchResponseTopic()
		responseTopic.Topic = t.Topic

		// Build high water mark map from topic partitions
		hwmMap := make(map[int32]int64)
		topicPartitions, err := h.metastore.GetTopicPartitions(t.Topic)
		if err != nil {
			return nil, err
		}
		for _, tp := range topicPartitions {
			hwmMap[tp.Partition] = tp.EndOffset
		}

		for _, p := range t.Partitions {
			batchesMetadata, err := h.metastore.GetRecordBatchesV2(t.Topic, p.Partition, p.FetchOffset)
			if err != nil {
				return nil, err
			}

			responsePartition := kmsg.NewFetchResponseTopicPartition()
			responsePartition.Partition = p.Partition
			responsePartition.ErrorCode = 0
			responsePartition.HighWatermark = hwmMap[p.Partition]

			remainingBudget := effectiveMaxBytes - totalResponseBytes
			partitionLimit := remainingBudget
			if p.PartitionMaxBytes > 0 && p.PartitionMaxBytes < partitionLimit {
				partitionLimit = p.PartitionMaxBytes
			}

			var recordBatches []byte
			var partitionBytes int32
			for i, metadata := range batchesMetadata {
				batchSize := int32(metadata.ByteLength)
				if i > 0 {
					if partitionBytes+batchSize > partitionLimit {
						break
					}
					if totalResponseBytes+partitionBytes+batchSize > effectiveMaxBytes {
						break
					}
				}

				data, err := h.downloadBatch(metadata.S3Key, metadata.ByteOffset, metadata.ByteLength)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch batch %s: %w", metadata.S3Key, err)
				}

				// Set RecordBatch.FirstOffset for consumer
				binary.BigEndian.PutUint64(data, uint64(metadata.StartOffset))
				recordBatches = append(recordBatches, data...)
				partitionBytes += batchSize
			}

			totalResponseBytes += partitionBytes
			responsePartition.RecordBatches = recordBatches
			responseTopic.Partitions = append(responseTopic.Partitions, responsePartition)
		}

		response.Topics = append(response.Topics, responseTopic)
	}
	return &response, nil
}

func (h *FetchRequestHandler) downloadBatch(key string, byteOffset, byteLength int64) ([]byte, error) {
	resp, err := h.s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &h.bucketName,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}
	return data[byteOffset : byteOffset+byteLength], nil
}
