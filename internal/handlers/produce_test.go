package handlers

import (
	"context"
	"errors"
	"hash/crc32"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// MockS3Client implements S3Client interface for testing
type MockS3Client struct {
	mock.Mock
}

func (m *MockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*s3.PutObjectOutput), args.Error(1)
}

// Test helper functions
func createValidProduceRequest(topicName string, partition int32, records []byte) *kmsg.ProduceRequest {
	request := kmsg.NewProduceRequest()
	request.SetVersion(3)

	topic := kmsg.ProduceRequestTopic{
		Topic: topicName,
		Partitions: []kmsg.ProduceRequestTopicPartition{
			{
				Partition: partition,
				Records:   records,
			},
		},
	}
	request.Topics = []kmsg.ProduceRequestTopic{topic}

	return &request
}

func createValidRecordBatch() []byte {
	// Create a minimal valid record batch with proper CRC calculation
	// We need to manually calculate CRC and Length fields

	// Create a single record with proper Length calculation
	record := &kmsg.Record{
		Attributes:     0,
		TimestampDelta: 0,
		OffsetDelta:    0,
		Key:            nil,
		Value:          []byte("test-value"),
		Headers:        nil,
	}

	// First serialize the record to calculate its length
	var recordBytes []byte
	recordBytes = record.AppendTo(recordBytes)

	// Set the record length (excluding the length field itself)
	record.Length = int32(len(recordBytes) - 4) // Subtract 4 bytes for the length field itself

	// Re-serialize with correct length
	recordBytes = nil
	recordBytes = record.AppendTo(recordBytes)

	// Create record batch with minimal required fields
	batch := &kmsg.RecordBatch{
		FirstOffset:          0,
		PartitionLeaderEpoch: 0,
		Magic:                2,
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       0,
		MaxTimestamp:         0,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           1,
		Records:              recordBytes,
	}

	// Serialize the batch without CRC and Length to calculate them
	var batchBytes []byte
	batchBytes = batch.AppendTo(batchBytes)

	// Calculate the length of everything after the Length field (excluding CRC and Length fields)
	// The wire format is: FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4) + Magic(1) + CRC(4) + ...
	// We need Length = size of everything after the Length field
	lengthFieldValue := int32(len(batchBytes) - 12) // Subtract FirstOffset(8) + Length(4)

	// Calculate CRC of everything after the CRC field using Castagnoli polynomial
	// CRC covers everything after the CRC field
	crcData := batchBytes[16:] // Skip FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4)
	crcValue := int32(crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli)))

	// Set the calculated values
	batch.Length = lengthFieldValue
	batch.CRC = crcValue

	// Final serialization with correct CRC and Length
	batchBytes = nil
	batchBytes = batch.AppendTo(batchBytes)

	return batchBytes
}

func (s *HandlersTestSuite) TestProduceRequestHandler_Success() {
	// Setup
	metastore := s.TestDB.InitMetastore()
	err := metastore.CreateTopicV2("test-topic-success", 1)
	require.NoError(s.T(), err)

	mockS3 := &MockS3Client{}
	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)

	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Create valid request
	records := createValidRecordBatch()
	request := createValidProduceRequest("test-topic-success", 0, records)

	// Execute
	response_, err := handler.Handle(request)

	// Verify
	require.NoError(s.T(), err)
	response, ok := response_.(*kmsg.ProduceResponse)
	require.True(s.T(), ok)
	require.Len(s.T(), response.Topics, 1)
	require.Equal(s.T(), "test-topic-success", response.Topics[0].Topic)
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	require.Equal(s.T(), int16(0), response.Topics[0].Partitions[0].ErrorCode)
	require.GreaterOrEqual(s.T(), response.Topics[0].Partitions[0].BaseOffset, int64(0))

	mockS3.AssertExpectations(s.T())
}

func (s *HandlersTestSuite) TestProduceRequestHandler_TransactionRejection() {
	// Setup
	metastore := s.TestDB.InitMetastore()
	mockS3 := &MockS3Client{}
	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Create request with transaction ID
	request := kmsg.NewProduceRequest()
	transactionID := "test-transaction"
	request.TransactionID = &transactionID

	// Execute
	_, err := handler.Handle(&request)

	// Verify
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "does not support transactional producers")
}

func (s *HandlersTestSuite) TestProduceRequestHandler_InvalidTopic() {
	// Setup
	metastore := s.TestDB.InitMetastore()
	mockS3 := &MockS3Client{}
	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Create request for non-existent topic
	records := createValidRecordBatch()
	request := createValidProduceRequest("non-existent-topic", 0, records)

	// Execute
	response_, err := handler.Handle(request)

	// Verify
	require.NoError(s.T(), err)
	response, ok := response_.(*kmsg.ProduceResponse)
	require.True(s.T(), ok)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	require.Equal(s.T(), kerr.UnknownTopicOrPartition.Code, response.Topics[0].Partitions[0].ErrorCode)
}

func (s *HandlersTestSuite) TestProduceRequestHandler_InvalidPartition() {
	// Setup
	metastore := s.TestDB.InitMetastore()
	err := metastore.CreateTopicV2("test-topic", 1) // Only partition 0 exists
	require.NoError(s.T(), err)

	mockS3 := &MockS3Client{}
	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Create request for invalid partition
	records := createValidRecordBatch()
	request := createValidProduceRequest("test-topic", 5, records) // Partition 5 doesn't exist

	// Execute
	response_, err := handler.Handle(request)

	// Verify
	require.NoError(s.T(), err)
	response, ok := response_.(*kmsg.ProduceResponse)
	require.True(s.T(), ok)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	require.Equal(s.T(), kerr.UnknownTopicOrPartition.Code, response.Topics[0].Partitions[0].ErrorCode)
}

func (s *HandlersTestSuite) TestProduceRequestHandler_CorruptMessage() {
	// Setup
	metastore := s.TestDB.InitMetastore()
	err := metastore.CreateTopicV2("test-topic", 1)
	require.NoError(s.T(), err)

	mockS3 := &MockS3Client{}
	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Create request with corrupt records
	corruptRecords := []byte{0x00, 0x01, 0x02} // Invalid record batch
	request := createValidProduceRequest("test-topic", 0, corruptRecords)

	// Execute
	response_, err := handler.Handle(request)

	// Verify
	require.NoError(s.T(), err)
	response, ok := response_.(*kmsg.ProduceResponse)
	require.True(s.T(), ok)
	require.Len(s.T(), response.Topics, 1)
	require.Len(s.T(), response.Topics[0].Partitions, 1)
	require.Equal(s.T(), kerr.CorruptMessage.Code, response.Topics[0].Partitions[0].ErrorCode)
}

func (s *HandlersTestSuite) TestProduceRequestHandler_S3Failure() {
	// Setup
	metastore := s.TestDB.InitMetastore()
	err := metastore.CreateTopicV2("test-topic-s3-fail", 1)
	require.NoError(s.T(), err)

	mockS3 := &MockS3Client{}
	mockS3.On("PutObject", mock.Anything, mock.Anything).Return((*s3.PutObjectOutput)(nil), errors.New("S3 upload failed"))

	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Create valid request
	records := createValidRecordBatch()
	request := createValidProduceRequest("test-topic-s3-fail", 0, records)

	// Execute
	_, err = handler.Handle(request)

	// Verify
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "S3 upload failed")

	mockS3.AssertExpectations(s.T())
}

// Mock metastore that fails on CommitRecordBatchesV2
type mockMetastoreCommitFails struct {
	metastore.GormMetastore
}

func (m *mockMetastoreCommitFails) CommitRecordBatchesV2(batches []metastore.RecordBatchV2) ([]metastore.BatchCommitOutputV2, error) {
	return nil, errors.New("metastore commit failed")
}

func (s *HandlersTestSuite) TestProduceRequestHandler_MetastoreFailure() {
	// Setup
	wrappedMetastore := s.TestDB.InitMetastore()
	err := wrappedMetastore.CreateTopicV2("test-topic-metastore-fail", 1)
	require.NoError(s.T(), err)

	// Create mock metastore that fails on commit
	metastore := &mockMetastoreCommitFails{*wrappedMetastore}

	mockS3 := &MockS3Client{}
	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)

	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Create valid request
	records := createValidRecordBatch()
	request := createValidProduceRequest("test-topic-metastore-fail", 0, records)

	// Execute
	_, err = handler.Handle(request)

	// Verify
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "record commit failed")

	mockS3.AssertExpectations(s.T())
}

func (s *HandlersTestSuite) TestProduceRequestHandler_ResponseMapping() {
	// Setup
	metastore := s.TestDB.InitMetastore()
	err := metastore.CreateTopicV2("test-topic-mapping", 2) // Create topic with 2 partitions
	require.NoError(s.T(), err)

	mockS3 := &MockS3Client{}
	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)

	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Create request with multiple partitions
	records := createValidRecordBatch()
	request := kmsg.NewProduceRequest()
	request.SetVersion(3)

	topic := kmsg.ProduceRequestTopic{
		Topic: "test-topic-mapping",
		Partitions: []kmsg.ProduceRequestTopicPartition{
			{Partition: 0, Records: records},
			{Partition: 1, Records: records},
		},
	}
	request.Topics = []kmsg.ProduceRequestTopic{topic}

	// Execute
	response_, err := handler.Handle(&request)

	// Verify
	require.NoError(s.T(), err)
	response, ok := response_.(*kmsg.ProduceResponse)
	require.True(s.T(), ok)
	require.Len(s.T(), response.Topics, 1)
	require.Equal(s.T(), "test-topic-mapping", response.Topics[0].Topic)
	require.Len(s.T(), response.Topics[0].Partitions, 2)

	// Verify both partitions have correct responses
	for i, partition := range response.Topics[0].Partitions {
		require.Equal(s.T(), int32(i), partition.Partition)
		require.Equal(s.T(), int16(0), partition.ErrorCode)
		require.GreaterOrEqual(s.T(), partition.BaseOffset, int64(0)) // Base offset should be >= 0
	}

	mockS3.AssertExpectations(s.T())
}

func (s *HandlersTestSuite) TestProduceRequestHandler_HandlerInterface() {
	// Setup
	metastore := s.TestDB.InitMetastore()
	mockS3 := &MockS3Client{}
	handler := NewProduceRequestHandlerWithS3(metastore, "test-bucket", mockS3)

	// Verify interface methods
	require.Equal(s.T(), kmsg.Produce, handler.Key())
	require.Equal(s.T(), int16(3), handler.MinVersion())
	require.Equal(s.T(), int16(3), handler.MaxVersion())
}
