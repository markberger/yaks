package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestAppendResponse_Success(t *testing.T) {
	correlationID := int32(1)

	// Create a mock ApiVersionsResponse
	response := &kmsg.ApiVersionsResponse{
		ApiKeys: []kmsg.ApiVersionsResponseApiKey{
			{ApiKey: 1, MinVersion: 1, MaxVersion: 2},
		},
		ErrorCode: 0,
		Version:   2,
	}

	expectedOutput := []byte{
		0x00, 0x00, 0x00, 0x14, // Size of the response (20 bytes)
		0x00, 0x00, 0x00, 0x01, // Correlation ID (1)
		0x00, 0x00, // ErrorCode (0)
		0x00, 0x00, 0x00, 0x01, // ApiKeys Length (1)
		0x00, 0x01, // ApiKey (1)
		0x00, 0x01, // MinVersion (1)
		0x00, 0x02, // MaxVersion (2)
		0x00, 0x00, 0x00, 0x00, // ThrottleMillis
	}

	dst := []byte{}
	result := AppendResponse(dst, response, correlationID)

	// Compare the result with the expected output
	if string(result) != string(expectedOutput) {
		t.Errorf("\nExpected\t%v\ngot\t\t%v", expectedOutput, result)
	}
}

func TestAppendResponse_SuccessFlexible(t *testing.T) {
	correlationID := int32(1)
	response := &kmsg.ApiVersionsResponse{
		ApiKeys: []kmsg.ApiVersionsResponseApiKey{
			{ApiKey: 1, MinVersion: 1, MaxVersion: 3},
		},
		ErrorCode: 0,
		Version:   3,
	}
	expectedOutput := []byte{
		0x00, 0x00, 0x00, 0x1D, // Size of the response
		0x00, 0x00, 0x00, 0x01, // Correlation ID (1)
		0x00, 0x00, // ErrorCode (0)
		0x02,       // ApiKeys Length (1)
		0x00, 0x01, // ApiKey (1)
		0x00, 0x01, // MinVersion (1)
		0x00, 0x03, // MaxVersion (3)
		0x00,                   // ApiKey Tags
		0x00, 0x00, 0x00, 0x00, // ThrottleMillis
		0x01, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Feature tags
	}

	dst := []byte{}
	dst = AppendResponse(dst, response, correlationID)
	if string(dst) != string(expectedOutput) {
		t.Errorf("\nExpected\t%v\ngot\t\t%v", expectedOutput, dst)
	}
}
