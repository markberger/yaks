package broker

import (
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func AppendResponse(
	dst []byte,
	response kmsg.Response,
	correlationID int32,
) []byte {
	// Reserve 4 bytes at beginning for size
	dst = append(dst, 0, 0, 0, 0)
	dst = kbin.AppendInt32(dst, correlationID)

	// Flexible responses (response header v1) require a tagged fields section
	// after the correlation ID. ApiVersions is exempt — it always uses header v0
	// so clients can parse it before version negotiation.
	if response.IsFlexible() && response.Key() != int16(kmsg.ApiVersions) {
		dst = append(dst, 0) // varint 0 = no tagged fields
	}

	dst = response.AppendTo(dst)
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(dst)-4))
	return dst
}
