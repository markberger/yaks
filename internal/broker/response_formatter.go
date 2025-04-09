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
	//dst = kbin.AppendBytes(dst, []byte{0})

	dst = response.AppendTo(dst)
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(dst)-4))
	return dst
}
