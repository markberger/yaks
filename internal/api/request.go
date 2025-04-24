package api

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Represents the metadata attached to every API request:
// https://kafka.apache.org/protocol#protocol_messages
type RequestHeader struct {
	key           int16
	version       int16
	correlationID int32
	clientID      string
	tags          kmsg.Tags
}

func (r *RequestHeader) Key() int16           { return r.key }
func (r *RequestHeader) KeyName() string      { return kmsg.NameForKey(r.key) }
func (r *RequestHeader) Version() int16       { return r.version }
func (r *RequestHeader) CorrelationID() int32 { return r.correlationID }
func (r *RequestHeader) ClientID() string     { return r.clientID }

func (r *RequestHeader) ReadFrom(b *kbin.Reader) error {
	r.key = b.Int16()
	r.version = b.Int16()
	r.correlationID = b.Int32()
	s := b.NullableString()
	if s != nil {
		r.clientID = *s
	}

	headerVersion, err := HeaderVersionForRequest(r.key, r.version)
	if err != nil {
		return err
	}
	if headerVersion >= 2 {
		r.tags = internalReadTags(b)
	}
	return nil
}

func internalReadTags(b *kbin.Reader) kmsg.Tags {
	var t kmsg.Tags
	n := b.Uvarint()
	for num := n; num > 0; num-- {
		key, size := b.Uvarint(), b.Uvarint()
		t.Set(key, b.Span(int(size)))
	}
	return t
}

func HeaderVersionForRequest(key int16, requestVersion int16) (int16, error) {
	switch key {
	case int16(kmsg.Produce):
		if requestVersion >= 9 {
			return 2, nil
		} else {
			return 1, nil
		}
	case int16(kmsg.ApiVersions):
		if requestVersion >= 3 {
			return 2, nil
		} else {
			return 1, nil
		}
	case int16(kmsg.Metadata):
		if requestVersion >= 9 {
			return 2, nil
		} else {
			return 1, nil
		}
	case int16(kmsg.CreateTopics):
		if requestVersion >= 5 {
			return 2, nil
		} else {
			return 1, nil
		}
	case int16(kmsg.Fetch):
		if requestVersion >= 12 {
			return 2, nil
		} else {
			return 1, nil
		}
	default:
		return 0, fmt.Errorf("unknown header version for key: %v", kmsg.NameForKey(key))
	}
}
