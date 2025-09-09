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

	// Determine whether request is "flexible" i.e. supports _tagged_fields
	// https://kafka.apache.org/protocol#protocol_messages
	request := kmsg.RequestForKey(r.key)
	if request == nil {
		return fmt.Errorf("api key has no associated request: %d", r.key)
	}
	request.SetVersion(r.version)
	if request.IsFlexible() {
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
