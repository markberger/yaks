package broker

import (
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type ApiVersionsRequest struct {
	kmsg.ApiVersionsRequest
}

func (v *ApiVersionsRequest) ReadFrom(src []byte) error {
	log.Info("got here")
	v.Default()
	b := kbin.Reader{Src: src}
	version := v.Version
	_ = version
	isFlexible := version >= 3
	_ = isFlexible
	s := v
	if version >= 3 {
		var v string
		b.Src = b.Src[1:]
		if isFlexible {
			v = CompactString(&b)
		} else {
			v = b.String()
		}
		s.ClientSoftwareName = v
		log.Info("ClientSoftwareName: ", s.ClientSoftwareName)
	}
	if version >= 3 {
		var v string
		if isFlexible {
			v = CompactString(&b)
		} else {
			v = b.String()
		}
		s.ClientSoftwareVersion = v
		log.Info("ClientSoftwareVersion: ", s.ClientSoftwareVersion)
	}
	if isFlexible {
		s.UnknownTags = internalReadTags(&b)
	}
	return b.Complete()
}

func internalReadTags(b *kbin.Reader) kmsg.Tags {
	var t kmsg.Tags
	n := Uvarint(b)
	log.Info("n tags: ", n)
	for num := n; num > 0; num-- {
		key, size := Uvarint(b), Uvarint(b)
		t.Set(key, b.Span(int(size)))
	}
	log.Info(t)
	return t
}

func Uvarint(b *kbin.Reader) uint32 {
	val, n := kbin.Uvarint(b.Src)
	log.Info("val: ", val, "  n: ", n)
	if n <= 0 {
		b.Src = nil
		return 0
	}
	b.Src = b.Src[n:]
	return val
}

func CompactString(b *kbin.Reader) string {
	v := Uvarint(b)
	log.Info("Uvarint: ", v)
	if v <= 1 {
		return ""
	}
	return string(b.Span(int(v) - 1))
}
