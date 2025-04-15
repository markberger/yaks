package broker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/markberger/yaks/internal/api"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Broker struct {
	NodeID          int32
	Host            string
	Port            int32
	handlerRegistry *handlerRegistry
}

func NewBroker(nodeID int32, host string, port int32) *Broker {
	b := Broker{nodeID, host, port, NewHandlerRegistry()}
	b.Add(NewApiVersionsRequestHandler(b.handlerRegistry))
	return &b
}

// A wrapper function that simply calls HandlerRegistry.Add
func (b *Broker) Add(handler Handler) {
	b.handlerRegistry.Add(handler)
}

func (b *Broker) ListenAndServe(ctx context.Context) {
	// TCP listener
	address := fmt.Sprintf("%s:%d", b.Host, b.Port)
	log.WithFields(log.Fields{"host": b.Host, "port": b.Port}).Info("Listening...")
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// Shutdown goroutine because Accept blocks
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	// Accept connections and spawn goroutines to handle requests
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				log.Info("TCP listener closed due to ctx cancel")
				return
			default:
				log.WithError(err).Error("Accept error")
				continue
			}
		}

		log.WithFields(log.Fields{"client": conn.RemoteAddr()}).Info("Connection accepted")
		go func() {
			b.handleConn(ctx, conn)
			log.WithFields(log.Fields{"client": conn.RemoteAddr()}).Info("Connection closed")
		}()
	}
}

// TODO: should return error for easier testing?
func (b *Broker) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	sizeBuf := make([]byte, 4)
	for {
		// Check context before proceeding
		select {
		case <-ctx.Done():
			log.WithField("client", conn.RemoteAddr()).Info("ctx signaled - closing connection")
			return
		default:
		}

		// Read from connection
		if _, err := io.ReadFull(reader, sizeBuf); err != nil {
			log.Error("failed to read msg size: ", err)
			return
		}
		size := binary.BigEndian.Uint32(sizeBuf)
		body := make([]byte, size)
		_, err := io.ReadFull(reader, body)
		if err != nil {
			log.Error("failed to read request body from client connection")
			return
		}

		// Parse the bytes into api.RequestHeader
		reader := kbin.Reader{Src: body}
		header := &api.RequestHeader{}
		err = header.ReadFrom(&reader)
		if err != nil {
			log.Error("failed to parse request header: ", err)
			return
		}
		log.WithFields(
			log.Fields{
				"client":        conn.RemoteAddr(),
				"clientID":      header.ClientID(),
				"key":           header.KeyName(),
				"version":       header.Version(),
				"correlationID": header.CorrelationID(),
				//"bytes":         body,
			},
		).Info("Received request")

		// Parse the remaining bytes into the respective kmsg.Request struct
		request := kmsg.RequestForKey(header.Key())
		request.SetVersion(header.Version())
		if err := request.ReadFrom(reader.Src); err != nil {
			log.Error("failed to parse request: ", err, "\nrequest: ", request)
			return
		}

		// Run the appropriate Handler to generate a kmsg.Response
		handler := b.handlerRegistry.Get(request)
		if handler == nil {
			log.Error("failed to find appropriate handler")
			return
		}

		response, err := handler.Handle(request)
		if err != nil {
			log.Error("Handler returned an error")
			return
		}

		// Serialize the response and send it to the client
		dst := make([]byte, 0)
		dst = AppendResponse(dst, response, header.CorrelationID())
		log.WithFields(
			log.Fields{
				"client":        conn.RemoteAddr(),
				"key":           header.KeyName(),
				"version":       header.Version(),
				"correlationID": header.CorrelationID(),
				//"bytes":         dst,
			},
		).Info("Sending response")

		_, err = conn.Write(dst)
		if err != nil {
			log.Error("Error encountered when writing response")
			return
		}
		log.WithFields(
			log.Fields{
				"client":        conn.RemoteAddr(),
				"key":           header.KeyName(),
				"version":       header.Version(),
				"correlationID": header.CorrelationID(),
				//"bytes":         dst,
			},
		).Info("Response sent")
	}
}
