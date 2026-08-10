// Command stream_echo demonstrates a STREAM socket talking to a plain TCP peer.
//
//	go run ./examples/stream_echo
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/exergy-dev/gozmq"
)

func main() {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	stream, err := zctx.NewSocket(gozmq.Stream)
	check(err, "new STREAM")
	defer stream.Close()
	check(stream.Bind("tcp://127.0.0.1:0"), "bind")

	raw, err := net.Dial("tcp", strings.TrimPrefix(stream.LastEndpoint(), "tcp://"))
	check(err, "raw TCP dial")
	defer raw.Close()

	const request = "GET /demo HTTP/1.1\r\nHost: gozmq.example\r\n\r\n"
	_, err = raw.Write([]byte(request))
	check(err, "raw TCP write")

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var peerID []byte
	var got bytes.Buffer
	for got.Len() < len(request) {
		msg, err := stream.RecvMsg(callCtx)
		check(err, "stream recv")
		if msg.NumFrames() != 2 {
			log.Fatalf("STREAM recv frames = %d, want identity + body", msg.NumFrames())
		}
		peerID = append(peerID[:0], msg.Frames()[0]...)
		got.Write(msg.Frames()[1])
		msg.Release()
	}
	fmt.Printf("STREAM received raw request:\n%s", got.String())

	response := "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello\n"
	check(stream.SendMsg(callCtx, gozmq.NewMsgFromFrames(peerID, []byte(response))), "stream send")

	buf := make([]byte, len(response))
	_, err = io.ReadFull(raw, buf)
	check(err, "raw TCP read")
	fmt.Printf("raw TCP peer received response:\n%s", string(buf))

	check(stream.SendMsg(callCtx, gozmq.NewMsgFromFrames(peerID)), "stream close peer")
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
