package gozmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

func TestHWMZeroDirectInprocDoesNotBlockWithoutReceiver(t *testing.T) {
	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://hwm-zero-direct",
		gozmq.WithSendHWM(0), gozmq.WithRecvHWM(0))

	sendManyWithoutReceiver(t, client, 1000)
	recvMany(t, server, 1000)
}

func TestHWMZeroSessionPathDoesNotBlockWithoutReceiver(t *testing.T) {
	_, server, client := newSocketPair(t, gozmq.Pair, "tcp://127.0.0.1:0",
		gozmq.WithSendHWM(0), gozmq.WithRecvHWM(0))

	sendManyWithoutReceiver(t, client, 200)
	recvMany(t, server, 200)
}

func sendManyWithoutReceiver(t *testing.T, sock *gozmq.Socket, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := sock.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte(fmt.Sprintf("%04d", i))))
		cancel()
		if err != nil {
			t.Fatalf("send[%d]: %v", i, err)
		}
	}
}

func recvMany(t *testing.T, sock *gozmq.Socket, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m, err := sock.RecvMsg(ctx)
		cancel()
		if err != nil {
			t.Fatalf("recv[%d]: %v", i, err)
		}
		m.Release()
	}
}
