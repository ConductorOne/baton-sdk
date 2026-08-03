package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	"github.com/stretchr/testify/require"
)

func TestVirtConnCloseIdempotentAndWriteAfterClose(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	accCh := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); accCh <- c }()
	tl.push(&rtunpb.Frame{Sid: 31, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})

	var c net.Conn
	select {
	case c = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Close twice is ok
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())

	// Write after close should fail with ErrClosed
	_, err = c.Write([]byte("x"))
	require.ErrorIs(t, err, net.ErrClosed)

	// Read after close yields EOF or ErrClosed
	_, err = c.Read(make([]byte, 1))
	require.True(t, errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed))
}

func TestVirtConnRemoteRstPropagatesToRead(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	accCh := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); accCh <- c }()
	tl.push(&rtunpb.Frame{Sid: 32, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var c net.Conn
	select {
	case c = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Remote sends RST
	tl.push(&rtunpb.Frame{Sid: 32, Kind: &rtunpb.Frame_Rst{Rst: &rtunpb.Rst{Code: rtunpb.RstCode_RST_CODE_INTERNAL}}})
	_, err = c.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrConnReset)
}

func TestVirtConnLocalRemoteAddr(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	accCh := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); accCh <- c }()
	tl.push(&rtunpb.Frame{Sid: 33, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var c net.Conn
	select {
	case c = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	require.NotNil(t, c.LocalAddr())
	require.NotNil(t, c.RemoteAddr())
}
