package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	"github.com/stretchr/testify/require"
)

type testLink struct {
	ctx  context.Context
	inCh chan *rtunpb.Frame
	sent []*rtunpb.Frame
}

func newTestLink() *testLink {
	return &testLink{ctx: context.Background(), inCh: make(chan *rtunpb.Frame, 32)}
}

func (t *testLink) Send(f *rtunpb.Frame) error {
	t.sent = append(t.sent, f)
	return nil
}

func (t *testLink) Recv() (*rtunpb.Frame, error) { return <-t.inCh, nil }
func (t *testLink) Context() context.Context     { return t.ctx }

func (t *testLink) push(f *rtunpb.Frame) { t.inCh <- f }

func TestSessionSynAcceptDataFin(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	defer ln.Close()

	// Accept in background
	accCh := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accCh <- c
		}
	}()

	// Send SYN for port 1 on sid 5
	tl.push(&rtunpb.Frame{Sid: 5, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var rwc net.Conn
	select {
	case rwc = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Deliver DATA from link to conn
	tl.push(&rtunpb.Frame{Sid: 5, Kind: &rtunpb.Frame_Data{Data: &rtunpb.Data{Payload: []byte("hi")}}})
	buf := make([]byte, 2)
	n, err := rwc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("hi"), buf)

	// Write from conn to link; expect a DATA frame sent
	_, err = rwc.Write([]byte("ok"))
	require.NoError(t, err)
	require.NotEmpty(t, tl.sent)
	last := tl.sent[len(tl.sent)-1]
	require.Equal(t, uint32(5), last.GetSid())
	require.IsType(t, &rtunpb.Frame_Data{}, last.Kind)
	require.Equal(t, []byte("ok"), last.GetData().GetPayload())

	// Send FIN from link; expect EOF on next read
	tl.push(&rtunpb.Frame{Sid: 5, Kind: &rtunpb.Frame_Fin{Fin: &rtunpb.Fin{Ack: false}}})
	_, err = rwc.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)

	// Close should send FIN
	_ = rwc.Close()
	require.NotEmpty(t, tl.sent)
	foundFin := false
	for _, f := range tl.sent {
		if f.GetSid() == 5 {
			if _, ok := f.Kind.(*rtunpb.Frame_Fin); ok {
				foundFin = true
				break
			}
		}
	}
	require.True(t, foundFin)
}

func TestSessionSynNoListenerRst(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Start session loop without listener by poking it: creating a throwaway listener on port 1 and closing is too involved,
	// so instead we trigger start by creating and closing a short-lived listener which ensures the goroutine is running.
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	ln.Close()

	// Send SYN for a port without a listener
	tl.push(&rtunpb.Frame{Sid: 9, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 2}}})
	// Allow loop to process
	time.Sleep(10 * time.Millisecond)
	require.NotEmpty(t, tl.sent)
	last := tl.sent[len(tl.sent)-1]
	require.Equal(t, uint32(9), last.GetSid())
	require.IsType(t, &rtunpb.Frame_Rst{}, last.Kind)
	require.Equal(t, rtunpb.RstCode_RST_CODE_NO_LISTENER, last.GetRst().GetCode())
}

func TestVirtConnReadDeadline(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	defer ln.Close()

	// Accept in background
	accCh := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accCh <- c
	}()

	// Establish
	tl.push(&rtunpb.Frame{Sid: 7, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var rwc net.Conn
	select {
	case rwc = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Set a short read deadline and expect timeout
	_ = rwc.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = rwc.Read(buf)
	require.ErrorIs(t, err, ErrTimeout)
}

func TestVirtConnHalfCloseWriteAfterRemoteFin(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	defer ln.Close()

	accCh := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accCh <- c
	}()
	tl.push(&rtunpb.Frame{Sid: 11, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var rwc net.Conn
	select {
	case rwc = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}
	// Remote sends FIN; we should still be able to Write()
	tl.push(&rtunpb.Frame{Sid: 11, Kind: &rtunpb.Frame_Fin{Fin: &rtunpb.Fin{Ack: false}}})
	_, err = rwc.Write([]byte("after"))
	require.NoError(t, err)
}

func TestWriteFragmentationIntoMultipleDataFrames(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	defer ln.Close()

	accCh := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); accCh <- c }()
	tl.push(&rtunpb.Frame{Sid: 21, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var rwc net.Conn
	select {
	case rwc = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Prepare a payload larger than maxWriteChunk (32KiB) to force fragmentation.
	big := make([]byte, 32*1024+4096)
	for i := range big {
		big[i] = byte(i)
	}
	prev := len(tl.sent)
	n, err := rwc.Write(big)
	require.NoError(t, err)
	require.Equal(t, len(big), n)

	// Collect new DATA frames for this SID and reassemble.
	var collected []byte
	for _, f := range tl.sent[prev:] {
		if f.GetSid() == 21 {
			if d := f.GetData(); d != nil {
				collected = append(collected, d.GetPayload()...)
			}
		}
	}
	require.Equal(t, big, collected)
}

func TestReadRemainderAcrossReads(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	defer ln.Close()

	accCh := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); accCh <- c }()
	tl.push(&rtunpb.Frame{Sid: 22, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var rwc net.Conn
	select {
	case rwc = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Deliver 3 bytes, then read 2, then read 1
	tl.push(&rtunpb.Frame{Sid: 22, Kind: &rtunpb.Frame_Data{Data: &rtunpb.Data{Payload: []byte("xyz")}}})
	buf := make([]byte, 2)
	n, err := rwc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("xy"), buf)
	buf2 := make([]byte, 2)
	n, err = rwc.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte("z\x00"), buf2) // last byte remains zero
}

func TestBackpressureInboundOverflow(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	defer ln.Close()

	accCh := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); accCh <- c }()
	tl.push(&rtunpb.Frame{Sid: 23, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var rwc net.Conn
	select {
	case rwc = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Push more frames than the read buffer can hold (capacity 256) without reading.
	for i := 0; i < 300; i++ {
		tl.push(&rtunpb.Frame{Sid: 23, Kind: &rtunpb.Frame_Data{Data: &rtunpb.Data{Payload: []byte{byte(i)}}}})
	}
	// Give the loop a moment to overflow and close.
	time.Sleep(20 * time.Millisecond)
	buf := make([]byte, 1)
	_, err = rwc.Read(buf)
	require.Error(t, err)
	require.NotEqual(t, io.EOF, err)
}

func TestWriteDeadlineTimeout(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	ln, err := s.Listen(ctx, 1)
	require.NoError(t, err)
	defer ln.Close()

	accCh := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); accCh <- c }()
	tl.push(&rtunpb.Frame{Sid: 24, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	var rwc net.Conn
	select {
	case rwc = <-accCh:
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}
	// Set deadline in the past and attempt write.
	_ = rwc.SetWriteDeadline(time.Now().Add(-1 * time.Millisecond))
	_, err = rwc.Write([]byte("x"))
	require.ErrorIs(t, err, ErrTimeout)
}
