package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
)

// TestConcurrentOperationsNoPanic validates no races/panics under concurrent use.
// Run with -race to detect data races.
func TestConcurrentOperationsNoPanic(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Concurrent Listen on ports 1-5
	for port := uint32(1); port <= 5; port++ {
		wg.Add(1)
		go func(p uint32) {
			defer wg.Done()
			ln, err := s.Listen(ctx, p)
			if err == nil {
				time.Sleep(10 * time.Millisecond)
				ln.Close()
			}
		}(port)
	}

	// Concurrent Open (reverse dial)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := s.Open(ctx, 1)
			if err == nil {
				time.Sleep(5 * time.Millisecond)
				conn.Close()
			}
		}()
	}

	// Feed random frames
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sid := uint32(100 + i)
			tl.push(&rtunpb.Frame{Sid: sid, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
			time.Sleep(2 * time.Millisecond)
			tl.push(&rtunpb.Frame{Sid: sid, Kind: &rtunpb.Frame_Data{Data: &rtunpb.Data{Payload: []byte("x")}}})
		}(i)
	}

	wg.Wait()
	// No panic or deadlock = success
}

func TestLateFrameAfterClose(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx := context.Background()

	ln, _ := s.Listen(ctx, 1)
	defer ln.Close()

	// Establish conn
	tl.push(&rtunpb.Frame{Sid: 99, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: 1}}})
	conn, _ := ln.Accept()
	conn.Close()

	// Give close time to propagate
	time.Sleep(20 * time.Millisecond)

	// Send late DATA; should not panic
	tl.push(&rtunpb.Frame{Sid: 99, Kind: &rtunpb.Frame_Data{Data: &rtunpb.Data{Payload: []byte("late")}}})
	time.Sleep(10 * time.Millisecond)
	// No panic = success
}

func TestContextCanceledOpen(t *testing.T) {
	tl := newTestLink()
	s := NewSession(tl)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.Open(ctx, 1)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}
