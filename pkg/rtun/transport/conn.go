package transport

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
)

const maxWriteChunk = 32 * 1024

// virtConn implements net.Conn over a multiplexed SID.
type virtConn struct {
	mux *Session
	sid uint32

	readCh  chan []byte
	readErr error
	readMu  sync.Mutex
	readRem []byte // remainder from partial reads

	writeMu     sync.Mutex
	writeClosed bool

	rdDeadline time.Time
	wrDeadline time.Time

	// closeReadOnce ensures the read side is closed exactly once, regardless of
	// whether closure originates locally (Close), remotely (FIN), or via RST.
	closeReadOnce sync.Once

	// idle timer management
	idleMu    sync.Mutex
	idleTimer *time.Timer
}

var _ net.Conn = (*virtConn)(nil)

func newVirtConn(m *Session, sid uint32) *virtConn {
	return &virtConn{
		mux:    m,
		sid:    sid,
		readCh: make(chan []byte, 256),
	}
}

// Read implements net.Conn for virtConn by returning data delivered for this SID, honoring read deadlines.
func (c *virtConn) Read(p []byte) (int, error) {
	// Check terminal error or remainder under lock
	c.readMu.Lock()
	if c.readErr != nil {
		err := c.readErr
		c.readMu.Unlock()
		return 0, err
	}
	if len(c.readRem) > 0 {
		n := copy(p, c.readRem)
		c.readRem = c.readRem[n:]
		c.readMu.Unlock()
		return n, nil
	}
	// Snapshot deadline then release lock before blocking on channel
	deadline := c.rdDeadline
	c.readMu.Unlock()

	if deadline.IsZero() {
		buf, ok := <-c.readCh
		if !ok {
			c.readMu.Lock()
			err := c.readErr
			c.readMu.Unlock()
			if err == nil {
				return 0, io.EOF
			}
			return 0, err
		}
		n := copy(p, buf)
		c.onActivity()
		if n < len(buf) {
			c.readMu.Lock()
			c.readRem = buf[n:]
			c.readMu.Unlock()
		}
		return n, nil
	}

	// Deadline set
	until := time.Until(deadline)
	if until <= 0 {
		return 0, ErrTimeout
	}
	timer := time.NewTimer(until)
	defer timer.Stop()
	select {
	case buf, ok := <-c.readCh:
		if !ok {
			c.readMu.Lock()
			err := c.readErr
			c.readMu.Unlock()
			if err == nil {
				return 0, io.EOF
			}
			return 0, err
		}
		n := copy(p, buf)
		c.onActivity()
		if n < len(buf) {
			c.readMu.Lock()
			c.readRem = buf[n:]
			c.readMu.Unlock()
		}
		return n, nil
	case <-timer.C:
		return 0, ErrTimeout
	}
}

// Write implements net.Conn for virtConn by sending DATA frames for this SID, honoring write deadlines.
func (c *virtConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeClosed {
		return 0, net.ErrClosed
	}
	// Writes are allowed even after remote FIN (half-close), so we do not block based on remote state.
	total := 0
	for len(p) > 0 {
		if !c.wrDeadline.IsZero() && time.Until(c.wrDeadline) <= 0 {
			if total == 0 {
				return 0, ErrTimeout
			}
			return total, ErrTimeout
		}
		chunk := p
		if len(chunk) > maxWriteChunk {
			chunk = p[:maxWriteChunk]
		}
		frame := &rtunpb.Frame{Sid: c.sid, Kind: &rtunpb.Frame_Data{Data: &rtunpb.Data{Payload: append([]byte(nil), chunk...)}}}
		if err := c.mux.link.Send(frame); err != nil {
			return total, err
		}
		c.onActivity()
		if c.mux.m != nil {
			c.mux.m.recordFrameTx(c.mux.link.Context(), "DATA")
			c.mux.m.recordBytesTx(c.mux.link.Context(), int64(len(chunk)))
		}
		total += len(chunk)
		p = p[len(chunk):]
	}
	return total, nil
}

// Close implements net.Conn for virtConn by half-closing writes (sending FIN) and releasing resources.
func (c *virtConn) Close() error {
	c.writeMu.Lock()
	if c.writeClosed {
		c.writeMu.Unlock()
		return nil
	}
	c.writeClosed = true
	c.writeMu.Unlock()
	// send FIN (ack=false)
	_ = c.mux.link.Send(&rtunpb.Frame{Sid: c.sid, Kind: &rtunpb.Frame_Fin{Fin: &rtunpb.Fin{Ack: false}}})
	// Unblock any pending Read by closing the read side
	c.closeReadOnce.Do(func() {
		c.readMu.Lock()
		if c.readErr == nil {
			c.readErr = io.EOF
		}
		c.readMu.Unlock()
		close(c.readCh)
	})
	c.stopIdleTimer()
	c.mux.removeConn(c.sid)
	return nil
}

// LocalAddr implements net.Conn.
func (c *virtConn) LocalAddr() net.Addr { return rtunAddr{"rtun-local"} }

// RemoteAddr implements net.Conn.
func (c *virtConn) RemoteAddr() net.Addr { return rtunAddr{"rtun-remote"} }

// SetDeadline implements net.Conn.
func (c *virtConn) SetDeadline(t time.Time) error {
	c.readMu.Lock()
	c.rdDeadline = t
	c.readMu.Unlock()
	c.writeMu.Lock()
	c.wrDeadline = t
	c.writeMu.Unlock()
	return nil
}

// SetReadDeadline implements net.Conn.
func (c *virtConn) SetReadDeadline(t time.Time) error {
	c.readMu.Lock()
	c.rdDeadline = t
	c.readMu.Unlock()
	return nil
}

// SetWriteDeadline implements net.Conn.
func (c *virtConn) SetWriteDeadline(t time.Time) error {
	c.writeMu.Lock()
	c.wrDeadline = t
	c.writeMu.Unlock()
	return nil
}

// feedData is called by the mux to deliver inbound bytes.
func (c *virtConn) feedData(b []byte) {
	// If we've already observed a terminal read error (EOF, overflow, RST), drop incoming data.
	c.readMu.Lock()
	alreadyErr := c.readErr
	c.readMu.Unlock()
	if alreadyErr != nil {
		return
	}
	select {
	case c.readCh <- b:
		// delivered
		c.onActivity()
	default:
		// backpressure: send RST, perform full RST handling (including write-side close), and detach from session
		err := errors.New("rtun: inbound buffer overflow")
		_ = c.mux.link.Send(&rtunpb.Frame{Sid: c.sid, Kind: &rtunpb.Frame_Rst{Rst: &rtunpb.Rst{Code: rtunpb.RstCode_RST_CODE_INTERNAL}}})
		c.handleRst(err)
		c.mux.removeConn(c.sid)
	}
}

func (c *virtConn) handleFin(ack bool) {
	// mark remote closed; signal EOF
	c.readMu.Lock()
	if c.readErr == nil {
		c.readErr = io.EOF
	}
	c.readMu.Unlock()
	c.closeReadOnce.Do(func() { close(c.readCh) })
	c.stopIdleTimer()
}

func (c *virtConn) handleRst(err error) {
	c.readMu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	c.readMu.Unlock()
	c.closeReadOnce.Do(func() { close(c.readCh) })
	c.writeMu.Lock()
	c.writeClosed = true
	c.writeMu.Unlock()
	c.stopIdleTimer()
}

type rtunAddr struct{ s string }

func (a rtunAddr) Network() string { return "rtun" }
func (a rtunAddr) String() string  { return a.s }

// startIdleTimer starts or resets the per-connection idle timer according to the Session's configuration.
func (c *virtConn) startIdleTimer() {
	timeout := c.mux.idleTimeout
	if timeout < 0 {
		return
	}
	c.idleMu.Lock()
	if c.idleTimer == nil {
		c.idleTimer = time.AfterFunc(timeout, func() {
			c.handleIdleTimeout()
		})
	} else {
		c.idleTimer.Reset(timeout)
	}
	c.idleMu.Unlock()
}

// onActivity resets the idle timer if enabled to reflect recent I/O activity.
func (c *virtConn) onActivity() {
	timeout := c.mux.idleTimeout
	if timeout < 0 {
		return
	}
	c.idleMu.Lock()
	if c.idleTimer != nil {
		_ = c.idleTimer.Reset(timeout)
	}
	c.idleMu.Unlock()
}

func (c *virtConn) stopIdleTimer() {
	c.idleMu.Lock()
	if c.idleTimer != nil {
		c.idleTimer.Stop()
		c.idleTimer = nil
	}
	c.idleMu.Unlock()
}

func (c *virtConn) handleIdleTimeout() {
	// Timer fired: send RST and tear down connection.
	_ = c.mux.link.Send(&rtunpb.Frame{Sid: c.sid, Kind: &rtunpb.Frame_Rst{Rst: &rtunpb.Rst{Code: rtunpb.RstCode_RST_CODE_INTERNAL}}})
	c.handleRst(ErrTimeout)
	c.mux.removeConn(c.sid)
	c.mux.markClosed(c.sid)
}
