package transport

import (
	"net"
	"sync"
)

type rtunListener struct {
	port    uint32
	accepts chan net.Conn
	mux     *Session
	mu      sync.Mutex
	closed  bool
	err     error
}

func (l *rtunListener) Accept() (net.Conn, error) {
	if l.err != nil {
		return nil, l.err
	}
	c, ok := <-l.accepts
	if !ok {
		if l.err != nil {
			return nil, l.err
		}
		return nil, ErrClosed
	}
	return c, nil
}

func (l *rtunListener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()
	l.mux.removeListener(l.port)
	close(l.accepts)
	return nil
}

func (l *rtunListener) Addr() net.Addr { return rtunAddr{"rtun-listener"} }

func (l *rtunListener) enqueue(c *virtConn) {
	select {
	case l.accepts <- c:
	default:
		// listener full, drop
		c.handleRst(ErrClosed)
	}
}

func (l *rtunListener) closeWithErr(err error) {
	l.mu.Lock()
	l.err = err
	if !l.closed {
		l.closed = true
		close(l.accepts)
	}
	l.mu.Unlock()
}
