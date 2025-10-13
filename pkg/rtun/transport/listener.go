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
	l.mu.Lock()
	lerr := l.err
	l.mu.Unlock()
	if lerr != nil {
		return nil, lerr
	}
	c, ok := <-l.accepts
	if !ok {
		l.mu.Lock()
		lerr := l.err
		l.mu.Unlock()
		if lerr != nil {
			return nil, lerr
		}
		return nil, net.ErrClosed
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
		c.handleRst(net.ErrClosed)
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
