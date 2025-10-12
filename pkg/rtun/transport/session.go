package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	sdkmetrics "github.com/conductorone/baton-sdk/pkg/metrics"
	"go.uber.org/zap"
)

// Option configures a Session.
type Option func(*options)

type options struct {
	logger *zap.Logger
	// allowedPorts is an allowlist of ports that the server is permitted to Open() toward the client.
	// If nil or empty, all ports are allowed.
	allowedPorts map[uint32]bool
	// maxPendingSIDs caps how many distinct SIDs may accumulate DATA-before-SYN buffers.
	maxPendingSIDs int
	// idleTimeout controls per-SID idle expiration; zero means use default (10m). Negative disables.
	idleTimeout time.Duration
	// metrics handler (optional)
	metrics sdkmetrics.Handler
}

// WithLogger sets a structured logger for the session.
func WithLogger(l *zap.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithAllowedPorts sets the explicit list of allowed ports for Session.Open().
func WithAllowedPorts(ports []uint32) Option {
	return func(o *options) {
		if len(ports) == 0 {
			o.allowedPorts = nil
			return
		}
		if o.allowedPorts == nil {
			o.allowedPorts = make(map[uint32]bool, len(ports))
		}
		for _, p := range ports {
			o.allowedPorts[p] = true
		}
	}
}

// WithMaxPendingSIDs sets the maximum number of distinct SIDs allowed to accumulate
// DATA-before-SYN pending buffers. Values <= 0 select the default (64).
func WithMaxPendingSIDs(n int) Option {
	return func(o *options) {
		o.maxPendingSIDs = n
	}
}

// WithIdleTimeout sets the per-SID idle timeout. Zero selects the default (10m). Negative disables.
func WithIdleTimeout(d time.Duration) Option {
	return func(o *options) {
		o.idleTimeout = d
	}
}

// WithMetricsHandler injects a metrics handler for transport-level metrics.
func WithMetricsHandler(h sdkmetrics.Handler) Option {
	return func(o *options) {
		o.metrics = h
	}
}

// Link is the minimal adapter that the generated gRPC stream satisfies.
// It is intentionally small to decouple from gRPC specifics and simplify testing.
type Link interface {
	Send(*rtunpb.Frame) error
	Recv() (*rtunpb.Frame, error)
	Context() context.Context
}

// Session represents a per-link dispatcher with a single Recv loop and listener/conn registry.
type Session struct {
	link   Link
	logger *zap.Logger

	mu        sync.Mutex
	started   bool
	closing   bool
	conns     map[uint32]*virtConn
	listeners map[uint32]*rtunListener
	nextSID   uint32
	pending   map[uint32][][]byte // queued DATA before SYN processed
	closed    closedSet           // closed SIDs for late-frame detection

	// configuration
	allowedPorts   map[uint32]bool
	maxPendingSIDs int
	idleTimeout    time.Duration

	// metrics (optional)
	m *transportMetrics
}

// NewSession constructs a per-link session. The Recv loop starts on first listener registration.
func NewSession(link Link, opts ...Option) *Session {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	logger := o.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	// defaults
	maxPending := o.maxPendingSIDs
	if maxPending <= 0 {
		maxPending = 64
	}
	idle := o.idleTimeout
	if idle == 0 {
		idle = 10 * time.Minute
	}
	s := &Session{
		link:           link,
		conns:          make(map[uint32]*virtConn),
		listeners:      make(map[uint32]*rtunListener),
		nextSID:        1,
		pending:        make(map[uint32][][]byte),
		logger:         logger,
		allowedPorts:   o.allowedPorts,
		maxPendingSIDs: maxPending,
		idleTimeout:    idle,
	}
	if o.metrics != nil {
		s.m = newTransportMetrics(o.metrics)
	}
	return s
}

// Listen exposes a net.Listener for a numeric port on this Session.
func (s *Session) Listen(ctx context.Context, port uint32, opts ...Option) (net.Listener, error) {
	l := &rtunListener{
		port:    port,
		accepts: make(chan net.Conn, 512), // effectively listener backlog
		mux:     s,
	}
	if err := s.addListener(l); err != nil {
		return nil, err
	}
	s.startOnce()
	return l, nil
}

func (s *Session) removeConn(sid uint32) {
	s.mu.Lock()
	delete(s.conns, sid)
	s.mu.Unlock()
}

func (s *Session) addListener(l *rtunListener) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return ErrClosed
	}
	if _, exists := s.listeners[l.port]; exists {
		return errors.New("rtun: listener already exists for port")
	}
	if s.listeners == nil {
		s.listeners = make(map[uint32]*rtunListener)
	}
	s.listeners[l.port] = l
	return nil
}

func (s *Session) removeListener(port uint32) {
	s.mu.Lock()
	delete(s.listeners, port)
	s.mu.Unlock()
}

func (s *Session) startOnce() {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	go s.recvLoop()
}

const maxPendingBufferSize = 64 * 1024

func (s *Session) recvLoop() {
	for {
		fr, err := s.link.Recv()
		if err != nil {
			s.mu.Lock()
			for _, c := range s.conns {
				c.handleRst(err)
			}
			for _, l := range s.listeners {
				l.closeWithErr(err)
			}
			s.closing = true
			s.mu.Unlock()
			return
		}
		if fr == nil {
			continue
		}
		sid := fr.GetSid()
		if s.m != nil {
			s.m.recordFrameRx(s.link.Context(), kindOf(fr))
		}
		// sid==0 is invalid; treat SYN on sid 0 as fatal, drop other frames silently.
		if sid == 0 {
			if _, isSyn := fr.Kind.(*rtunpb.Frame_Syn); isSyn {
				s.logger.Warn("protocol violation: SYN with sid 0; closing link")
				s.failLocked(errors.New("rtun: protocol violation (sid 0)"))
				return
			}
			continue
		}
		switch k := fr.Kind.(type) {
		case *rtunpb.Frame_Syn:
			port := k.Syn.GetPort()
			s.mu.Lock()
			l := s.listeners[port]
			if l == nil {
				s.mu.Unlock()
				// Send RST outside lock
				_ = s.link.Send(&rtunpb.Frame{Sid: sid, Kind: &rtunpb.Frame_Rst{Rst: &rtunpb.Rst{Code: rtunpb.RstCode_RST_CODE_NO_LISTENER}}})
				if s.m != nil {
					s.m.recordRstSent(s.link.Context(), "no_listener")
				}
				continue
			}
			// Duplicate SYN on existing SID is a fatal protocol error for the link.
			if _, exists := s.conns[sid]; exists || s.closed.IsClosed(sid) {
				s.mu.Unlock()
				s.logger.Warn("protocol violation: duplicate SYN for existing or closed SID; closing link", zap.Uint32("sid", sid))
				s.failLocked(errors.New("rtun: duplicate SYN"))
				return
			}
			vc := newVirtConn(s, sid)
			if s.conns == nil {
				s.conns = make(map[uint32]*virtConn)
			}
			s.conns[sid] = vc
			vc.startIdleTimer()
			if s.m != nil {
				s.m.incSidsActive(s.link.Context(), 1)
			}
			// Drain any pending data queued before SYN
			if q := s.pending[sid]; len(q) > 0 {
				for _, p := range q {
					vc.feedData(p)
				}
				delete(s.pending, sid)
			}
			s.mu.Unlock()
			l.enqueue(vc)
		case *rtunpb.Frame_Data:
			s.mu.Lock()
			// Ignore if SID is closed (late frame)
			if s.closed.IsClosed(sid) {
				s.mu.Unlock()
				continue
			}
			c := s.conns[sid]
			payload := append([]byte(nil), k.Data.GetPayload()...)
			if s.m != nil {
				s.m.recordBytesRx(s.link.Context(), int64(len(payload)))
			}
			if c != nil {
				s.mu.Unlock()
				c.feedData(payload)
			} else {
				// defensive programming decision: DATA-before-SYN is a protocol error. RST and do not buffer.
				s.mu.Unlock()
				_ = s.link.Send(&rtunpb.Frame{Sid: sid, Kind: &rtunpb.Frame_Rst{Rst: &rtunpb.Rst{Code: rtunpb.RstCode_RST_CODE_INTERNAL}}})
				if s.m != nil {
					s.m.recordRstSent(s.link.Context(), "protocol_violation")
				}
				continue
			}
		case *rtunpb.Frame_Fin:
			s.mu.Lock()
			c := s.conns[sid]
			s.mu.Unlock()
			if c != nil {
				c.handleFin(k.Fin.GetAck())
				s.removeConn(sid)
				s.mu.Lock()
				s.closed.Close(sid)
				s.mu.Unlock()
				if s.m != nil {
					s.m.incSidsActive(s.link.Context(), -1)
				}
			}
		case *rtunpb.Frame_Rst:
			s.mu.Lock()
			c := s.conns[sid]
			s.mu.Unlock()
			if c != nil {
				if s.m != nil {
					s.m.recordRstRecv(s.link.Context(), k.Rst.GetCode().String())
				}
				c.handleRst(ErrConnReset)
				s.removeConn(sid)
				s.mu.Lock()
				s.closed.Close(sid)
				s.mu.Unlock()
				if s.m != nil {
					s.m.incSidsActive(s.link.Context(), -1)
				}
			}
		}
	}
}

// Start begins the Recv loop if not already started.
func (s *Session) Start() { s.startOnce() }

// Open initiates a reverse connection to the remote client on the given port.
// It allocates a new SID, sends SYN, and returns a net.Conn bound to that SID.
// If ctx is canceled before SYN is sent, returns ctx.Err().
func (s *Session) Open(ctx context.Context, port uint32) (net.Conn, error) {
	// Check context before acquiring lock
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Enforce allowed ports policy if configured
	if s.allowedPorts != nil {
		if !s.allowedPorts[port] {
			return nil, errors.New("rtun: port not allowed by HELLO policy")
		}
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	sid := s.nextSID
	if sid == 0 {
		sid = 1
	}
	s.nextSID = sid + 1
	vc := newVirtConn(s, sid)
	s.conns[sid] = vc
	s.mu.Unlock()

	// Send SYN to remote
	if err := s.link.Send(&rtunpb.Frame{Sid: sid, Kind: &rtunpb.Frame_Syn{Syn: &rtunpb.Syn{Port: port}}}); err != nil {
		// Cleanup on failure
		s.removeConn(sid)
		return nil, err
	}
	vc.startIdleTimer()
	if s.m != nil {
		s.m.incSidsActive(s.link.Context(), 1)
		s.m.recordFrameTx(s.link.Context(), "SYN")
	}
	return vc, nil
}

// markClosed records the SID as closed to ignore late frames.
func (s *Session) markClosed(sid uint32) {
	s.mu.Lock()
	s.closed.Close(sid)
	s.mu.Unlock()
}

// failLocked closes all session resources and marks the session as closing.
// It must be called outside of s.mu (we only use it here where no lock is held),
// but it acquires the lock internally for safety.
func (s *Session) failLocked(err error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	for _, c := range s.conns {
		c.handleRst(err)
	}
	for _, l := range s.listeners {
		l.closeWithErr(err)
	}
	s.closing = true
	s.mu.Unlock()
}

// metrics helpers
type transportMetrics struct {
	framesRx  sdkmetrics.Int64Counter
	framesTx  sdkmetrics.Int64Counter
	bytesRx   sdkmetrics.Int64Counter
	bytesTx   sdkmetrics.Int64Counter
	rstSent   sdkmetrics.Int64Counter
	rstRecv   sdkmetrics.Int64Counter
	sidsGauge sdkmetrics.Int64Gauge

	sids int64
}

func newTransportMetrics(h sdkmetrics.Handler) *transportMetrics {
	m := &transportMetrics{
		framesRx:  h.Int64Counter("rtun.transport.frames_rx_total", "transport frames received", sdkmetrics.Dimensionless),
		framesTx:  h.Int64Counter("rtun.transport.frames_tx_total", "transport frames sent", sdkmetrics.Dimensionless),
		bytesRx:   h.Int64Counter("rtun.transport.data_bytes_rx_total", "transport bytes received", sdkmetrics.Bytes),
		bytesTx:   h.Int64Counter("rtun.transport.data_bytes_tx_total", "transport bytes sent", sdkmetrics.Bytes),
		rstSent:   h.Int64Counter("rtun.transport.rst_sent_total", "RST frames sent by code", sdkmetrics.Dimensionless),
		rstRecv:   h.Int64Counter("rtun.transport.rst_recv_total", "RST frames received by code", sdkmetrics.Dimensionless),
		sidsGauge: h.Int64Gauge("rtun.transport.sids_active", "active SIDs per session", sdkmetrics.Dimensionless),
	}
	// initialize gauge to 0
	m.sidsGauge.Observe(context.Background(), 0, nil)
	return m
}

func (m *transportMetrics) incSidsActive(ctx context.Context, delta int64) {
	m.sids += delta
	m.sidsGauge.Observe(ctx, m.sids, nil)
}

func (m *transportMetrics) recordFrameRx(ctx context.Context, kind string) {
	m.framesRx.Add(ctx, 1, map[string]string{"kind": kind})
}

func (m *transportMetrics) recordFrameTx(ctx context.Context, kind string) {
	m.framesTx.Add(ctx, 1, map[string]string{"kind": kind})
}

func (m *transportMetrics) recordBytesRx(ctx context.Context, n int64) {
	m.bytesRx.Add(ctx, n, nil)
}

func (m *transportMetrics) recordBytesTx(ctx context.Context, n int64) {
	m.bytesTx.Add(ctx, n, nil)
}

func (m *transportMetrics) recordRstSent(ctx context.Context, code string) {
	m.rstSent.Add(ctx, 1, map[string]string{"code": code})
}

func (m *transportMetrics) recordRstRecv(ctx context.Context, code string) {
	m.rstRecv.Add(ctx, 1, map[string]string{"code": code})
}

func kindOf(fr *rtunpb.Frame) string {
	switch fr.Kind.(type) {
	case *rtunpb.Frame_Hello:
		return "HELLO"
	case *rtunpb.Frame_Syn:
		return "SYN"
	case *rtunpb.Frame_Data:
		return "DATA"
	case *rtunpb.Frame_Fin:
		return "FIN"
	case *rtunpb.Frame_Rst:
		return "RST"
	default:
		return "UNKNOWN"
	}
}
