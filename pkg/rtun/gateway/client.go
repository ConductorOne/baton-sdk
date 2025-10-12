package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	defaultReadBufferCap = 16
	defaultWriteQueueCap = 16
	maxChunkSize         = 32 * 1024
)

// Dialer opens reverse connections to clients via a gateway server.
type Dialer struct {
	gatewayAddr string
	creds       credentials.TransportCredentials
	// configuration
	readBufferCap int // number of frames to buffer for reads
	writeQueueCap int // number of chunks to buffer for writes
}

// DialerOption configures the Dialer.
type DialerOption func(*Dialer)

// WithReadBufferCapacity sets the number of inbound frames to buffer before backpressure blocks producer.
func WithReadBufferCapacity(capacity int) DialerOption {
	return func(d *Dialer) {
		if capacity > 0 {
			d.readBufferCap = capacity
		}
	}
}

// WithWriteQueueCapacity sets the number of outbound chunks to queue before backpressure blocks writers.
func WithWriteQueueCapacity(capacity int) DialerOption {
	return func(d *Dialer) {
		if capacity > 0 {
			d.writeQueueCap = capacity
		}
	}
}

// NewDialerWithOptions creates a gateway client with extra configuration.
func NewDialer(gatewayAddr string, creds credentials.TransportCredentials, opts ...DialerOption) *Dialer {
	d := &Dialer{
		gatewayAddr:   gatewayAddr,
		creds:         creds,
		readBufferCap: defaultReadBufferCap,
		writeQueueCap: defaultWriteQueueCap,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

// DialContext opens a reverse connection to clientID:port via the gateway.
// Returns ErrNotFound if the gateway doesn't own the client (caller should re-resolve owner).
func (d *Dialer) DialContext(ctx context.Context, clientID string, port uint32) (net.Conn, error) {
	logger := ctxzap.Extract(ctx).With(zap.String("client_id", clientID), zap.Uint32("port", port))

	// Dial gateway
	cc, err := grpc.DialContext(ctx, d.gatewayAddr,
		grpc.WithTransportCredentials(d.creds),
	)
	if err != nil {
		return nil, fmt.Errorf("gateway dial failed: %w", err)
	}

	client := rtunpb.NewReverseDialerClient(cc)
	// Create a cancellable stream context so Close() can interrupt Recv/Send.
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := client.Open(streamCtx)
	if err != nil {
		cancel()
		cc.Close()
		return nil, fmt.Errorf("gateway open stream failed: %w", err)
	}

	// Send OpenRequest with gSID=1 (simple case: one connection per stream)
	gsid := uint32(1)
	if err := stream.Send(&rtunpb.GatewayRequest{Kind: &rtunpb.GatewayRequest_OpenReq{
		OpenReq: &rtunpb.OpenRequest{Gsid: gsid, ClientId: clientID, Port: port},
	}}); err != nil {
		stream.CloseSend()
		cancel()
		cc.Close()
		return nil, fmt.Errorf("gateway send OpenRequest failed: %w", err)
	}

	// Recv OpenResponse
	resp, err := stream.Recv()
	if err != nil {
		stream.CloseSend()
		cancel()
		cc.Close()
		return nil, fmt.Errorf("gateway recv OpenResponse failed: %w", err)
	}

	openResp := resp.GetOpenResp()
	if openResp == nil {
		stream.CloseSend()
		cancel()
		cc.Close()
		return nil, ErrProtocol
	}
	if openResp.GetGsid() != gsid {
		stream.CloseSend()
		cancel()
		cc.Close()
		return nil, fmt.Errorf("gateway returned mismatched gSID: got %d, want %d", openResp.GetGsid(), gsid)
	}

	switch openResp.Result.(type) {
	case *rtunpb.OpenResponse_NotFound:
		stream.CloseSend()
		cancel()
		cc.Close()
		logger.Info("client not found on gateway")
		return nil, ErrNotFound
	case *rtunpb.OpenResponse_Opened:
		logger.Info("gateway connection opened")
		doneCh := make(chan struct{})
		gc := &gatewayConn{
			stream: stream,
			cc:     cc,
			gsid:   gsid,
			cancel: cancel,
			doneCh: doneCh,
		}
		gc.r = newReader(stream, gsid, d.readBufferCap, doneCh)
		gc.w = newWriter(stream, gsid, d.writeQueueCap, doneCh)
		return gc, nil
	default:
		stream.CloseSend()
		cancel()
		cc.Close()
		return nil, ErrProtocol
	}
}

var _ net.Conn = (*gatewayConn)(nil)

// gatewayConn implements net.Conn over a gateway stream.
type gatewayConn struct {
	stream rtunpb.ReverseDialer_OpenClient
	cc     *grpc.ClientConn
	gsid   uint32
	cancel context.CancelFunc

	// reader/writer components
	r *reader
	w *writer

	writeMu     sync.Mutex
	writeClosed bool

	closeOnce sync.Once
	doneCh    chan struct{}

	rdDeadline time.Time
	wrDeadline time.Time
}

type writeMsg struct {
	payload []byte
	fin     bool
}

type reader struct {
	stream rtunpb.ReverseDialer_OpenClient
	gsid   uint32
	bufCap int
	ch     chan []byte
	doneCh <-chan struct{}

	mu   sync.Mutex
	rem  []byte
	err  error
	once sync.Once
}

func newReader(stream rtunpb.ReverseDialer_OpenClient, gsid uint32, bufCap int, doneCh <-chan struct{}) *reader {
	if bufCap <= 0 {
		bufCap = defaultReadBufferCap
	}
	return &reader{
		stream: stream,
		gsid:   gsid,
		bufCap: bufCap,
		doneCh: doneCh,
	}
}

func (r *reader) start() {
	r.once.Do(func() {
		r.ch = make(chan []byte, r.bufCap)
		go r.loop()
	})
}

func (r *reader) loop() {
	defer close(r.ch)
	for {
		resp, err := r.stream.Recv()
		if err != nil {
			r.mu.Lock()
			if r.err == nil {
				r.err = err
			}
			r.mu.Unlock()
			return
		}
		fr := resp.GetFrame()
		if fr == nil || fr.GetSid() != r.gsid {
			continue
		}
		switch k := fr.Kind.(type) {
		case *rtunpb.Frame_Data:
			payload := append([]byte(nil), k.Data.GetPayload()...)
			select {
			case r.ch <- payload:
			case <-r.doneCh:
				return
			case <-r.stream.Context().Done():
				return
			}
		case *rtunpb.Frame_Fin:
			r.mu.Lock()
			r.err = io.EOF
			r.mu.Unlock()
			return
		case *rtunpb.Frame_Rst:
			r.mu.Lock()
			r.err = fmt.Errorf("gateway: connection reset (code %v)", k.Rst.GetCode())
			r.mu.Unlock()
			return
		}
	}
}

func (r *reader) next(ctx context.Context) ([]byte, error) {
	r.start()
	select {
	case buf, ok := <-r.ch:
		if !ok {
			r.mu.Lock()
			err := r.err
			r.mu.Unlock()
			if err == nil {
				return nil, io.EOF
			}
			return nil, err
		}
		return buf, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type writer struct {
	stream   rtunpb.ReverseDialer_OpenClient
	gsid     uint32
	queueCap int
	ch       chan writeMsg
	doneCh   <-chan struct{}

	mu   sync.Mutex
	err  error
	once sync.Once
}

func newWriter(stream rtunpb.ReverseDialer_OpenClient, gsid uint32, queueCap int, doneCh <-chan struct{}) *writer {
	if queueCap <= 0 {
		queueCap = defaultWriteQueueCap
	}
	return &writer{
		stream:   stream,
		gsid:     gsid,
		queueCap: queueCap,
		doneCh:   doneCh,
	}
}

func (w *writer) start() {
	w.once.Do(func() {
		w.ch = make(chan writeMsg, w.queueCap)
		go w.loop()
	})
}

func (w *writer) setErr(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
}

func (w *writer) getErr() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *writer) loop() {
	for {
		select {
		case msg := <-w.ch:
			if msg.fin {
				_ = w.stream.Send(&rtunpb.GatewayRequest{Kind: &rtunpb.GatewayRequest_Frame{
					Frame: &rtunpb.Frame{Sid: w.gsid, Kind: &rtunpb.Frame_Fin{Fin: &rtunpb.Fin{}}},
				}})
				continue
			}
			if err := w.stream.Send(&rtunpb.GatewayRequest{Kind: &rtunpb.GatewayRequest_Frame{
				Frame: &rtunpb.Frame{Sid: w.gsid, Kind: &rtunpb.Frame_Data{Data: &rtunpb.Data{Payload: msg.payload}}},
			}}); err != nil {
				w.setErr(err)
				return
			}
		case <-w.doneCh:
			return
		case <-w.stream.Context().Done():
			return
		}
	}
}

func (w *writer) enqueue(msg writeMsg, deadline time.Time) error {
	w.start()
	// fast-fail if previous send error
	if err := w.getErr(); err != nil {
		return err
	}

	if !deadline.IsZero() {
		until := time.Until(deadline)
		if until <= 0 {
			return context.DeadlineExceeded
		}
		timer := time.NewTimer(until)
		defer timer.Stop()
		select {
		case w.ch <- msg:
			return nil
		case <-timer.C:
			return context.DeadlineExceeded
		case <-w.doneCh:
			return fmt.Errorf("rtun/gateway: write after close: %w", net.ErrClosed)
		case <-w.stream.Context().Done():
			return w.stream.Context().Err()
		}
	}
	select {
	case w.ch <- msg:
		return nil
	case <-w.doneCh:
		return fmt.Errorf("rtun/gateway: write after close: %w", net.ErrClosed)
	case <-w.stream.Context().Done():
		return w.stream.Context().Err()
	}
}

func (g *gatewayConn) Read(p []byte) (int, error) {
	// Consume remainder first
	g.r.mu.Lock()
	if len(g.r.rem) > 0 {
		n := copy(p, g.r.rem)
		g.r.rem = g.r.rem[n:]
		g.r.mu.Unlock()
		return n, nil
	}
	g.r.mu.Unlock()

	// Compute deadline context
	var ctx context.Context
	var cancel context.CancelFunc
	if g.rdDeadline.IsZero() {
		ctx = context.Background()
		cancel = func() {}
	} else {
		until := time.Until(g.rdDeadline)
		if until <= 0 {
			return 0, context.DeadlineExceeded
		}
		ctx, cancel = context.WithTimeout(context.Background(), until)
	}
	defer cancel()

	buf, err := g.r.next(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
			return 0, err
		}
		if g.isClosed() {
			return 0, fmt.Errorf("rtun/gateway: read on closed connection: %w", net.ErrClosed)
		}
		return 0, err
	}
	n := copy(p, buf)
	if n < len(buf) {
		g.r.mu.Lock()
		g.r.rem = buf[n:]
		g.r.mu.Unlock()
	}
	return n, nil
}

func (g *gatewayConn) isClosed() bool {
	select {
	case <-g.doneCh:
		return true
	default:
		return false
	}
}

// recvLoop moved into reader.loop

func (g *gatewayConn) Write(p []byte) (int, error) {
	g.writeMu.Lock()
	if g.writeClosed {
		g.writeMu.Unlock()
		return 0, fmt.Errorf("rtun/gateway: write on closed connection: %w", net.ErrClosed)
	}
	g.writeMu.Unlock()

	if err := g.w.getErr(); err != nil {
		return 0, err
	}

	total := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > maxChunkSize {
			chunk = p[:maxChunkSize]
		}
		cp := append([]byte(nil), chunk...)
		if err := g.w.enqueue(writeMsg{payload: cp}, g.wrDeadline); err != nil {
			if total == 0 {
				return 0, err
			}
			return total, err
		}
		total += len(chunk)
		p = p[len(chunk):]
	}
	return total, nil
}

// writer loop moved into writer.loop

func (g *gatewayConn) Close() error {
	g.closeOnce.Do(func() {
		g.writeMu.Lock()
		if !g.writeClosed {
			g.writeClosed = true
			// best-effort FIN without blocking; if writer not started yet, send directly
			if g.w != nil && g.w.ch != nil {
				select {
				case g.w.ch <- writeMsg{fin: true}:
				default:
				}
			} else {
				_ = g.stream.Send(&rtunpb.GatewayRequest{Kind: &rtunpb.GatewayRequest_Frame{
					Frame: &rtunpb.Frame{Sid: g.gsid, Kind: &rtunpb.Frame_Fin{Fin: &rtunpb.Fin{}}},
				}})
			}
		}
		g.writeMu.Unlock()

		// Cancel stream context to unblock Recv/Send
		if g.cancel != nil {
			g.cancel()
		}
		close(g.doneCh)
		_ = g.stream.CloseSend()
		_ = g.cc.Close()
	})
	return nil
}

func (g *gatewayConn) LocalAddr() net.Addr  { return gatewayAddr{"gateway-local"} }
func (g *gatewayConn) RemoteAddr() net.Addr { return gatewayAddr{"gateway-remote"} }

func (g *gatewayConn) SetDeadline(t time.Time) error {
	g.rdDeadline = t
	g.wrDeadline = t
	return nil
}

func (g *gatewayConn) SetReadDeadline(t time.Time) error {
	g.rdDeadline = t
	return nil
}

func (g *gatewayConn) SetWriteDeadline(t time.Time) error {
	g.wrDeadline = t
	return nil
}

type gatewayAddr struct{ s string }

func (a gatewayAddr) Network() string { return "gateway" }
func (a gatewayAddr) String() string  { return a.s }
