package gateway

import (
	"context"
	"io"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	"github.com/conductorone/baton-sdk/pkg/rtun/server"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// Server implements the ReverseDialer gateway service.
// It bridges caller streams to rtun sessions on the owner server process.
type Server struct {
	rtunpb.UnimplementedReverseDialerServer

	reg      *server.Registry
	serverID string
	m        *gwMetrics
}

// NewServer creates a gateway server that bridges callers to local rtun sessions.
func NewServer(reg *server.Registry, serverID string, opts ...Option) *Server {
	var o options
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&o)
	}
	s := &Server{
		reg:      reg,
		serverID: serverID,
	}
	if o.metrics != nil {
		s.m = newGwMetrics(o.metrics)
	}
	return s
}

// Open handles a gateway stream: caller sends OpenRequest(s) and Frames; gateway bridges to rtun.
const (
	writerQueueCap = 256
	writeDeadline  = 30 * time.Second
)

type entry struct {
	conn      net.Conn
	writeCh   chan []byte
	done      chan struct{}
	closeOnce sync.Once
	m         *gwMetrics
	ctx       context.Context
}

func newEntry(conn net.Conn, m *gwMetrics, ctx context.Context) *entry {
	e := &entry{
		conn:    conn,
		writeCh: make(chan []byte, writerQueueCap),
		done:    make(chan struct{}),
		m:       m,
		ctx:     ctx,
	}
	go e.writerLoop()
	return e
}

func (e *entry) writerLoop() {
	for {
		select {
		case <-e.done:
			return
		case buf, ok := <-e.writeCh:
			if !ok {
				return
			}
			_ = e.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if _, err := e.conn.Write(buf); err != nil {
				if e.m != nil {
					e.m.addWriteErr(e.ctx)
				}
				e.close()
				return
			}
		}
	}
}

func (e *entry) send(b []byte) bool {
	cp := append([]byte(nil), b...)
	select {
	case <-e.done:
		return false
	case e.writeCh <- cp:
		return true
	default:
		e.close()
		return false
	}
}

func (e *entry) close() {
	e.closeOnce.Do(func() {
		_ = e.conn.Close()
		close(e.done)
		close(e.writeCh)
	})
}

func (s *Server) Open(stream rtunpb.ReverseDialer_OpenServer) error {
	ctx := stream.Context()
	logger := ctxzap.Extract(ctx).With(zap.String("server_id", s.serverID))

	var mu sync.Mutex
	entries := make(map[uint32]*entry)
	var wg sync.WaitGroup

	defer func() {
		// Cleanup: close all connections and wait for readers
		mu.Lock()
		for gsid, ent := range entries {
			ent.close()
			delete(entries, gsid)
		}
		mu.Unlock()
		wg.Wait()
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		switch k := req.Kind.(type) {
		case *rtunpb.GatewayRequest_OpenReq:
			openReq := k.OpenReq
			gsid := openReq.GetGsid()
			clientID := openReq.GetClientId()
			port := openReq.GetPort()

			logger := logger.With(zap.Uint32("gsid", gsid), zap.String("client_id", clientID), zap.Uint32("port", port))
			if s.m != nil {
				s.m.addOpenReq(ctx)
				s.m.addFrameRx(ctx, "OPEN_REQ")
			}

			// Check for duplicate gSID
			mu.Lock()
			if _, exists := entries[gsid]; exists {
				mu.Unlock()
				logger.Warn("duplicate gSID in OpenRequest")
				_ = stream.Send(&rtunpb.GatewayResponse{Kind: &rtunpb.GatewayResponse_Frame{
					Frame: &rtunpb.Frame{Sid: gsid, Kind: &rtunpb.Frame_Rst{Rst: &rtunpb.Rst{Code: rtunpb.RstCode_RST_CODE_INTERNAL}}},
				}})
				continue
			}
			mu.Unlock()

			// Open reverse connection via local registry
			addr := formatRtunAddr(clientID, port)
			conn, err := s.reg.DialContext(ctx, addr)
			if err != nil {
				logger.Info("client not found or dial failed", zap.Error(err))
				_ = stream.Send(&rtunpb.GatewayResponse{Kind: &rtunpb.GatewayResponse_OpenResp{
					OpenResp: &rtunpb.OpenResponse{Gsid: gsid, Result: &rtunpb.OpenResponse_NotFound{NotFound: &rtunpb.NotFound{}}},
				}})
				if s.m != nil {
					s.m.addOpenMiss(ctx)
				}
				continue
			}

			// Store conn and reply success
			mu.Lock()
			ent := newEntry(conn, s.m, ctx)
			entries[gsid] = ent
			mu.Unlock()

			logger.Info("opened reverse connection")
			if err := stream.Send(&rtunpb.GatewayResponse{Kind: &rtunpb.GatewayResponse_OpenResp{
				OpenResp: &rtunpb.OpenResponse{Gsid: gsid, Result: &rtunpb.OpenResponse_Opened{Opened: &rtunpb.Opened{}}},
			}}); err != nil {
				conn.Close()
				return err
			}
			if s.m != nil {
				s.m.addOpenOK(ctx)
			}

			// Spawn reader: conn → caller
			wg.Add(1)
			go s.bridgeRead(ctx, conn, gsid, stream, &wg, logger)

		case *rtunpb.GatewayRequest_Frame:
			fr := k.Frame
			gsid := fr.GetSid()

			mu.Lock()
			ent := entries[gsid]
			mu.Unlock()

			if ent == nil {
				// Unknown gSID; send RST
				_ = stream.Send(&rtunpb.GatewayResponse{Kind: &rtunpb.GatewayResponse_Frame{
					Frame: &rtunpb.Frame{Sid: gsid, Kind: &rtunpb.Frame_Rst{Rst: &rtunpb.Rst{Code: rtunpb.RstCode_RST_CODE_INTERNAL}}},
				}})
				continue
			}

			// Handle frame
			switch fk := fr.Kind.(type) {
			case *rtunpb.Frame_Data:
				if s.m != nil {
					s.m.addFrameRx(ctx, "DATA")
				}
				if ok := ent.send(fk.Data.GetPayload()); !ok {
					if s.m != nil {
						s.m.addWriterDrop(ctx)
					}
					mu.Lock()
					delete(entries, gsid)
					mu.Unlock()
				}
			case *rtunpb.Frame_Fin:
				if s.m != nil {
					s.m.addFrameRx(ctx, "FIN")
				}
				mu.Lock()
				ent.close()
				delete(entries, gsid)
				mu.Unlock()
			case *rtunpb.Frame_Rst:
				if s.m != nil {
					s.m.addFrameRx(ctx, "RST")
				}
				mu.Lock()
				ent.close()
				delete(entries, gsid)
				mu.Unlock()
			}
		}
	}
}

// bridgeRead reads from rtun conn and sends frames to the caller stream.
func (s *Server) bridgeRead(ctx context.Context, conn net.Conn, gsid uint32, stream rtunpb.ReverseDialer_OpenServer, wg *sync.WaitGroup, logger *zap.Logger) {
	defer wg.Done()
	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			if err := stream.Send(&rtunpb.GatewayResponse{Kind: &rtunpb.GatewayResponse_Frame{
				Frame: &rtunpb.Frame{Sid: gsid, Kind: &rtunpb.Frame_Data{Data: &rtunpb.Data{Payload: append([]byte(nil), buf[:n]...)}}},
			}}); err != nil {
				logger.Warn("failed to send data to caller", zap.Error(err))
				return
			}
			if s.m != nil {
				s.m.addFrameTx(ctx, "DATA")
			}
		}
		if err != nil {
			if err == io.EOF {
				_ = stream.Send(&rtunpb.GatewayResponse{Kind: &rtunpb.GatewayResponse_Frame{
					Frame: &rtunpb.Frame{Sid: gsid, Kind: &rtunpb.Frame_Fin{Fin: &rtunpb.Fin{}}},
				}})
				if s.m != nil {
					s.m.addFrameTx(ctx, "FIN")
				}
			} else {
				_ = stream.Send(&rtunpb.GatewayResponse{Kind: &rtunpb.GatewayResponse_Frame{
					Frame: &rtunpb.Frame{Sid: gsid, Kind: &rtunpb.Frame_Rst{Rst: &rtunpb.Rst{Code: rtunpb.RstCode_RST_CODE_INTERNAL}}},
				}})
				if s.m != nil {
					s.m.addFrameTx(ctx, "RST")
				}
			}
			return
		}
	}
}

func formatRtunAddr(clientID string, port uint32) string {
	u := url.URL{
		Scheme: "rtun",
		Host: net.JoinHostPort(
			clientID,
			strconv.Itoa(int(port)),
		),
	}
	return u.String()
}
