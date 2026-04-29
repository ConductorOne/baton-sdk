package c1api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

const (
	fileChunkSize                            = 1 * 1024 * 512 // 512KB
	defaultC1ServiceClientIdleCloseThreshold = time.Minute
)

type BatonServiceClient interface {
	batonHelloClient

	GetTask(ctx context.Context, req *v1.BatonServiceGetTaskRequest) (*v1.BatonServiceGetTaskResponse, error)
	Heartbeat(ctx context.Context, req *v1.BatonServiceHeartbeatRequest) (*v1.BatonServiceHeartbeatResponse, error)
	FinishTask(ctx context.Context, req *v1.BatonServiceFinishTaskRequest) (*v1.BatonServiceFinishTaskResponse, error)
	Upload(ctx context.Context, task *v1.Task, r io.ReadSeeker) error
}

type batonHelloClient interface {
	Hello(ctx context.Context, req *v1.BatonServiceHelloRequest) (*v1.BatonServiceHelloResponse, error)
}

type c1ServiceConn struct {
	cc       *grpc.ClientConn
	client   v1.BatonServiceClient
	inFlight int
	retired  bool
}

type c1ServiceClient struct {
	addr     string
	dialOpts []grpc.DialOption
	hostID   string

	mtx                  sync.Mutex
	hostIDOnce           sync.Once
	active               *c1ServiceConn
	draining             []*c1ServiceConn
	dialDone             chan struct{}
	dialCancel           context.CancelFunc
	closeWhenIdle        bool
	closed               bool
	idleCloseThreshold   time.Duration
	suppressCloseLogging bool
}

func (c *c1ServiceClient) getHostID() string {
	c.hostIDOnce.Do(func() {
		if c.hostID != "" {
			return
		}

		hostID, _ := os.Hostname()
		if envHost, ok := os.LookupEnv("BATON_HOST_ID"); ok {
			hostID = envHost
		}

		if hostID == "" {
			hostID = "baton-sdk"
		}

		c.hostID = hostID
	})
	return c.hostID
}

func (c *c1ServiceClient) closeIdleThreshold() time.Duration {
	if c.idleCloseThreshold > 0 {
		return c.idleCloseThreshold
	}
	return defaultC1ServiceClientIdleCloseThreshold
}

func (c *c1ServiceClient) dial(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	return c.dialWithContext(dialCtx)
}

func (c *c1ServiceClient) dialWithContext(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext( //nolint:staticcheck // grpc.DialContext is deprecated but we are using it still.
		ctx,
		c.addr,
		c.dialOpts...,
	)
}

func (c *c1ServiceClient) beginRPC(ctx context.Context) (v1.BatonServiceClient, *grpc.ClientConn, error) {
	for {
		c.mtx.Lock()
		if c.closed {
			c.mtx.Unlock()
			return nil, nil, errors.New("c1 service client is closed")
		}
		c.closeWhenIdle = false

		if c.active != nil {
			c.active.inFlight++
			client := c.active.client
			conn := c.active.cc
			c.mtx.Unlock()
			return client, conn, nil
		}

		if c.dialDone != nil {
			dialDone := c.dialDone
			c.mtx.Unlock()

			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-dialDone:
				continue
			}
		}

		dialDone := make(chan struct{})
		dialCtx, dialCancel := context.WithTimeout(ctx, time.Second*30)
		c.dialDone = dialDone
		c.dialCancel = dialCancel
		c.mtx.Unlock()

		cc, err := c.dialWithContext(dialCtx)
		dialCancel()

		c.mtx.Lock()
		if c.dialDone == dialDone {
			c.dialDone = nil
			c.dialCancel = nil
			close(dialDone)
		}
		if err != nil {
			c.mtx.Unlock()
			return nil, nil, err
		}
		if c.closed {
			c.mtx.Unlock()
			_ = cc.Close()
			return nil, nil, errors.New("c1 service client is closed")
		}

		c.active = &c1ServiceConn{
			cc:       cc,
			client:   v1.NewBatonServiceClient(cc),
			inFlight: 1,
		}
		client := c.active.client
		conn := c.active.cc
		c.mtx.Unlock()
		return client, conn, nil
	}
}

func (c *c1ServiceClient) endRPC(conn *grpc.ClientConn, rpcErr error) {
	var closeConns []*c1ServiceConn

	c.mtx.Lock()
	handle := c.findConnLocked(conn)
	if handle != nil && handle.inFlight > 0 {
		handle.inFlight--
	}

	if shouldResetConnection(rpcErr) && handle != nil && !handle.retired {
		if c.active == handle {
			c.active = nil
		}
		handle.retired = true
		if handle.inFlight > 0 {
			c.draining = append(c.draining, handle)
		} else {
			closeConns = append(closeConns, handle)
		}
	}

	if c.active != nil && c.active.inFlight == 0 {
		if c.closeWhenIdle {
			c.active.retired = true
			closeConns = append(closeConns, c.active)
			c.active = nil
		}
	}
	closeConns = append(closeConns, c.collectDrainedLocked()...)
	c.mtx.Unlock()

	c.closeHandles(closeConns)
}

func (c *c1ServiceClient) doRPC(ctx context.Context, retryOnReset bool, call func(v1.BatonServiceClient) error) error {
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		var client v1.BatonServiceClient
		var conn *grpc.ClientConn
		client, conn, err = c.beginRPC(ctx)
		if err != nil {
			return err
		}

		err = call(client)
		shouldRetry := retryOnReset && shouldResetConnection(err) && attempt == 0
		c.endRPC(conn, err)
		if shouldRetry {
			continue
		}
		return err
	}
	return err
}

func shouldResetConnection(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "client connection is closing") ||
		strings.Contains(msg, "connection error") ||
		strings.Contains(msg, "error reading from server") ||
		strings.Contains(msg, "transport is closing") ||
		strings.Contains(msg, "connection reset by peer")
}

func (c *c1ServiceClient) findConnLocked(conn *grpc.ClientConn) *c1ServiceConn {
	if conn == nil {
		return nil
	}
	if c.active != nil && c.active.cc == conn {
		return c.active
	}
	for _, handle := range c.draining {
		if handle.cc == conn {
			return handle
		}
	}
	return nil
}

func (c *c1ServiceClient) collectDrainedLocked() []*c1ServiceConn {
	var closeConns []*c1ServiceConn
	remaining := c.draining[:0]
	for _, handle := range c.draining {
		if handle.inFlight == 0 {
			closeConns = append(closeConns, handle)
			continue
		}
		remaining = append(remaining, handle)
	}
	c.draining = remaining
	return closeConns
}

func (c *c1ServiceClient) closeHandles(handles []*c1ServiceConn) {
	for _, handle := range handles {
		if handle == nil || handle.cc == nil {
			continue
		}
		if err := handle.cc.Close(); err != nil && !c.suppressCloseLogging {
			zap.L().Error("failed to close client connection", zap.Error(err))
		}
	}
}

// CloseIfIdleFor closes an idle connection only when the caller knows the next
// expected C1 API use is far enough away. It is not a background idle timer.
func (c *c1ServiceClient) CloseIfIdleFor(nextUse time.Duration) error {
	if nextUse <= c.closeIdleThreshold() {
		return nil
	}

	var closeConns []*c1ServiceConn
	c.mtx.Lock()
	if c.closed {
		c.mtx.Unlock()
		return nil
	}
	if c.active != nil {
		if c.active.inFlight == 0 {
			closeConns = append(closeConns, c.active)
			c.active = nil
		} else {
			c.closeWhenIdle = true
		}
	}
	closeConns = append(closeConns, c.collectDrainedLocked()...)
	if len(closeConns) == 0 {
		c.closeWhenIdle = true
	}
	c.mtx.Unlock()

	c.closeHandles(closeConns)
	return nil
}

// TeardownServiceClient tears down the client immediately. It is used by runner
// shutdown and intentionally does not wait for long-running connector tasks to complete.
func (c *c1ServiceClient) TeardownServiceClient() error {
	var closeConns []*c1ServiceConn
	c.mtx.Lock()
	c.closed = true
	if c.dialDone != nil {
		if c.dialCancel != nil {
			c.dialCancel()
			c.dialCancel = nil
		}
		close(c.dialDone)
		c.dialDone = nil
	}
	if c.active != nil {
		closeConns = append(closeConns, c.active)
		c.active = nil
	}
	closeConns = append(closeConns, c.draining...)
	c.draining = nil
	c.closeWhenIdle = false
	c.mtx.Unlock()

	var retErr error
	for _, handle := range closeConns {
		if handle == nil || handle.cc == nil {
			continue
		}
		retErr = errors.Join(retErr, handle.cc.Close())
	}
	return retErr
}

func (c *c1ServiceClient) Hello(ctx context.Context, in *v1.BatonServiceHelloRequest) (*v1.BatonServiceHelloResponse, error) {
	ctx, span := tracer.Start(ctx, "c1ServiceClient.Hello")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	in.SetHostId(c.getHostID())

	var resp *v1.BatonServiceHelloResponse
	err = c.doRPC(ctx, true, func(client v1.BatonServiceClient) error {
		resp, err = client.Hello(ctx, in)
		return err
	})
	return resp, err
}

func (c *c1ServiceClient) GetTask(ctx context.Context, in *v1.BatonServiceGetTaskRequest) (*v1.BatonServiceGetTaskResponse, error) {
	ctx, span := tracer.Start(ctx, "c1ServiceClient.GetTask")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	in.SetHostId(c.getHostID())

	var resp *v1.BatonServiceGetTaskResponse
	err = c.doRPC(ctx, false, func(client v1.BatonServiceClient) error {
		resp, err = client.GetTask(ctx, in)
		return err
	})
	return resp, err
}

func (c *c1ServiceClient) Heartbeat(ctx context.Context, in *v1.BatonServiceHeartbeatRequest) (*v1.BatonServiceHeartbeatResponse, error) {
	ctx, span := tracer.Start(ctx, "c1ServiceClient.Heartbeat")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	in.SetHostId(c.getHostID())

	var resp *v1.BatonServiceHeartbeatResponse
	err = c.doRPC(ctx, true, func(client v1.BatonServiceClient) error {
		resp, err = client.Heartbeat(ctx, in)
		return err
	})
	return resp, err
}

func (c *c1ServiceClient) FinishTask(ctx context.Context, in *v1.BatonServiceFinishTaskRequest) (*v1.BatonServiceFinishTaskResponse, error) {
	ctx, span := tracer.Start(ctx, "c1ServiceClient.FinishTask")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	in.SetHostId(c.getHostID())

	var resp *v1.BatonServiceFinishTaskResponse
	err = c.doRPC(ctx, false, func(client v1.BatonServiceClient) error {
		resp, err = client.FinishTask(ctx, in)
		return err
	})
	return resp, err
}

func (c *c1ServiceClient) Upload(ctx context.Context, task *v1.Task, r io.ReadSeeker) error {
	ctx, span := tracer.Start(ctx, "c1ServiceClient.Upload")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx)
	const maxAttempts = 3
	for i := range maxAttempts {
		err = c.upload(ctx, task, r)
		if err == nil {
			return nil
		}
		l.Warn("failed to upload asset", zap.Error(err))
		if i < maxAttempts-1 {
			backoff := time.Second * time.Duration(i)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return err
}

func (c *c1ServiceClient) upload(ctx context.Context, task *v1.Task, r io.ReadSeeker) error {
	ctx, span := tracer.Start(ctx, "c1ServiceClient.Upload")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx)

	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		l.Error("failed to seek to start of upload asset", zap.Error(err))
		return err
	}

	client, conn, err := c.beginRPC(ctx)
	if err != nil {
		l.Error("failed to get client connection", zap.Error(err))
		return err
	}
	defer func() { c.endRPC(conn, err) }()

	uc, err := client.UploadAsset(ctx)
	if err != nil {
		l.Error("UploadAsset returned error", zap.Error(err))
		return err
	}

	hasher := sha256.New()
	rLen, err := io.Copy(hasher, r)
	if err != nil {
		l.Error("failed to calculate sha256 of upload asset", zap.Error(err))
		return err
	}
	shaChecksum := hasher.Sum(nil)

	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		l.Error("failed to seek to start of upload asset", zap.Error(err))
		return err
	}

	err = uc.Send(v1.BatonServiceUploadAssetRequest_builder{
		Metadata: v1.BatonServiceUploadAssetRequest_UploadMetadata_builder{
			HostId: c.getHostID(),
			TaskId: task.GetId(),
		}.Build(),
	}.Build())
	if err != nil {
		l.Error("failed to send upload metadata", zap.Error(err))
		return err
	}

	chunkCount := int(math.Ceil(float64(rLen) / float64(fileChunkSize)))
	for i := range chunkCount {
		l.Debug(
			"sending upload chunk",
			zap.Int("chunk", i),
			zap.Int("total_chunks", chunkCount),
		)

		chunkSize := fileChunkSize
		if i == chunkCount-1 {
			chunkSize = int(rLen) - i*fileChunkSize
		}

		chunk := make([]byte, chunkSize)
		_, err = r.Read(chunk)
		if err != nil {
			l.Error("failed to read upload asset", zap.Error(err))
			return err
		}

		err = uc.Send(v1.BatonServiceUploadAssetRequest_builder{
			Data: v1.BatonServiceUploadAssetRequest_UploadData_builder{
				Data: chunk,
			}.Build(),
		}.Build())
		if err != nil {
			l.Error("failed to send upload chunk", zap.Error(err))
			return err
		}
	}

	err = uc.Send(v1.BatonServiceUploadAssetRequest_builder{
		Eof: v1.BatonServiceUploadAssetRequest_UploadEOF_builder{
			Sha256Checksum: shaChecksum,
		}.Build(),
	}.Build())
	if err != nil {
		l.Error("failed to send upload metadata", zap.Error(err))
		return err
	}

	_, err = uc.CloseAndRecv()
	if err != nil {
		l.Error("failed to close upload client", zap.Error(err))
		return err
	}

	l.Info("uploaded asset", zap.String("task_id", task.GetId()), zap.Int64("size", rLen))
	return nil
}

// newServiceClient creates a client and dials to the gRPC server set with BATON_C1_API_HOST.
func newServiceClient(ctx context.Context, clientID string, clientSecret string) (BatonServiceClient, error) {
	credProvider, clientName, tokenHost, err := ugrpc.NewC1CredentialProvider(ctx, clientID, clientSecret)
	if err != nil {
		return nil, err
	}

	if envHost, ok := os.LookupEnv("BATON_C1_API_HOST"); ok {
		tokenHost = envHost
	}
	// assume the token host does not have a port set, and we should use the default https port
	addr := ugrpc.HostPort(tokenHost, "443")
	host, port, err := net.SplitHostPort(tokenHost)
	if err == nil {
		addr = ugrpc.HostPort(host, port)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})),
		grpc.WithPerRPCCredentials(credProvider),
		grpc.WithUserAgent(fmt.Sprintf("%s baton-sdk/%s", clientName, sdk.Version)),
		grpc.WithBlock(), //nolint:staticcheck // grpc.WithBlock is deprecated but we are using it still.
	}

	return &c1ServiceClient{
		addr:     addr,
		dialOpts: dialOpts,
	}, nil
}
