package c1api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
)

const (
	fileChunkSize = 1 * 1024 * 512 // 512KB
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

type c1ServiceClient struct {
	addr     string
	dialOpts []grpc.DialOption
	hostID   string
}

func (c *c1ServiceClient) getHostID() string {
	if c.hostID != "" {
		return c.hostID
	}

	hostID, _ := os.Hostname()
	if envHost, ok := os.LookupEnv("BATON_HOST_ID"); ok {
		hostID = envHost
	}

	if hostID == "" {
		hostID = "baton-sdk"
	}

	c.hostID = hostID
	return c.hostID
}

func (c *c1ServiceClient) getClientConn(ctx context.Context) (v1.BatonServiceClient, func(), error) {
	dialCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	cc, err := grpc.DialContext(
		dialCtx,
		c.addr,
		c.dialOpts...,
	)
	if err != nil {
		return nil, nil, err
	}

	return v1.NewBatonServiceClient(cc), func() {
		err = cc.Close()
		if err != nil {
			ctxzap.Extract(ctx).Error("failed to close client connection", zap.Error(err))
		}
	}, nil
}

func (c *c1ServiceClient) Hello(ctx context.Context, in *v1.BatonServiceHelloRequest) (*v1.BatonServiceHelloResponse, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	in.HostId = c.getHostID()

	return client.Hello(ctx, in)
}

func (c *c1ServiceClient) GetTask(ctx context.Context, in *v1.BatonServiceGetTaskRequest) (*v1.BatonServiceGetTaskResponse, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	in.HostId = c.getHostID()

	return client.GetTask(ctx, in)
}

func (c *c1ServiceClient) Heartbeat(ctx context.Context, in *v1.BatonServiceHeartbeatRequest) (*v1.BatonServiceHeartbeatResponse, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	in.HostId = c.getHostID()

	return client.Heartbeat(ctx, in)
}

func (c *c1ServiceClient) FinishTask(ctx context.Context, in *v1.BatonServiceFinishTaskRequest) (*v1.BatonServiceFinishTaskResponse, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	in.HostId = c.getHostID()

	return client.FinishTask(ctx, in)
}

func (c *c1ServiceClient) Upload(ctx context.Context, task *v1.Task, r io.ReadSeeker) error {
	l := ctxzap.Extract(ctx)

	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return err
	}
	defer done()

	uc, err := client.UploadAsset(ctx)
	if err != nil {
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

	err = uc.Send(&v1.BatonServiceUploadAssetRequest{
		Msg: &v1.BatonServiceUploadAssetRequest_Metadata{
			Metadata: &v1.BatonServiceUploadAssetRequest_UploadMetadata{
				HostId: c.getHostID(),
				TaskId: task.Id,
			},
		},
	})
	if err != nil {
		l.Error("failed to send upload metadata", zap.Error(err))
		return err
	}

	chunkCount := uint64(math.Ceil(float64(rLen) / float64(fileChunkSize)))
	for i := uint64(0); i < chunkCount; i++ {
		l.Debug("sending upload chunk", zap.Uint64("chunk", i), zap.Uint64("total_chunks", chunkCount))

		chunkSize := fileChunkSize
		if i == chunkCount-1 {
			chunkSize = int(rLen) - int(i)*fileChunkSize
		}

		chunk := make([]byte, chunkSize)
		_, err = r.Read(chunk)
		if err != nil {
			l.Error("failed to read upload asset", zap.Error(err))
			return err
		}

		err = uc.Send(&v1.BatonServiceUploadAssetRequest{
			Msg: &v1.BatonServiceUploadAssetRequest_Data{
				Data: &v1.BatonServiceUploadAssetRequest_UploadData{
					Data: chunk,
				},
			},
		})
		if err != nil {
			l.Error("failed to send upload chunk", zap.Error(err))
			return err
		}
	}

	err = uc.Send(&v1.BatonServiceUploadAssetRequest{
		Msg: &v1.BatonServiceUploadAssetRequest_Eof{
			Eof: &v1.BatonServiceUploadAssetRequest_UploadEOF{
				Sha256Checksum: shaChecksum,
			},
		},
	})
	if err != nil {
		l.Error("failed to send upload metadata", zap.Error(err))
		return err
	}

	_, err = uc.CloseAndRecv()
	if err != nil {
		l.Error("failed to close upload client", zap.Error(err))
		return err
	}

	return nil
}

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
		grpc.WithBlock(),
	}

	return &c1ServiceClient{
		addr:     addr,
		dialOpts: dialOpts,
	}, nil
}
