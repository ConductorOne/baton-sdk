package server

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// RecommendedGRPCServerOptions returns server options enabling basic keepalive and
// reasonable message size limits suitable for RTUN services.
func RecommendedGRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(4 * 1024 * 1024),
		grpc.MaxSendMsgSize(4 * 1024 * 1024),
		grpc.MaxConcurrentStreams(250),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:              30 * time.Second,
			Timeout:           10 * time.Second,
			MaxConnectionIdle: 0,
			MaxConnectionAge:  0,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}
