package match

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/conductorone/baton-sdk/pkg/rtun/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// OwnerRouter helps route work to the server process that owns a client's link.
type OwnerRouter struct {
	Locator   *Locator
	Directory Directory
	DialOpts  []grpc.DialOption
}

// DialOwner resolves the owner of clientID and returns a gRPC connection to that server.
// The caller should use this connection to invoke services on the owner, where the owner
// can use its local Registry.DialContext to perform reverse RPCs.
func (r *OwnerRouter) DialOwner(ctx context.Context, clientID string) (*grpc.ClientConn, string, error) {
	owner, _, err := r.Locator.OwnerOf(ctx, clientID)
	if err != nil {
		return nil, "", fmt.Errorf("rtun: locate owner: %w", err)
	}
	addr, err := r.Directory.Resolve(ctx, owner)
	if err != nil {
		return nil, "", fmt.Errorf("rtun: resolve owner address: %w", err)
	}
	opts := r.DialOpts
	conn, err := grpc.NewClient("passthrough:///"+addr, opts...)
	if err != nil {
		return nil, "", fmt.Errorf("rtun: dial owner: %w", err)
	}
	return conn, owner, nil
}

// LocalReverseDial is a helper to be called ON the owner server process.
// It uses the local Registry to open a reverse connection to the client.
// clientID must be URL-safe; use url.PathEscape if it contains special characters.
func LocalReverseDial(ctx context.Context, reg *server.Registry, clientID string, port uint32) (*grpc.ClientConn, error) {
	u := url.URL{
		Scheme: "rtun",
		Host: net.JoinHostPort(
			clientID,
			strconv.FormatUint(uint64(port), 10),
		),
	}
	addr := u.String()
	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithContextDialer(reg.DialContext),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("rtun: reverse dial: %w", err)
	}
	return conn, nil
}
