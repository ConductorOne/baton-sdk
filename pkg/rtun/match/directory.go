package match

import (
	"context"
	"errors"
	"time"
)

var ErrNotImplemented = errors.New("rtun/match: not implemented")

type Directory interface {
	Advertise(ctx context.Context, serverID string, addr string, ttl time.Duration) error
	Revoke(ctx context.Context, serverID string) error
	Resolve(ctx context.Context, serverID string) (addr string, err error)
}
