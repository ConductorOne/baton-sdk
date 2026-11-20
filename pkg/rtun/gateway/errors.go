package gateway

import "errors"

var (
	ErrNotFound    = errors.New("rtun/gateway: client not found on this server")
	ErrInvalidGSID = errors.New("rtun/gateway: invalid or duplicate gSID")
	ErrProtocol    = errors.New("rtun/gateway: protocol error")
)
