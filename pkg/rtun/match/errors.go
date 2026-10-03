package match

import "errors"

var (
	// ErrClientOffline indicates no servers currently advertise the client.
	ErrClientOffline = errors.New("rtun/match: client offline")
)
