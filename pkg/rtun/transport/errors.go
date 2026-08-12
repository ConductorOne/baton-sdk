package transport

import "errors"

var (
	// ErrConnReset indicates the remote reset the connection.
	ErrConnReset = errors.New("rtun: connection reset")
	// ErrTimeout indicates an operation exceeded its deadline.
	ErrTimeout = errors.New("rtun: deadline exceeded")
)
