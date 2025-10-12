package transport

import "errors"

var (
	ErrConnReset = errors.New("rtun: connection reset")
	ErrClosed    = errors.New("rtun: closed")
	ErrTimeout   = errors.New("rtun: deadline exceeded")
)
