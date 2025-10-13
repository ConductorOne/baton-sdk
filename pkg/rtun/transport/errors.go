package transport

import "errors"

var (
	ErrConnReset = errors.New("rtun: connection reset")
	ErrTimeout   = errors.New("rtun: deadline exceeded")
)
