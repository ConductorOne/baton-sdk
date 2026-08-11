//go:build !windows

package uhttp

import (
	"errors"
	"syscall"
)

// The socket-failure predicates below are split per platform because Winsock
// reports these conditions with WSAE* codes that are distinct syscall.Errno
// values from the POSIX names, and syscall.Errno.Is does not bridge the two.
// See errors_windows.go for the Winsock spellings.

func isConnectionReset(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED)
}

func isConnectionRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

func isBrokenPipe(err error) bool {
	return errors.Is(err, syscall.EPIPE)
}

func isNetworkUnreachable(err error) bool {
	return errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.ENETDOWN)
}

// isSocketTimeout is always false here: syscall.Errno.Timeout() already reports
// true for ETIMEDOUT, so the net.Error timeout branch in
// wrapTransientNetworkError catches it.
func isSocketTimeout(error) bool {
	return false
}
