//go:build windows

package uhttp

import (
	"errors"
	"syscall"

	"golang.org/x/sys/windows"
)

// Winsock reports socket failures with WSAE* codes, which are distinct
// syscall.Errno values from the POSIX names of the same conditions.
// syscall.Errno.Is on Windows only bridges the permission/exist/not-exist/
// unsupported families, so matching the POSIX names alone silently fails to
// classify any real socket error on Windows. Each predicate therefore accepts
// both spellings.

func isConnectionReset(err error) bool {
	// Windows reports as WSAECONNABORTED some teardowns that POSIX reports as
	// ECONNRESET or EPIPE, so it belongs with the reset class rather than
	// being a separate condition.
	return errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, windows.WSAECONNRESET) ||
		errors.Is(err, windows.WSAECONNABORTED)
}

func isConnectionRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, windows.WSAECONNREFUSED)
}

func isBrokenPipe(err error) bool {
	// WSAESHUTDOWN ("cannot send after socket shutdown") is the Winsock
	// counterpart of writing to a pipe the peer already closed.
	return errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, windows.WSAESHUTDOWN)
}

func isNetworkUnreachable(err error) bool {
	return errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.ENETDOWN) ||
		errors.Is(err, windows.WSAEHOSTUNREACH) ||
		errors.Is(err, windows.WSAENETUNREACH) ||
		errors.Is(err, windows.WSAENETDOWN)
}

// isSocketTimeout restores the parity that net.Error.Timeout() misses on
// Windows: syscall.Errno.Timeout() recognizes only the POSIX ETIMEDOUT value,
// so a WSAETIMEDOUT would otherwise fall through unclassified.
func isSocketTimeout(err error) bool {
	return errors.Is(err, windows.WSAETIMEDOUT)
}
