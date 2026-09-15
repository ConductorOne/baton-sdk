//go:build !windows

package uhttp

import (
	"syscall"
)

// The socket-failure spellings below are split per platform because Winsock
// reports these conditions with WSAE* codes that are distinct syscall.Errno
// values from the POSIX names, and syscall.Errno.Is does not bridge the two.
// See errors_windows.go for the Winsock spellings.
var transientSocketConditions = []socketCondition{
	{err: syscall.ECONNRESET, class: socketReset},
	{err: syscall.ECONNABORTED, class: socketReset},
	{err: syscall.ECONNREFUSED, class: socketRefused},
	{err: syscall.EPIPE, class: socketBrokenPipe},
	{err: syscall.EHOSTUNREACH, class: socketNetworkUnreachable},
	{err: syscall.ENETUNREACH, class: socketNetworkUnreachable},
	{err: syscall.ENETDOWN, class: socketNetworkUnreachable},
	{err: syscall.ETIMEDOUT, class: socketTimeout},
}

func isConnectionReset(err error) bool {
	return hasSocketClass(err, socketReset)
}

func isConnectionRefused(err error) bool {
	return hasSocketClass(err, socketRefused)
}

func isBrokenPipe(err error) bool {
	return hasSocketClass(err, socketBrokenPipe)
}

func isNetworkUnreachable(err error) bool {
	return hasSocketClass(err, socketNetworkUnreachable)
}

// isSocketTimeout is always false here: syscall.Errno.Timeout() already reports
// true for ETIMEDOUT, so the net.Error timeout branch in
// wrapTransientNetworkError catches it. ETIMEDOUT is still listed above so
// oauth2.go's text-only classifier knows the spelling this platform prints.
func isSocketTimeout(error) bool {
	return false
}
