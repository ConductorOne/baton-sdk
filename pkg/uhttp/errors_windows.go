//go:build windows

package uhttp

import (
	"syscall"

	"golang.org/x/sys/windows"
)

// Winsock reports socket failures with WSAE* codes, which are distinct
// syscall.Errno values from the POSIX names of the same conditions.
// syscall.Errno.Is on Windows only bridges the permission/exist/not-exist/
// unsupported families, so matching the POSIX names alone silently fails to
// classify any real socket error on Windows. Both spellings are listed here,
// which is also how oauth2.go's text-only classifier learns the Winsock
// message strings.
//
// WSAECONNABORTED covers teardowns POSIX reports as ECONNRESET or EPIPE, so it
// belongs to the reset class rather than being a condition of its own, and
// WSAESHUTDOWN ("cannot send after socket shutdown") is the Winsock
// counterpart of writing to a pipe the peer already closed.
var transientSocketConditions = []socketCondition{
	{err: syscall.ECONNRESET, class: socketReset},
	{err: windows.WSAECONNRESET, class: socketReset},
	{err: windows.WSAECONNABORTED, class: socketReset},
	{err: syscall.ECONNREFUSED, class: socketRefused},
	{err: windows.WSAECONNREFUSED, class: socketRefused},
	{err: syscall.EPIPE, class: socketBrokenPipe},
	{err: windows.WSAESHUTDOWN, class: socketBrokenPipe},
	{err: syscall.EHOSTUNREACH, class: socketNetworkUnreachable},
	{err: syscall.ENETUNREACH, class: socketNetworkUnreachable},
	{err: syscall.ENETDOWN, class: socketNetworkUnreachable},
	{err: windows.WSAEHOSTUNREACH, class: socketNetworkUnreachable},
	{err: windows.WSAENETUNREACH, class: socketNetworkUnreachable},
	{err: windows.WSAENETDOWN, class: socketNetworkUnreachable},
	{err: syscall.ETIMEDOUT, class: socketTimeout},
	{err: windows.WSAETIMEDOUT, class: socketTimeout},
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

// isSocketTimeout restores the parity that net.Error.Timeout() misses on
// Windows: syscall.Errno.Timeout() recognizes only the POSIX ETIMEDOUT value,
// so a WSAETIMEDOUT would otherwise fall through unclassified.
func isSocketTimeout(err error) bool {
	return hasSocketClass(err, socketTimeout)
}
