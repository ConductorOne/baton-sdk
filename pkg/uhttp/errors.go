package uhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wrapTransientNetworkError mirrors Baton HTTP retry classification for callers
// that use the transport directly, such as oauth2-backed SDK clients.
func wrapTransientNetworkError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return WrapErrors(codes.Unavailable, "unexpected EOF", err)
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return WrapErrors(codes.Unavailable, "connection reset", err)
	}
	if isHTTP2ClientConnectionLost(err) {
		return WrapErrors(codes.Unavailable, "http2 client connection lost", err)
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return WrapErrors(codes.DeadlineExceeded, fmt.Sprintf("request timeout: %v", urlErr.URL), urlErr)
		}
		if urlErr.Temporary() {
			return WrapErrors(codes.Unavailable, fmt.Sprintf("temporary error: %v", urlErr.URL), urlErr)
		}
	}

	// Catches net.Error timeout types not wrapped in url.Error
	// (e.g. tls.handshakeTimeoutError at the RoundTrip level).
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return WrapErrors(codes.DeadlineExceeded, fmt.Sprintf("network timeout: %v", err), err)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request timeout")
	}

	return err
}

func isHTTP2ClientConnectionLost(err error) bool {
	return strings.Contains(err.Error(), "http2: client connection lost")
}
