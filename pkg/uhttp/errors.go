package uhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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

// requestNeverSent reports whether err was raised before any request bytes
// were written: the TCP dial (including the dial to an HTTP proxy, which
// net/http wraps with Op "proxyconnect"). Retrying such failures is safe
// for every method because the server never saw the request.
func requestNeverSent(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial" || opErr.Op == "proxyconnect"
	}
	return false
}

// isStaleConnectionError reports whether err looks like a pooled connection
// that died between requests: the reset/EOF classes a proxy or origin
// produces when it tore the connection down while it sat in the pool.
func isStaleConnectionError(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		isHTTP2ClientConnectionLost(err)
}

// declaredIdempotent mirrors net/http's Request.isReplayable: safe methods
// are idempotent by definition, and other methods may declare idempotence
// with an Idempotency-Key header (https://golang.org/issue/19943).
func declaredIdempotent(req *http.Request) bool {
	switch req.Method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	}
	return req.Header.Get("Idempotency-Key") != "" || req.Header.Get("X-Idempotency-Key") != ""
}
