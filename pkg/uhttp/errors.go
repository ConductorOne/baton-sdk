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

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wrapTransientNetworkError mirrors Baton HTTP retry classification for callers
// that use the transport directly, such as oauth2-backed SDK clients.
func wrapTransientNetworkError(err error) error {
	if err == nil {
		return nil
	}

	// A rejected token request is classified in oauth2.go, from the RFC 6749
	// error param rather than the HTTP status alone. This branch is total
	// once errors.As matches, so a RetrieveError is never run through the
	// network-error checks below.
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) {
		return classifyOAuth2RetrieveError(retrieveErr, err)
	}

	if errors.Is(err, io.ErrUnexpectedEOF) {
		return WrapErrors(codes.Unavailable, "unexpected EOF", err)
	}
	// A bare EOF reaching the caller means the peer closed the connection
	// before any response headers arrived, usually a pooled connection it
	// had already torn down.
	if errors.Is(err, io.EOF) {
		return WrapErrors(codes.Unavailable, "connection closed before response", err)
	}
	if isConnectionReset(err) {
		return wrapSocketClass(socketReset, err)
	}
	if isConnectionRefused(err) {
		return wrapSocketClass(socketRefused, err)
	}
	if isBrokenPipe(err) {
		return wrapSocketClass(socketBrokenPipe, err)
	}
	if isNetworkUnreachable(err) {
		return wrapSocketClass(socketNetworkUnreachable, err)
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		switch {
		case dnsErr.IsTimeout:
			return WrapErrors(codes.DeadlineExceeded, "dns lookup timeout", err)
		case dnsErr.IsTemporary:
			return WrapErrors(codes.Unavailable, "temporary dns lookup failure", err)
		case dnsErr.IsNotFound:
			return WrapErrors(codes.InvalidArgument, "dns lookup failed: NXDOMAIN", err)
		default:
			return WrapErrors(codes.Unavailable, "dns lookup failed", err)
		}
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
	// Winsock timeouts do not satisfy the check above, so they need their own.
	if isSocketTimeout(err) {
		return WrapErrors(codes.DeadlineExceeded, fmt.Sprintf("network timeout: %v", err), err)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request timeout")
	}

	return err
}

// socketClass groups the socket failures the platform predicates in
// errors_other.go and errors_windows.go match. Each platform lists the
// errnos it spells a class with in transientSocketConditions, so the
// predicates, the messages wrapTransientNetworkError attaches, and the
// text-only table in oauth2.go all derive from one list and cannot drift
// apart.
type socketClass int

const (
	socketReset socketClass = iota
	socketRefused
	socketBrokenPipe
	socketNetworkUnreachable
	socketTimeout
)

// socketCondition is one platform spelling of a socketClass.
type socketCondition struct {
	err   error
	class socketClass
}

// socketClassifications is the classification each class receives.
// socketTimeout's message is reached only from oauth2.go's text-only table:
// on the typed path a timeout is caught by the net.Error branch below (or by
// isSocketTimeout on Windows), which keeps the underlying error in the
// message.
var socketClassifications = map[socketClass]struct {
	code codes.Code
	msg  string
}{
	socketReset:              {code: codes.Unavailable, msg: "connection reset"},
	socketRefused:            {code: codes.Unavailable, msg: "connection refused"},
	socketBrokenPipe:         {code: codes.Unavailable, msg: "broken pipe"},
	socketNetworkUnreachable: {code: codes.Unavailable, msg: "network unreachable"},
	socketTimeout:            {code: codes.DeadlineExceeded, msg: "network timeout"},
}

// hasSocketClass reports whether err is any platform spelling of class.
func hasSocketClass(err error, class socketClass) bool {
	for _, condition := range transientSocketConditions {
		if condition.class == class && errors.Is(err, condition.err) {
			return true
		}
	}
	return false
}

func wrapSocketClass(class socketClass, err error) error {
	classification := socketClassifications[class]
	return WrapErrors(classification.code, classification.msg, err)
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
	return isConnectionReset(err) ||
		isBrokenPipe(err) ||
		errors.Is(err, io.EOF) ||
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
