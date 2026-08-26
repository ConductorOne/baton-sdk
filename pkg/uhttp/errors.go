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

	// A failed OAuth2 token exchange (rejected client credentials, wrong
	// scope, expired secret, etc.) surfaces here as *oauth2.RetrieveError
	// wrapping the token endpoint's real response — not as a network blip.
	// RFC 6749 §5.2's "error" parameter is the authoritative signal: some
	// servers report it on an HTTP 200 (x/oauth2 still treats that as a
	// RetrieveError), and the spec's own default status for invalid_client/
	// invalid_grant is 400, which GrpcCodeFromHTTPStatus maps to
	// InvalidArgument — not the Unauthenticated/PermissionDenied a bad-
	// credentials rejection should produce. Only fall back to the HTTP
	// status when the server didn't send a recognized error code. Callers
	// (e.g. exit.LogExit) rely on this mapping to tell a real auth failure
	// apart from an unclassified error.
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) {
		if code, ok := oauthTokenErrorCode(retrieveErr.ErrorCode); ok {
			return WrapErrors(code, oauthTokenErrorMessage(retrieveErr), err)
		}
		if retrieveErr.Response != nil {
			return WrapErrors(GrpcCodeFromHTTPStatus(retrieveErr.Response.StatusCode), oauthTokenErrorMessage(retrieveErr), err)
		}
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
		return WrapErrors(codes.Unavailable, "connection reset", err)
	}
	if isConnectionRefused(err) {
		return WrapErrors(codes.Unavailable, "connection refused", err)
	}
	if isBrokenPipe(err) {
		return WrapErrors(codes.Unavailable, "broken pipe", err)
	}
	if isNetworkUnreachable(err) {
		return WrapErrors(codes.Unavailable, "network unreachable", err)
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

// oauthTokenErrorCode maps an RFC 6749 §5.2 token-error "error" parameter to
// a grpc code. ok is false when errCode is empty or not one of the values
// the spec defines, signaling the caller to fall back to the HTTP status.
func oauthTokenErrorCode(errCode string) (code codes.Code, ok bool) {
	switch errCode {
	case "invalid_client", "unauthorized_client":
		return codes.Unauthenticated, true
	case "access_denied":
		return codes.PermissionDenied, true
	case "invalid_grant", "invalid_scope", "invalid_request", "unsupported_grant_type", "unsupported_response_type":
		return codes.InvalidArgument, true
	default:
		return codes.Unknown, false
	}
}

// oauthTokenErrorMessage prefers the RFC 6749 error/error_description pair
// the token endpoint sent, since that survives even when the HTTP status
// alone would be misleading (e.g. a 200 response carrying an error body).
func oauthTokenErrorMessage(retrieveErr *oauth2.RetrieveError) string {
	switch {
	case retrieveErr.ErrorCode != "" && retrieveErr.ErrorDescription != "":
		return fmt.Sprintf("%s: %s", retrieveErr.ErrorCode, retrieveErr.ErrorDescription)
	case retrieveErr.ErrorCode != "":
		return retrieveErr.ErrorCode
	case retrieveErr.Response != nil:
		return retrieveErr.Response.Status
	default:
		return "oauth2 token request failed"
	}
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
