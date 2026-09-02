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

	"github.com/conductorone/baton-sdk/pkg/ratelimit"
)

// wrapTransientNetworkError mirrors Baton HTTP retry classification for callers
// that use the transport directly, such as oauth2-backed SDK clients.
func wrapTransientNetworkError(err error) error {
	if err == nil {
		return nil
	}

	// A transient token-endpoint status (429/5xx) stays retryable even if
	// the body also carries a recognized RFC 6749 error param; otherwise
	// the error param takes priority over the HTTP status, since some
	// servers report it on a 200 and 400 is the spec default for
	// invalid_client/invalid_grant, both of which GrpcCodeFromHTTPStatus
	// alone would misclassify. This branch is total once errors.As matches,
	// so a RetrieveError is never run through the network-error checks below.
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) {
		if retrieveErr.Response != nil && isTransientHTTPStatus(retrieveErr.Response.StatusCode) {
			return wrapTransientOAuthTokenError(retrieveErr, err)
		}
		if code, ok := oauthTokenErrorCode(retrieveErr.ErrorCode); ok {
			return WrapErrors(code, oauthTokenErrorMessage(retrieveErr), err)
		}
		code := codes.Unknown
		if retrieveErr.Response != nil {
			code = GrpcCodeFromHTTPStatus(retrieveErr.Response.StatusCode)
		}
		return WrapErrors(code, oauthTokenErrorMessage(retrieveErr), err)
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

// isTransientHTTPStatus reports whether GrpcCodeFromHTTPStatus maps
// statusCode to a code retry.Retryer.ShouldWaitAndRetry treats as retryable
// (Unavailable or DeadlineExceeded), so a transient token-endpoint failure
// stays retryable regardless of what error param the body also carries.
func isTransientHTTPStatus(statusCode int) bool {
	switch GrpcCodeFromHTTPStatus(statusCode) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// wrapTransientOAuthTokenError mirrors WrapErrorsWithRateLimitInfo's detail
// attachment (retry.Retryer reads it for rate-limit-aware backoff), while
// keeping ErrorDescription in the message the way oauthTokenErrorMessage
// does elsewhere in this file.
func wrapTransientOAuthTokenError(retrieveErr *oauth2.RetrieveError, err error) error {
	msg := retrieveErr.Response.Status
	if retrieveErr.ErrorDescription != "" {
		msg = fmt.Sprintf("%s: %s", msg, retrieveErr.ErrorDescription)
	}
	st := status.New(GrpcCodeFromHTTPStatus(retrieveErr.Response.StatusCode), msg)
	if description, rlErr := ratelimit.ExtractRateLimitData(retrieveErr.Response.StatusCode, &retrieveErr.Response.Header); rlErr == nil {
		if withDetails, detailsErr := st.WithDetails(description); detailsErr == nil {
			st = withDetails
		}
	}
	return errors.Join(st.Err(), err)
}

// oauthTokenErrorCode maps an RFC 6749 §5.2 token-error "error" parameter to
// a grpc code. ok is false when errCode is empty or unrecognized, signaling
// the caller to fall back to the HTTP status.
func oauthTokenErrorCode(errCode string) (codes.Code, bool) {
	switch errCode {
	case "invalid_client", "invalid_grant":
		return codes.Unauthenticated, true
	case "unauthorized_client", "access_denied":
		return codes.PermissionDenied, true
	case "invalid_scope", "invalid_request", "unsupported_grant_type", "unsupported_response_type":
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
