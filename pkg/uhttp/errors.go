package uhttp

import (
	"context"
	"crypto/tls"
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
	return classifyTransportFailure(err, endpointOf(err))
}

// classifyTransportFailure classifies err. endpoint is the host and path of
// the request when the caller has it: Transport.RoundTrip classifies the
// dial error before net/http wraps it in a *url.Error, so the URL is not in
// err yet. An empty endpoint falls back to a *url.Error already in err.
func classifyTransportFailure(err error, endpoint string) error {
	if err == nil {
		return nil
	}
	if endpoint == "" {
		endpoint = endpointOf(err)
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
		return WrapErrors(codes.Unavailable, withEndpoint("unexpected EOF", endpoint), err)
	}
	// A bare EOF reaching the caller means the peer closed the connection
	// before any response headers arrived, usually a pooled connection it
	// had already torn down.
	if errors.Is(err, io.EOF) {
		return WrapErrors(codes.Unavailable, withEndpoint("connection closed before response", endpoint), err)
	}
	if isConnectionReset(err) {
		return wrapSocketClass(socketReset, err, endpoint)
	}
	if isConnectionRefused(err) {
		return wrapSocketClass(socketRefused, err, endpoint)
	}
	if isBrokenPipe(err) {
		return wrapSocketClass(socketBrokenPipe, err, endpoint)
	}
	if isNetworkUnreachable(err) {
		return wrapSocketClass(socketNetworkUnreachable, err, endpoint)
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		switch {
		case dnsErr.IsTimeout:
			return WrapErrors(codes.DeadlineExceeded, withEndpoint("dns lookup timeout", endpoint), err)
		case dnsErr.IsTemporary:
			return WrapErrors(codes.Unavailable, withEndpoint("temporary dns lookup failure", endpoint), err)
		case dnsErr.IsNotFound:
			return WrapErrors(codes.InvalidArgument, withEndpoint("dns lookup failed: NXDOMAIN", endpoint), err)
		default:
			return WrapErrors(codes.Unavailable, withEndpoint("dns lookup failed", endpoint), err)
		}
	}

	if isHTTP2ClientConnectionLost(err) {
		return WrapErrors(codes.Unavailable, withEndpoint("http2 client connection lost", endpoint), err)
	}

	// A certificate the client does not trust will not become trusted on a
	// retry, so this is terminal: the fix is in the deployment's trust store
	// or the server's certificate.
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return WrapErrors(codes.FailedPrecondition, withEndpoint("tls certificate verification failed", endpoint), err)
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return WrapErrors(codes.DeadlineExceeded, withEndpoint("request timeout", endpoint), err)
		}
		if urlErr.Temporary() {
			return WrapErrors(codes.Unavailable, withEndpoint("temporary error", endpoint), err)
		}
	}

	// Ahead of the net.Error check, which context.DeadlineExceeded also
	// satisfies, so the caller's deadline reads the same at the RoundTrip
	// level as it does wrapped in a *url.Error.
	if errors.Is(err, context.DeadlineExceeded) {
		return WrapErrors(codes.DeadlineExceeded, withEndpoint("request timeout", endpoint), err)
	}
	if errors.Is(err, context.Canceled) {
		return WrapErrors(codes.Canceled, withEndpoint("request canceled", endpoint), err)
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

	return err
}

// wrapRequestError classifies a request that got no response. An error that
// already carries a gRPC status was classified closer to the failure, by
// Transport.RoundTrip, and is returned as it is: one request states one status.
func wrapRequestError(err error) error {
	if err == nil || status.Code(err) != codes.Unknown {
		return err
	}
	return wrapTransientNetworkError(err)
}

// hostAndPath is rawURL without its scheme and query string. The description
// this package writes uses it. A joined *url.Error still prints the full URL,
// and status.FromError copies that text into the status message.
func hostAndPath(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Host + u.Path
}

// endpointOf is the host and path of a *url.Error in err, without the query string.
func endpointOf(err error) string {
	var urlErr *url.Error
	if err == nil || !errors.As(err, &urlErr) {
		return ""
	}
	return hostAndPath(urlErr.URL)
}

// requestEndpoint is the host and path of the request RoundTrip was given.
func requestEndpoint(req *http.Request) string {
	if req == nil || req.URL == nil {
		return ""
	}
	return req.URL.Host + req.URL.Path
}

// withEndpoint appends endpoint to msg. An empty endpoint keeps msg as it is.
func withEndpoint(msg, endpoint string) string {
	if endpoint == "" {
		return msg
	}
	return msg + ": " + endpoint
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

func wrapSocketClass(class socketClass, err error, endpoint string) error {
	classification := socketClassifications[class]
	return WrapErrors(classification.code, withEndpoint(classification.msg, endpoint), err)
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
