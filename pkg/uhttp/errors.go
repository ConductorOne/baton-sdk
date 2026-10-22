package uhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// classifyTransportFailure classifies err. endpoint is the request the caller
// has in hand: Transport.RoundTrip classifies the dial error before net/http
// wraps it in a *url.Error, so the URL is not in err yet. Pass "" when it is.
func classifyTransportFailure(err error, endpoint string) error {
	if err == nil {
		return nil
	}
	endpoint = failedEndpoint(err, endpoint)

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
		return wrapSocketClass(socketTimeout, err, endpoint)
	}
	// Winsock timeouts do not satisfy the check above, so they need their own.
	if isSocketTimeout(err) {
		return wrapSocketClass(socketTimeout, err, endpoint)
	}

	return err
}

// wrapRequestError classifies a request that got no response.
func wrapRequestError(err error) error {
	if err == nil {
		return nil
	}
	//nolint:errorlint // http.Client.Do returns this wrapper, and its text is what a reader sees first.
	if urlErr, ok := err.(*url.Error); ok && urlErr.Err != nil {
		err = withoutRequestURL(urlErr)
	}
	// Transport.RoundTrip classified this one: one request states one status.
	if status.Code(err) != codes.Unknown {
		return err
	}
	return classifyTransportFailure(err, "")
}

// withoutRequestURL is urlErr stating only what it wraps. http.Client.Do
// prints the request URL ahead of that, query string included, and
// status.FromError copies the whole text into the status message of a wrapped
// status. A connector is free to send a credential as a query parameter. What
// names the call instead is the description classifyTransportFailure writes
// for the class it recognizes, and the connector's own wrapper otherwise, as
// it is for an HTTP error status. The *url.Error stays reachable for
// errors.Is and errors.As.
func withoutRequestURL(urlErr *url.Error) error {
	return &maskedError{text: urlErr.Err.Error(), err: urlErr}
}

// maskedError states text in place of what err prints. errors.Is and
// errors.As still reach err.
type maskedError struct {
	text string
	err  error
}

func (e *maskedError) Error() string { return e.text }
func (e *maskedError) Unwrap() error { return e.err }

// requestURL is u without its query string, which is where a connector may
// send a credential. The scheme stays, so the description states a URL and a
// request that is not https is visible in it. The path stays percent-encoded:
// a decoded '%0A' would add a line to the description that reads as a status
// of its own.
func requestURL(u *url.URL) string {
	if u.Scheme == "" {
		return u.Host + u.EscapedPath()
	}
	return u.Scheme + "://" + u.Host + u.EscapedPath()
}

// endpointOf is the request a *url.Error in err was for.
func endpointOf(err error) string {
	var urlErr *url.Error
	if err == nil || !errors.As(err, &urlErr) {
		return ""
	}
	u, parseErr := url.Parse(urlErr.URL)
	if parseErr != nil {
		return ""
	}
	return requestURL(u)
}

// requestEndpoint is the request RoundTrip was given.
func requestEndpoint(req *http.Request) string {
	if req == nil || req.URL == nil {
		return ""
	}
	return requestURL(req.URL)
}

// failedEndpoint is what the request failed to reach. An empty endpoint falls
// back to a *url.Error already in err, which is what a caller downstream of
// RoundTrip has. A proxyconnect error is raised before the target host sees
// the request, so naming the request's own endpoint there reports the target
// as the host that refused.
func failedEndpoint(err error, endpoint string) string {
	if endpoint == "" {
		endpoint = endpointOf(err)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "proxyconnect" {
		return endpoint
	}
	proxy := "proxy"
	var dialErr *net.OpError
	if errors.As(opErr.Err, &dialErr) && dialErr.Addr != nil {
		proxy += " " + dialErr.Addr.String()
	}
	if endpoint == "" {
		return proxy
	}
	return proxy + " for " + endpoint
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
// predicates, the messages classifyTransportFailure attaches, and the
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
