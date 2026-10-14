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

	// A certificate the client does not trust will not become trusted on a
	// retry, so this is terminal: the fix is in the deployment's trust store
	// or the server's certificate.
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return WrapErrors(codes.FailedPrecondition, "tls certificate verification failed", err)
	}

	// The URL stays out of these messages: its query string can carry
	// credentials. wrapRequestError names the host instead.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return WrapErrors(codes.DeadlineExceeded, "request timeout", urlErr)
		}
		if urlErr.Temporary() {
			return WrapErrors(codes.Unavailable, "temporary error", urlErr)
		}
	}

	// Ahead of the net.Error check, which context.DeadlineExceeded also
	// satisfies, so the caller's deadline reads the same at the RoundTrip
	// level as it does wrapped in a *url.Error.
	if errors.Is(err, context.DeadlineExceeded) {
		return WrapErrors(codes.DeadlineExceeded, "request timeout", err)
	}
	if errors.Is(err, context.Canceled) {
		return WrapErrors(codes.Canceled, "request canceled", err)
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

// grpcStatusCarrier is what status.FromError looks for on an error.
type grpcStatusCarrier interface {
	GRPCStatus() *status.Status
}

// wrapRequestError classifies a request that got no response and names host
// in its status. An error already classified closer to the failure (by
// Transport.RoundTrip) keeps that code and message rather than gaining a
// second status. Unwrap still reaches the original error, so errors.Is and
// errors.As see the *url.Error and the transport error under it.
func wrapRequestError(host string, err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.Unknown {
		err = wrapTransientNetworkError(err)
	}

	var carrier grpcStatusCarrier
	if !errors.As(err, &carrier) || carrier.GRPCStatus().Code() == codes.Unknown {
		return err
	}

	st := carrier.GRPCStatus()
	if host != "" && !strings.Contains(st.Message(), host) {
		p := st.Proto()
		p.Message = fmt.Sprintf("%s to %s", p.GetMessage(), host)
		st = status.FromProto(p)
	}
	allStatuses := func(*status.Status) bool { return true }
	return &restatedError{status: st, cause: withoutStatus(err, allStatuses), err: err}
}

// restatedError states its status once, followed by what the errors under it
// said without their own status lines. The status is reached through Unwrap,
// ahead of the original error, rather than through a GRPCStatus method, so
// status.FromError keeps the whole text, cause included, as it does for an
// errors.Join.
type restatedError struct {
	status *status.Status
	cause  error
	err    error
}

func (e *restatedError) Error() string {
	if e.cause == nil {
		return e.status.Err().Error()
	}
	return e.status.Err().Error() + "\n" + e.cause.Error()
}

func (e *restatedError) Unwrap() []error {
	return []error{e.status.Err(), e.err}
}

// withoutStatus is err without the status lines drop selects and without
// *url.Error wrapping, whose message repeats the request URL and its query
// string. For a request with no response, what is left is what the dialer,
// resolver or TLS stack reported. It inspects one layer at a time, so a type
// switch rather than errors.As.
func withoutStatus(err error, drop func(*status.Status) bool) error {
	switch e := err.(type) { //nolint:errorlint // walks the wrapping layer by layer on purpose
	case *restatedError:
		return e.cause
	case *url.Error:
		return withoutStatus(e.Err, drop)
	case interface{ Unwrap() []error }:
		var kept []error
		for _, child := range e.Unwrap() {
			if cause := withoutStatus(child, drop); cause != nil {
				kept = append(kept, cause)
			}
		}
		switch len(kept) {
		case 0:
			return nil
		case 1:
			return kept[0]
		default:
			return errors.Join(kept...)
		}
	case grpcStatusCarrier:
		if drop(e.GRPCStatus()) {
			return nil
		}
		return err
	default:
		return err
	}
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
