package uhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/pkg/ratelimit"
)

// oauth2FetchTokenPrefix is the text golang.org/x/oauth2 puts on a token
// request that failed before a token could be parsed out of the response.
// It gates classifyFlattenedOAuth2TokenError, the one classifier here that
// reads message text instead of error identity.
const oauth2FetchTokenPrefix = "oauth2: cannot fetch token" //nolint:gosec // G101: a message prefix, not a credential

// maxErrorParamLength bounds an RFC 6749 error param before it becomes part
// of a grpc status message. It applies whether the param came off the
// *oauth2.RetrieveError or out of the body: x/oauth2 populates
// ErrorDescription from up to 1 MiB of response body on the
// clientcredentials and oauth2.Config paths, and that whole body can be one
// JSON string field.
const maxErrorParamLength = 256

// ClassifyOAuth2TokenError attaches a grpc status to a golang.org/x/oauth2
// token-exchange failure, so retry.Retryer can act on it: a token endpoint
// that answered 500 or timed out is Unavailable/DeadlineExceeded (retryable),
// while a rejected credential stays Unauthenticated or PermissionDenied
// (terminal). Without it every token failure reaches the syncer as
// codes.Unknown, which retry.Retryer treats as terminal, so a sub-second
// token-endpoint blip discards a whole sync.
//
// Connectors that build their own oauth2.TokenSource call this directly or
// go through NewClassifyingTokenSource; the AuthCredentials helpers in
// authcredentials.go already do.
func ClassifyOAuth2TokenError(err error) error {
	if err == nil {
		return nil
	}

	// An error that already carries a status was classified by a producer
	// closer to the failure, usually Transport.RoundTrip in this package.
	// status.Code reads the outermost status, so re-wrapping here would
	// bury the code the retryer needs behind this one.
	if status.Code(err) != codes.Unknown {
		return err
	}

	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) {
		return classifyOAuth2RetrieveError(retrieveErr, err)
	}

	if classified := classifyFlattenedOAuth2TokenError(err); classified != nil {
		return classified
	}

	return wrapTransientNetworkError(err)
}

// NewClassifyingTokenSource returns a TokenSource that runs every error from
// inner through ClassifyOAuth2TokenError. Wrap the outermost source, the one
// clientcredentials.Config.TokenSource / jwt.Config.TokenSource /
// oauth2.Config.TokenSource return, so the reuse-and-refresh caching those
// build stays in place and only a real token fetch is classified.
func NewClassifyingTokenSource(inner oauth2.TokenSource) oauth2.TokenSource {
	if inner == nil {
		return nil
	}
	return &classifyingTokenSource{inner: inner}
}

type classifyingTokenSource struct {
	inner oauth2.TokenSource
}

var _ oauth2.TokenSource = (*classifyingTokenSource)(nil)

func (c *classifyingTokenSource) Token() (*oauth2.Token, error) {
	token, err := c.inner.Token()
	if err != nil {
		return nil, ClassifyOAuth2TokenError(err)
	}
	return token, nil
}

// classifyOAuth2RetrieveError maps a token endpoint's rejection to a grpc
// code. It is reached from ClassifyOAuth2TokenError, which x/oauth2's token
// sources feed, and from wrapTransientNetworkError, for the callers that
// drive a token endpoint through this package's transport directly.
func classifyOAuth2RetrieveError(retrieveErr *oauth2.RetrieveError, err error) error {
	tokenErr := oauth2TokenErrorFrom(retrieveErr)

	// A transient token-endpoint status (429/5xx) stays retryable even if
	// the body also carries a recognized RFC 6749 error param; otherwise
	// the error param takes priority over the HTTP status, since some
	// servers report it on a 200 and 400 is the spec default for
	// invalid_client/invalid_grant, both of which GrpcCodeFromHTTPStatus
	// alone would misclassify.
	if tokenErr.resp != nil && isTransientHTTPStatus(tokenErr.resp.StatusCode) {
		return wrapTransientOAuth2TokenError(tokenErr, err)
	}
	if code, ok := oauth2TokenErrorCode(tokenErr.code); ok {
		return WrapErrors(code, tokenErr.message(), err)
	}
	code := codes.Unknown
	if tokenErr.resp != nil {
		code = GrpcCodeFromHTTPStatus(tokenErr.resp.StatusCode)
	}
	return WrapErrors(code, tokenErr.message(), err)
}

// oauth2TokenError is the RFC 6749 §5.2 view of a rejected token request:
// the error/error_description pair, plus the response that carried them.
// The pair is not always on the *oauth2.RetrieveError — jwt.Config's token
// source builds that error from the response alone (jwt/jwt.go:143 in
// x/oauth2 v0.36.0) and leaves both fields empty — so oauth2TokenErrorFrom
// recovers it from the body.
type oauth2TokenError struct {
	resp        *http.Response
	code        string
	description string
}

func oauth2TokenErrorFrom(retrieveErr *oauth2.RetrieveError) oauth2TokenError {
	tokenErr := oauth2TokenError{
		resp:        retrieveErr.Response,
		code:        retrieveErr.ErrorCode,
		description: retrieveErr.ErrorDescription,
	}
	if tokenErr.code == "" {
		code, description := oauth2ErrorParamsFromBody(retrieveErr.Response, retrieveErr.Body)
		tokenErr.code = code
		if tokenErr.description == "" {
			tokenErr.description = description
		}
	}

	// Bound both params here rather than at each source, so nothing reaches
	// message() or wrapTransientOAuth2TokenError unbounded.
	tokenErr.code = truncateErrorParam(tokenErr.code)
	tokenErr.description = truncateErrorParam(tokenErr.description)
	return tokenErr
}

// message prefers the RFC 6749 error/error_description pair the token
// endpoint sent, since that survives even when the HTTP status alone would
// be misleading (e.g. a 200 response carrying an error body).
func (e oauth2TokenError) message() string {
	switch {
	case e.code != "" && e.description != "":
		return fmt.Sprintf("%s: %s", e.code, e.description)
	case e.code != "":
		return e.code
	case e.resp != nil && e.description != "":
		return fmt.Sprintf("%s: %s", e.resp.Status, e.description)
	case e.resp != nil:
		return e.resp.Status
	case e.description != "":
		return e.description
	default:
		return "oauth2 token request failed"
	}
}

// wrapTransientOAuth2TokenError mirrors WrapErrorsWithRateLimitInfo's detail
// attachment (retry.Retryer reads it for rate-limit-aware backoff), while
// keeping the description in the message the way oauth2TokenError.message
// does elsewhere in this file.
func wrapTransientOAuth2TokenError(tokenErr oauth2TokenError, err error) error {
	msg := tokenErr.resp.Status
	if tokenErr.description != "" {
		msg = fmt.Sprintf("%s: %s", msg, tokenErr.description)
	}
	st := status.New(GrpcCodeFromHTTPStatus(tokenErr.resp.StatusCode), msg)
	if description, rlErr := ratelimit.ExtractRateLimitData(tokenErr.resp.StatusCode, &tokenErr.resp.Header); rlErr == nil {
		if withDetails, detailsErr := st.WithDetails(description); detailsErr == nil {
			st = withDetails
		}
	}
	return errors.Join(st.Err(), err)
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

// oauth2TokenErrorCode maps an RFC 6749 token-error "error" parameter to a
// grpc code. ok is false when errCode is empty or unrecognized, signaling
// the caller to fall back to the HTTP status.
func oauth2TokenErrorCode(errCode string) (codes.Code, bool) {
	switch errCode {
	case "invalid_client", "invalid_grant":
		return codes.Unauthenticated, true
	case "unauthorized_client", "access_denied":
		return codes.PermissionDenied, true
	case "invalid_scope", "invalid_request", "unsupported_grant_type", "unsupported_response_type":
		return codes.InvalidArgument, true
	// RFC 6749 §4.1.2.1 defines these two as transient by construction, so
	// they are retryable whatever status the endpoint pairs them with.
	case "server_error", "temporarily_unavailable":
		return codes.Unavailable, true
	default:
		return codes.Unknown, false
	}
}

// oauth2ErrorParamsFromBody recovers the RFC 6749 error/error_description
// pair from the response body, for the token sources that leave those fields
// unset on *oauth2.RetrieveError.
func oauth2ErrorParamsFromBody(resp *http.Response, body []byte) (string, string) {
	if len(body) == 0 {
		return "", ""
	}

	contentType := ""
	if resp != nil {
		contentType, _, _ = mime.ParseMediaType(resp.Header.Get(ContentType))
	}

	switch contentType {
	// Mirrors x/oauth2's own content-type handling: some token endpoints
	// answer with a query string rather than JSON.
	case "application/x-www-form-urlencoded", "text/plain":
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return "", ""
		}
		return vals.Get("error"), vals.Get("error_description")
	default:
		return oauth2ErrorParamsFromJSONBody(body)
	}
}

func oauth2ErrorParamsFromJSONBody(body []byte) (string, string) {
	var parsed struct {
		Error            json.RawMessage `json:"error"`
		ErrorDescription string          `json:"error_description"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", ""
	}
	description := parsed.ErrorDescription
	if len(parsed.Error) == 0 {
		return "", description
	}

	var errCode string
	if err := json.Unmarshal(parsed.Error, &errCode); err == nil {
		return errCode, description
	}

	// RFC 6749 §5.2 defines "error" as a string. An endpoint that answers
	// with anything else carries no error param this can act on, so the
	// code stays empty and classification falls back to the HTTP status.
	// The body becomes the description instead, bounded: it is the only
	// detail such an endpoint gave, and no shape can be assumed of it.
	// (Google's token endpoint nests a code/message/status object there.)
	if description == "" {
		description = strings.TrimSpace(string(body))
	}
	return "", description
}

func truncateErrorParam(param string) string {
	if len(param) <= maxErrorParamLength {
		return param
	}
	runes := []rune(param)
	if len(runes) <= maxErrorParamLength {
		return param
	}
	return string(runes[:maxErrorParamLength]) + "..."
}

// grpcStatusInText matches what status.Err().Error() prints, which is all
// that is left of a classified error once x/oauth2 flattens it with %v.
var grpcStatusInText = regexp.MustCompile(`rpc error: code = ([A-Za-z]+) desc = ([^\n]*)`)

// grpcCodeByName inverts codes.Code.String(), for reading a code back out of
// flattened status text. codes.Unauthenticated is the highest code defined.
var grpcCodeByName = func() map[string]codes.Code {
	byName := make(map[string]codes.Code, codes.Unauthenticated+1)
	for code := codes.OK; code <= codes.Unauthenticated; code++ {
		byName[code.String()] = code
	}
	return byName
}()

type flattenedTokenFailure struct {
	substr string
	code   codes.Code
	msg    string
}

// flattenedTokenFailures classifies the token-request failures that reach a
// caller as text only. Its socket entries are built from
// transientSocketConditions, the same per-platform list the predicates in
// errors_other.go and errors_windows.go match on, with each errno's own
// Error() string as the text the OS wrote into the flattened message. So the
// two classifiers cannot disagree about a socket failure, and the Winsock
// spellings are covered on Windows without being transcribed by hand.
// TestFlattenedAndTypedClassificationAgree walks that list and requires both
// paths to land on one code.
//
// Order matters. "no such host" comes before the timeout entries because a
// resolver failure text can carry both, and EOF comes last because
// "unexpected EOF" contains it.
var flattenedTokenFailures = buildFlattenedTokenFailures()

func buildFlattenedTokenFailures() []flattenedTokenFailure {
	failures := []flattenedTokenFailure{
		{substr: "no such host", code: codes.InvalidArgument, msg: "dns lookup failed: NXDOMAIN"},
		{substr: "server misbehaving", code: codes.Unavailable, msg: "temporary dns lookup failure"},
		{substr: "context deadline exceeded", code: codes.DeadlineExceeded, msg: "request timeout"},
		{substr: "Client.Timeout exceeded", code: codes.DeadlineExceeded, msg: "request timeout"},
		{substr: "TLS handshake timeout", code: codes.DeadlineExceeded, msg: "request timeout"},
		{substr: "i/o timeout", code: codes.DeadlineExceeded, msg: "request timeout"},
	}

	for _, condition := range transientSocketConditions {
		classification := socketClassifications[condition.class]
		failures = append(failures, flattenedTokenFailure{
			substr: condition.err.Error(),
			code:   classification.code,
			msg:    classification.msg,
		})
	}

	return append(failures,
		flattenedTokenFailure{substr: "http2: client connection lost", code: codes.Unavailable, msg: "http2 client connection lost"},
		flattenedTokenFailure{substr: "EOF", code: codes.Unavailable, msg: "connection closed before response"},
	)
}

// classifyFlattenedOAuth2TokenError classifies a token failure whose error
// identity x/oauth2 destroyed. jwt/jwt.go:135, :140 and :154 (x/oauth2
// v0.36.0) wrap the transport failure with %v, not %w, so the status this
// package's transport attached, and the *url.Error and syscall errno under
// it, are gone by the time any caller sees the error — errors.As and
// status.Code cannot recover them, and no dependency bump fixes it. Text is
// the only evidence left, so this is the one classifier here that reads it,
// gated on the prefix x/oauth2 puts on exactly these failures. It returns
// nil when the text carries nothing it can act on.
func classifyFlattenedOAuth2TokenError(err error) error {
	msg := err.Error()
	if !strings.Contains(msg, oauth2FetchTokenPrefix) {
		return nil
	}

	// A status this package's transport attached survives flattening as
	// text, and it is a better answer than any substring match: it was
	// decided from the typed error.
	if match := grpcStatusInText.FindStringSubmatch(msg); match != nil {
		if code, ok := grpcCodeByName[match[1]]; ok && code != codes.OK && code != codes.Unknown {
			return WrapErrors(code, fmt.Sprintf("%s: %s", oauth2FetchTokenPrefix, match[2]), err)
		}
	}

	for _, failure := range flattenedTokenFailures {
		if strings.Contains(msg, failure.substr) {
			return WrapErrors(failure.code, fmt.Sprintf("%s: %s", oauth2FetchTokenPrefix, failure.msg), err)
		}
	}
	return nil
}
