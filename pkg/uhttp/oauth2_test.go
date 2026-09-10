package uhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/retry"
)

// statusMessage reads the message off the status attached to err. It is not
// status.Convert(err).Message(): grpc replaces that message with the whole
// wrapped error text (status.go:123) when the status is wrapped, which hides
// what the classifier actually chose.
func statusMessage(t *testing.T, err error) string {
	t.Helper()
	var withStatus interface{ GRPCStatus() *status.Status }
	require.True(t, errors.As(err, &withStatus), "no status attached to %v", err)
	return withStatus.GRPCStatus().Message()
}

func newTestRetryer(t *testing.T) *retry.Retryer {
	t.Helper()
	return retry.NewRetryer(t.Context(), retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		MaxDelay:     time.Millisecond,
	})
}

// jsonTokenFailure builds the error x/oauth2's jwt token source produces: a
// *oauth2.RetrieveError carrying the body and response only, with the RFC
// 6749 params left empty.
func jsonTokenFailure(statusCode int, body string) error {
	header := http.Header{}
	header.Set(ContentType, "application/json")
	return &url.Error{
		Op:  "Post",
		URL: "https://example.com/oauth/token",
		Err: &oauth2.RetrieveError{
			Response: &http.Response{
				StatusCode: statusCode,
				Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
				Header:     header,
			},
			Body: []byte(body),
		},
	}
}

func TestClassifyOAuth2TokenError(t *testing.T) {
	formHeader := http.Header{}
	formHeader.Set(ContentType, "application/x-www-form-urlencoded")

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
		// wantUnchanged marks the cases with nothing actionable in them,
		// which must come back as they went in rather than wearing a
		// codes.Unknown status they did not have before.
		wantUnchanged bool
	}{
		{
			// The GCP case from CXP-890: the endpoint answered 500 and the
			// body carries a non-RFC error param, so the status decides.
			name:     "500 with a JSON error param is retryable",
			err:      jsonTokenFailure(http.StatusInternalServerError, `{"error":"internal_failure"}`),
			wantCode: codes.Unavailable,
			wantMsg:  "500 Internal Server Error",
		},
		{
			name:     "503 with an unparseable body falls back to the status",
			err:      jsonTokenFailure(http.StatusServiceUnavailable, "<html>upstream connect error</html>"),
			wantCode: codes.Unavailable,
			wantMsg:  "503 Service Unavailable",
		},
		{
			// A non-string "error" member is not an RFC 6749 error param,
			// so the status decides and the body is kept as the detail.
			// This is the shape Google's token endpoint answers with.
			name:     "non-string error member on a 500 is retryable",
			err:      jsonTokenFailure(http.StatusInternalServerError, `{"error":{"code":500,"message":"Internal error encountered.","status":"INTERNAL"}}`),
			wantCode: codes.Unavailable,
			wantMsg:  `500 Internal Server Error: {"error":{"code":500,"message":"Internal error encountered.","status":"INTERNAL"}}`,
		},
		{
			name:     "non-string error member on a 400 stays terminal",
			err:      jsonTokenFailure(http.StatusBadRequest, `{"error":{"code":400,"message":"Invalid grant: account not found"}}`),
			wantCode: codes.InvalidArgument,
			wantMsg:  `400 Bad Request: {"error":{"code":400,"message":"Invalid grant: account not found"}}`,
		},
		{
			name:     "error_description wins over the body of a non-string error member",
			err:      jsonTokenFailure(http.StatusBadRequest, `{"error":["invalid_grant"],"error_description":"account not found"}`),
			wantCode: codes.InvalidArgument,
			wantMsg:  "400 Bad Request: account not found",
		},
		{
			name:     "invalid_grant recovered from the body is Unauthenticated",
			err:      jsonTokenFailure(http.StatusBadRequest, `{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid_grant: Invalid JWT Signature.",
		},
		{
			name:     "invalid_client recovered from the body is Unauthenticated",
			err:      jsonTokenFailure(http.StatusUnauthorized, `{"error":"invalid_client"}`),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid_client",
		},
		{
			name:     "unauthorized_client recovered from the body is PermissionDenied",
			err:      jsonTokenFailure(http.StatusBadRequest, `{"error":"unauthorized_client"}`),
			wantCode: codes.PermissionDenied,
			wantMsg:  "unauthorized_client",
		},
		{
			// An error param on a 200 is why the body is read at all: the
			// status alone would call this a success.
			name:     "error param recovered from a 200 body still classifies",
			err:      jsonTokenFailure(http.StatusOK, `{"error":"invalid_grant"}`),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid_grant",
		},
		{
			name:     "server_error is transient whatever status carries it",
			err:      jsonTokenFailure(http.StatusBadRequest, `{"error":"server_error"}`),
			wantCode: codes.Unavailable,
			wantMsg:  "server_error",
		},
		{
			name:     "temporarily_unavailable is transient whatever status carries it",
			err:      jsonTokenFailure(http.StatusBadRequest, `{"error":"temporarily_unavailable"}`),
			wantCode: codes.Unavailable,
			wantMsg:  "temporarily_unavailable",
		},
		{
			name: "form-encoded body params are recovered too",
			err: &url.Error{Op: "Post", URL: "https://example.com/oauth/token", Err: &oauth2.RetrieveError{
				Response: &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request", Header: formHeader},
				Body:     []byte("error=invalid_grant&error_description=expired+refresh+token"),
			}},
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid_grant: expired refresh token",
		},
		{
			// The params on the error win over the body: x/oauth2 already
			// parsed them for the paths that populate them.
			name: "populated RFC params take precedence over the body",
			err: &url.Error{Op: "Post", URL: "https://example.com/oauth/token", Err: &oauth2.RetrieveError{
				Response:  &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
				ErrorCode: "invalid_scope",
				Body:      []byte(`{"error":"invalid_grant"}`),
			}},
			wantCode: codes.InvalidArgument,
			wantMsg:  "invalid_scope",
		},
		{
			name:     "empty body with no params falls back to the status",
			err:      jsonTokenFailure(http.StatusForbidden, ""),
			wantCode: codes.PermissionDenied,
			wantMsg:  "403 Forbidden",
		},
		{
			name:     "a body carrying only a description keeps it alongside the status",
			err:      jsonTokenFailure(http.StatusBadRequest, `{"error_description":"missing assertion"}`),
			wantCode: codes.InvalidArgument,
			wantMsg:  "400 Bad Request: missing assertion",
		},
		{
			// jwt.Config's token source builds this shape whenever the
			// response itself could not be read, so Response is nil while
			// the body it did read is not.
			name: "no response at all still yields the body description",
			err: &url.Error{Op: "Post", URL: "https://example.com/oauth/token", Err: &oauth2.RetrieveError{
				Body: []byte(`{"error_description":"no response"}`),
			}},
			wantCode: codes.Unknown,
			wantMsg:  "no response",
		},
		{
			name: "unparseable form body falls back to the status",
			err: &url.Error{Op: "Post", URL: "https://example.com/oauth/token", Err: &oauth2.RetrieveError{
				Response: &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request", Header: formHeader},
				Body:     []byte("error=%zz"),
			}},
			wantCode: codes.InvalidArgument,
			wantMsg:  "400 Bad Request",
		},
		{
			name:     "flattened timeout is DeadlineExceeded",
			err:      fmt.Errorf(`oauth2: cannot fetch token: Post "https://oauth2.googleapis.com/token": context deadline exceeded`),
			wantCode: codes.DeadlineExceeded,
			wantMsg:  "request timeout",
		},
		{
			name:     "flattened client timeout is DeadlineExceeded",
			err:      fmt.Errorf(`oauth2: cannot fetch token: Post "https://example.com/token": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`),
			wantCode: codes.DeadlineExceeded,
			wantMsg:  "request timeout",
		},
		{
			name:     "flattened connection reset is Unavailable",
			err:      fmt.Errorf(`oauth2: cannot fetch token: Post "https://example.com/token": read tcp 10.0.0.1:52000->10.0.0.2:443: read: connection reset by peer`),
			wantCode: codes.Unavailable,
			wantMsg:  "connection reset",
		},
		{
			name:     "flattened EOF is Unavailable",
			err:      fmt.Errorf(`oauth2: cannot fetch token: Post "https://example.com/token": EOF`),
			wantCode: codes.Unavailable,
			wantMsg:  "connection closed before response",
		},
		{
			// NXDOMAIN is terminal here for the same reason it is terminal
			// in wrapTransientNetworkError: a hostname that does not
			// resolve is a misconfiguration.
			name:     "flattened NXDOMAIN stays terminal",
			err:      fmt.Errorf(`oauth2: cannot fetch token: Post "https://example.invalid/token": dial tcp: lookup example.invalid: no such host`),
			wantCode: codes.InvalidArgument,
			wantMsg:  "NXDOMAIN",
		},
		{
			// A status this package's transport attached is recovered from
			// the flattened text rather than re-derived from substrings.
			name:     "flattened grpc status is recovered",
			err:      fmt.Errorf("oauth2: cannot fetch token: Post \"https://example.com/token\": rpc error: code = Unavailable desc = http2 client connection lost\nsomething else"),
			wantCode: codes.Unavailable,
			wantMsg:  "http2 client connection lost",
		},
		{
			// Recovering the code faithfully includes the terminal ones:
			// the transport decided this from the typed error.
			name:     "flattened terminal grpc status is recovered, not upgraded",
			err:      fmt.Errorf(`oauth2: cannot fetch token: Post "https://example.invalid/token": rpc error: code = InvalidArgument desc = dns lookup failed: NXDOMAIN`),
			wantCode: codes.InvalidArgument,
			wantMsg:  "NXDOMAIN",
		},
		{
			// Nothing actionable in the text: staying Unknown is the
			// conservative answer, not a retry.
			name:          "flattened malformed response body stays Unknown",
			err:           fmt.Errorf(`oauth2: cannot fetch token: invalid character '<' looking for beginning of value`),
			wantCode:      codes.Unknown,
			wantMsg:       "cannot fetch token",
			wantUnchanged: true,
		},
		{
			// A network failure that reached the caller with its identity
			// intact is classified by the network rules, not by text.
			name: "typed network error is classified by identity",
			err: &url.Error{Op: "Post", URL: "https://example.com/token", Err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: os.NewSyscallError("read", syscall.ECONNRESET),
			}},
			wantCode: codes.Unavailable,
			wantMsg:  "connection reset",
		},
		{
			name:          "unrelated error is left alone",
			err:           fmt.Errorf("creating JWT config failed: bad key"),
			wantCode:      codes.Unknown,
			wantMsg:       "creating JWT config failed",
			wantUnchanged: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyOAuth2TokenError(tt.err)
			require.Equal(t, tt.wantCode, status.Code(got), "got %v", got)
			require.ErrorIs(t, got, tt.err, "classification must preserve the original error for errors.Is/As")

			if tt.wantUnchanged {
				require.Equal(t, tt.err, got)
				require.Contains(t, got.Error(), tt.wantMsg)
				return
			}
			require.Contains(t, statusMessage(t, got), tt.wantMsg)
		})
	}
}

func TestClassifyOAuth2TokenError_Nil(t *testing.T) {
	require.NoError(t, ClassifyOAuth2TokenError(nil))
}

// A caller closer to the failure already classified it; re-wrapping would
// bury that code behind this one, since status.Code reads the outermost.
func TestClassifyOAuth2TokenError_KeepsAnExistingStatus(t *testing.T) {
	classified := WrapErrors(codes.DeadlineExceeded, "request timeout", fmt.Errorf("oauth2: cannot fetch token: boom"))
	require.Equal(t, classified, ClassifyOAuth2TokenError(classified))
}

// The two classifiers over this domain — ClassifyOAuth2TokenError above the
// token source and wrapTransientNetworkError inside the transport — meet
// whenever a connector wraps a token-backed client in BaseHttpClient, so
// either order has to settle on one code.
func TestClassifyOAuth2TokenError_IsStableUnderReclassification(t *testing.T) {
	for _, err := range []error{
		jsonTokenFailure(http.StatusInternalServerError, `{"error":"internal_failure"}`),
		jsonTokenFailure(http.StatusBadRequest, `{"error":"invalid_grant"}`),
		fmt.Errorf(`oauth2: cannot fetch token: Post "https://example.com/token": context deadline exceeded`),
	} {
		classified := ClassifyOAuth2TokenError(err)
		want := status.Code(classified)

		require.Equal(t, want, status.Code(ClassifyOAuth2TokenError(classified)))
		require.Equal(t, want, status.Code(wrapTransientNetworkError(classified)))
		require.Equal(t, want, status.Code(ClassifyOAuth2TokenError(wrapTransientNetworkError(err))))
	}
}

// The two classifiers over this domain have to agree on every socket
// condition the platform predicates know, not only on the ones a test author
// thought of: which path a failure takes depends solely on whether the caller
// installed this package's transport under oauth2.HTTPClient. Walking
// transientSocketConditions closes that over the platform's own list, so a
// new errno cannot be added to it without both paths being checked, and the
// Winsock spellings are covered when this runs on Windows.
func TestFlattenedAndTypedClassificationAgree(t *testing.T) {
	flatten := func(err error) error {
		//nolint:errorlint // reproduces x/oauth2's %v flattening; %w would defeat the test
		return fmt.Errorf("oauth2: cannot fetch token: %v", err)
	}
	socketFailure := func(inner error) error {
		return &url.Error{Op: "Post", URL: "https://example.com/token", Err: &net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: os.NewSyscallError("connect", inner),
		}}
	}

	require.NotEmpty(t, transientSocketConditions)
	classesPresent := map[socketClass]bool{}
	for _, condition := range transientSocketConditions {
		classesPresent[condition.class] = true
	}
	for class := range socketClassifications {
		require.True(t, classesPresent[class],
			"class %d has no spelling on this platform, so neither classifier can reach it", class)
	}

	for _, condition := range transientSocketConditions {
		t.Run(condition.err.Error(), func(t *testing.T) {
			typed := socketFailure(condition.err)

			typedCode := status.Code(wrapTransientNetworkError(typed))
			require.Equal(t, socketClassifications[condition.class].code, typedCode,
				"the platform predicates and the shared condition list disagree")

			require.Equal(t, typedCode, status.Code(ClassifyOAuth2TokenError(flatten(typed))),
				"same failure, one code, whichever path it takes")
		})
	}

	// The conditions that are not socket errnos, and so are not on the
	// shared list, still have to agree.
	dnsFailure := func(dnsErr *net.DNSError) error {
		return &url.Error{Op: "Post", URL: "https://example.com/token", Err: &net.OpError{Op: "dial", Net: "tcp", Err: dnsErr}}
	}
	others := []struct {
		name  string
		typed error
	}{
		{name: "NXDOMAIN", typed: dnsFailure(&net.DNSError{Err: "no such host", Name: "example.invalid", IsNotFound: true})},
		{name: "temporary dns failure", typed: dnsFailure(&net.DNSError{Err: "server misbehaving", Name: "example.com", IsTemporary: true})},
		{name: "context deadline exceeded", typed: context.DeadlineExceeded},
	}
	for _, tt := range others {
		t.Run(tt.name, func(t *testing.T) {
			typedCode := status.Code(wrapTransientNetworkError(tt.typed))
			require.NotEqual(t, codes.Unknown, typedCode, "the typed classifier must have an answer for this case")
			require.Equal(t, typedCode, status.Code(ClassifyOAuth2TokenError(flatten(tt.typed))))
		})
	}
}

// ETIMEDOUT is the one condition whose typed classification does not come off
// the shared list — net.Error.Timeout() answers it before any predicate runs
// (isSocketTimeout on Windows) — so dropping it from the list would leave the
// flattened path silently unclassified again, which is the gap this table was
// widened to close. The text comes from syscall rather than from the list, so
// this fails if the entry goes away.
func TestClassifyOAuth2TokenError_FlattenedConnectTimeout(t *testing.T) {
	typed := &url.Error{Op: "Post", URL: "https://example.com/token", Err: &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: os.NewSyscallError("connect", syscall.ETIMEDOUT),
	}}
	//nolint:errorlint // reproduces x/oauth2's %v flattening; %w would defeat the test
	flattened := fmt.Errorf("oauth2: cannot fetch token: %v", typed)

	require.Contains(t, flattened.Error(), syscall.ETIMEDOUT.Error())
	require.Equal(t, codes.DeadlineExceeded, status.Code(ClassifyOAuth2TokenError(flattened)))
	require.True(t, newTestRetryer(t).ShouldWaitAndRetry(t.Context(), ClassifyOAuth2TokenError(flattened)),
		"a token fetch whose TCP connect timed out must be retryable")
}

// The table is scanned in order and the first match wins, so an entry whose
// text contains an earlier entry's text can never be reached. Substring
// overlap is only safe when both land on the same code.
func TestFlattenedTokenFailures_OrderIsUnambiguous(t *testing.T) {
	for i, earlier := range flattenedTokenFailures {
		for j, later := range flattenedTokenFailures {
			if i >= j || earlier.code == later.code {
				continue
			}
			require.False(t, strings.Contains(later.substr, earlier.substr),
				"%q (%s) is shadowed by the earlier %q (%s); reorder or split them",
				later.substr, later.code, earlier.substr, earlier.code)
		}
	}
}

// A recovered description reaches a grpc status message and from there the
// logs, and x/oauth2 only caps the body it reads at 1 MiB.
func TestClassifyOAuth2TokenError_TruncatesRecoveredDescription(t *testing.T) {
	body, err := json.Marshal(map[string]string{
		"error":             "invalid_grant",
		"error_description": strings.Repeat("x", 4096),
	})
	require.NoError(t, err)

	got := ClassifyOAuth2TokenError(jsonTokenFailure(http.StatusBadRequest, string(body)))
	require.Equal(t, codes.Unauthenticated, status.Code(got))
	msg := statusMessage(t, got)
	require.Less(t, len(msg), 512, "message %q was not truncated", msg)
	require.Contains(t, msg, "invalid_grant")
	require.True(t, utf8.ValidString(msg), "truncation split a rune: %q", msg)
}

// x/oauth2 populates ErrorDescription itself on the clientcredentials and
// oauth2.Config paths, from up to 1 MiB of body, so the bound has to cover
// the params that arrive on the error and not only what is parsed out of a
// body. Both the terminal and the transient path build the message.
func TestClassifyOAuth2TokenError_TruncatesParamsFromTheError(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		found string
	}{
		{
			name: "terminal path",
			err: &url.Error{Op: "Post", URL: "https://example.com/oauth/token", Err: &oauth2.RetrieveError{
				Response:         &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
				ErrorCode:        "invalid_grant",
				ErrorDescription: strings.Repeat("y", 5000),
			}},
			found: "invalid_grant",
		},
		{
			name: "transient path",
			err: &url.Error{Op: "Post", URL: "https://example.com/oauth/token", Err: &oauth2.RetrieveError{
				Response:         &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable"},
				ErrorDescription: strings.Repeat("z", 5000),
			}},
			found: "503 Service Unavailable",
		},
		{
			name: "error code itself",
			err: &url.Error{Op: "Post", URL: "https://example.com/oauth/token", Err: &oauth2.RetrieveError{
				Response:  &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
				ErrorCode: strings.Repeat("w", 5000),
			}},
			found: "www",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := statusMessage(t, ClassifyOAuth2TokenError(tt.err))
			require.Less(t, len(msg), 512, "message was not truncated: %d bytes", len(msg))
			require.Contains(t, msg, tt.found)
		})
	}
}

// The bound is on runes, so a description that is long in bytes but short in
// characters survives whole.
func TestClassifyOAuth2TokenError_KeepsMultibyteDescriptionUnderTheBound(t *testing.T) {
	description := strings.Repeat("é", maxErrorParamLength-1)
	body, err := json.Marshal(map[string]string{"error": "invalid_grant", "error_description": description})
	require.NoError(t, err)

	got := ClassifyOAuth2TokenError(jsonTokenFailure(http.StatusBadRequest, string(body)))
	require.Equal(t, "invalid_grant: "+description, statusMessage(t, got))
}

func TestClassifyOAuth2TokenError_AttachesRateLimitDetails(t *testing.T) {
	header := http.Header{}
	header.Set(ContentType, "application/json")
	header.Set("Retry-After", "120")
	got := ClassifyOAuth2TokenError(&url.Error{Op: "Post", URL: "https://example.com/token", Err: &oauth2.RetrieveError{
		Response: &http.Response{StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests", Header: header},
		Body:     []byte(`{"error":"rate_limit_exceeded","error_description":"slow down"}`),
	}})

	require.Equal(t, codes.Unavailable, status.Code(got))
	var found *v2.RateLimitDescription
	var withStatus interface{ GRPCStatus() *status.Status }
	require.True(t, errors.As(got, &withStatus))
	for _, detail := range withStatus.GRPCStatus().Details() {
		if rl, ok := detail.(*v2.RateLimitDescription); ok {
			found = rl
		}
	}
	require.NotNil(t, found, "expected a RateLimitDescription detail from the Retry-After header")
	require.Contains(t, statusMessage(t, got), "slow down")
}

// The retryer is the consumer this classification exists for, and the
// anti-goal matters as much as the goal: a rejected credential must not spin
// in a retryer that has no attempt limit at its default settings.
func TestClassifyOAuth2TokenError_RetryerAgreement(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantRetry bool
	}{
		{name: "500 token endpoint", err: jsonTokenFailure(http.StatusInternalServerError, `{"error":"internal_failure"}`), wantRetry: true},
		{name: "429 token endpoint", err: jsonTokenFailure(http.StatusTooManyRequests, ""), wantRetry: true},
		{name: "flattened timeout", err: fmt.Errorf(`oauth2: cannot fetch token: Post "https://example.com/token": context deadline exceeded`), wantRetry: true},
		{name: "invalid_grant", err: jsonTokenFailure(http.StatusBadRequest, `{"error":"invalid_grant"}`), wantRetry: false},
		{name: "invalid_client", err: jsonTokenFailure(http.StatusUnauthorized, `{"error":"invalid_client"}`), wantRetry: false},
		{name: "unauthorized_client", err: jsonTokenFailure(http.StatusForbidden, `{"error":"unauthorized_client"}`), wantRetry: false},
		{name: "invalid_scope", err: jsonTokenFailure(http.StatusBadRequest, `{"error":"invalid_scope"}`), wantRetry: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyOAuth2TokenError(tt.err)
			require.Equal(t, tt.wantRetry, newTestRetryer(t).ShouldWaitAndRetry(t.Context(), got), "classified as %v", got)
		})
	}
}

func TestNewClassifyingTokenSource_NilInner(t *testing.T) {
	require.Nil(t, NewClassifyingTokenSource(nil))
}

func TestNewClassifyingTokenSource_PassesTokensThrough(t *testing.T) {
	want := &oauth2.Token{AccessToken: "test-access-token"}
	got, err := NewClassifyingTokenSource(oauth2.StaticTokenSource(want)).Token()
	require.NoError(t, err)
	require.Equal(t, want.AccessToken, got.AccessToken)
}

// tokenServer answers every request with the given status and body.
func tokenServer(t *testing.T, statusCode int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ContentType, "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func testJWTConfig(tokenURL string) *jwt.Config {
	return &jwt.Config{
		Email:      "test-email",
		TokenURL:   tokenURL,
		PrivateKey: getDummyPrivateKey(),
		Subject:    "test-subject",
	}
}

// The premise of classifyFlattenedOAuth2TokenError: x/oauth2's jwt token
// source wraps a transport failure with %v (jwt/jwt.go:135, :140, :154 in
// v0.36.0), so the *url.Error and any status attached below it are gone
// before a caller sees the error. If a later x/oauth2 stops flattening,
// this test fails and the text-matching fallback can be dropped.
func TestXOauth2JWT_FlattensTransportErrors(t *testing.T) {
	released := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-released
	}))
	defer server.Close()
	defer close(released)

	ctx := context.WithValue(t.Context(), oauth2.HTTPClient, &http.Client{Timeout: 100 * time.Millisecond})
	_, rawErr := testJWTConfig(server.URL).TokenSource(ctx).Token()
	require.Error(t, rawErr)

	var urlErr *url.Error
	require.False(t, errors.As(rawErr, &urlErr),
		"x/oauth2 no longer flattens the transport error; the text fallback can go: %v", rawErr)
	require.Equal(t, codes.Unknown, status.Code(rawErr), "premise: the error carries no status of its own")
	require.Contains(t, rawErr.Error(), oauth2FetchTokenPrefix)

	require.Equal(t, codes.DeadlineExceeded, status.Code(ClassifyOAuth2TokenError(rawErr)),
		"a token endpoint that timed out must be retryable")
}

// The second premise: jwt.Config's token source builds *oauth2.RetrieveError
// from the response alone (jwt/jwt.go:143), so the RFC 6749 params are only
// in the body. This is the GCP shape from CXP-890.
func TestXOauth2JWT_LeavesRFCParamsEmpty(t *testing.T) {
	server := tokenServer(t, http.StatusBadRequest, `{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`)

	_, rawErr := testJWTConfig(server.URL).TokenSource(t.Context()).Token()
	require.Error(t, rawErr)

	var retrieveErr *oauth2.RetrieveError
	require.True(t, errors.As(rawErr, &retrieveErr))
	require.Empty(t, retrieveErr.ErrorCode, "premise: the RFC 6749 params are not parsed on this path")
	require.NotEmpty(t, retrieveErr.Body)

	got := ClassifyOAuth2TokenError(rawErr)
	require.Equal(t, codes.Unauthenticated, status.Code(got))
	require.Contains(t, statusMessage(t, got), "invalid_grant")
	require.False(t, newTestRetryer(t).ShouldWaitAndRetry(t.Context(), got),
		"a rejected credential must not be retried")
}

// A 500 from the token endpoint, end to end through the jwt token source:
// the case that discarded whole syncs.
func TestXOauth2JWT_TransientStatusIsRetryable(t *testing.T) {
	server := tokenServer(t, http.StatusInternalServerError, `{"error":"internal_failure"}`)

	_, rawErr := testJWTConfig(server.URL).TokenSource(t.Context()).Token()
	require.Error(t, rawErr)
	require.Equal(t, codes.Unknown, status.Code(rawErr), "premise: unclassified, so the retryer would give up")

	got := ClassifyOAuth2TokenError(rawErr)
	require.Equal(t, codes.Unavailable, status.Code(got))
	require.True(t, newTestRetryer(t).ShouldWaitAndRetry(t.Context(), got))
}

func TestOAuth2ClientCredentials_GetClientClassifiesTokenFailure(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantCode   codes.Code
	}{
		{name: "500 is retryable", statusCode: http.StatusInternalServerError, body: `{"error":"internal_failure"}`, wantCode: codes.Unavailable},
		{name: "invalid_client is terminal", statusCode: http.StatusUnauthorized, body: `{"error":"invalid_client"}`, wantCode: codes.Unauthenticated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := tokenServer(t, tt.statusCode, tt.body)
			creds := &OAuth2ClientCredentials{cfg: &clientcredentials.Config{
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				TokenURL:     server.URL,
			}}

			client, err := creds.GetClient(t.Context())
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/api", nil)
			require.NoError(t, err)
			resp, err := client.Do(req) //nolint:bodyclose // the token exchange fails before a response exists
			require.Nil(t, resp)
			require.Equal(t, tt.wantCode, status.Code(err), "got %v", err)
		})
	}
}

func TestOAuth2JWT_GetClientClassifiesTokenFailure(t *testing.T) {
	server := tokenServer(t, http.StatusInternalServerError, `{"error":"internal_failure"}`)
	creds := &OAuth2JWT{
		Credentials: []byte("test-credentials"),
		CreateJWTConfig: func(credentials []byte, scopes ...string) (*jwt.Config, error) {
			return testJWTConfig(server.URL), nil
		},
	}

	client, err := creds.GetClient(t.Context())
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/api", nil)
	require.NoError(t, err)
	resp, err := client.Do(req) //nolint:bodyclose // the token exchange fails before a response exists
	require.Nil(t, resp)
	require.Equal(t, codes.Unavailable, status.Code(err), "got %v", err)
}

// An empty access token is what forces this helper to refresh on the first
// request; a stored token with no expiry never refreshes at all.
func TestOAuth2RefreshToken_GetClientClassifiesRefreshFailure(t *testing.T) {
	server := tokenServer(t, http.StatusBadRequest, `{"error":"invalid_grant","error_description":"Token has been expired or revoked."}`)
	creds := NewOAuth2RefreshToken("client-id", "client-secret", "https://example.com/callback", server.URL, "", "refresh-token", nil)

	client, err := creds.GetClient(t.Context())
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/api", nil)
	require.NoError(t, err)
	resp, err := client.Do(req) //nolint:bodyclose // the token exchange fails before a response exists
	require.Nil(t, resp)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "got %v", err)
	require.False(t, newTestRetryer(t).ShouldWaitAndRetry(t.Context(), err))
}

// What a connector can still match on after the helpers classify a token
// failure. The status is added by joining, never by replacing, so both the
// error identity and the text x/oauth2 produced survive: existing
// errors.As/errors.Is checks and message matches in connectors keep working.
func TestOAuth2JWT_GetClientPreservesErrorIdentity(t *testing.T) {
	server := tokenServer(t, http.StatusBadRequest, `{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`)
	creds := &OAuth2JWT{
		Credentials: []byte("test-credentials"),
		CreateJWTConfig: func(credentials []byte, scopes ...string) (*jwt.Config, error) {
			return testJWTConfig(server.URL), nil
		},
	}

	client, err := creds.GetClient(t.Context())
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/api", nil)
	require.NoError(t, err)
	_, doErr := client.Do(req) //nolint:bodyclose // the token exchange fails before a response exists
	require.Error(t, doErr)

	require.Equal(t, codes.Unauthenticated, status.Code(doErr))

	var retrieveErr *oauth2.RetrieveError
	require.True(t, errors.As(doErr, &retrieveErr), "*oauth2.RetrieveError must still be reachable: %v", doErr)
	require.Equal(t, http.StatusBadRequest, retrieveErr.Response.StatusCode)

	require.Contains(t, doErr.Error(), oauth2FetchTokenPrefix, "x/oauth2's own text must survive")
	require.Contains(t, doErr.Error(), "invalid_grant", "the response body must survive")

	// The token source stays an oauth2.TokenSource on an oauth2.Transport,
	// which is the shape connectors reach through when they inspect it.
	oauthTransport, ok := client.Transport.(*oauth2.Transport)
	require.True(t, ok)
	_, tokenErr := oauthTransport.Source.Token()
	require.Equal(t, codes.Unauthenticated, status.Code(tokenErr))
}
