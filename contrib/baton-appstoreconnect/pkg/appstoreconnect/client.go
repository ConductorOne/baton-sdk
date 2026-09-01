package appstoreconnect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// DefaultBaseURL is the App Store Connect API root.
	DefaultBaseURL = "https://api.appstoreconnect.apple.com"

	// DefaultPageSize is Apple's maximum `limit` for the collections this connector reads.
	DefaultPageSize = 200

	// maxIncludedVisibleApps is Apple's maximum for `limit[visibleApps]`. Users with more visible
	// apps than this need a follow-up request against the relationship endpoint.
	maxIncludedVisibleApps = 50

	// rateLimitHeader is Apple's non-standard budget header, e.g.
	// "user-hour-lim:3600;user-hour-rem:3599;".
	rateLimitHeader = "X-Rate-Limit"

	rateLimitKeyLimit     = "user-hour-lim"
	rateLimitKeyRemaining = "user-hour-rem"

	// rateLimitWindow is the width of Apple's published request budget. The header reports what is
	// left but never when the window rolls over, so the worst case is used as the reset hint.
	rateLimitWindow = time.Hour
)

// Client talks to the App Store Connect API v1.
type Client struct {
	*uhttp.BaseHttpClient

	baseURL *url.URL
	tokens  *TokenSource
}

// NewClient builds an App Store Connect API client authenticated with an ES256 API key.
func NewClient(httpClient *http.Client, keyID, issuerID, privateKeyPEM, baseURL string) (*Client, error) {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("baton-appstoreconnect: invalid base URL: %w", err)
	}

	tokens, err := NewTokenSource(keyID, issuerID, privateKeyPEM)
	if err != nil {
		return nil, err
	}

	return &Client{
		BaseHttpClient: uhttp.NewBaseHttpClient(httpClient),
		baseURL:        parsed,
		tokens:         tokens,
	}, nil
}

// ListUsers returns one page of team members. Roles arrive inline on the user record, so a single
// paginated pass covers users and their role assignments with no per-user fan-out. Pass the
// previous call's nextURL to continue; the empty string starts at the first page.
func (c *Client) ListUsers(ctx context.Context, nextURL string) ([]User, string, annotations.Annotations, error) {
	requestURL := nextURL
	if requestURL == "" {
		query := url.Values{}
		query.Set("limit", strconv.Itoa(DefaultPageSize))
		query.Set("include", "visibleApps")
		query.Set("limit[visibleApps]", strconv.Itoa(maxIncludedVisibleApps))
		requestURL = c.url("/v1/users", query)
	}

	var response usersResponse
	annos, err := c.doRequest(ctx, http.MethodGet, requestURL, nil, &response)
	if err != nil {
		return nil, "", annos, err
	}

	return response.Data, response.Links.Next, annos, nil
}

// GetUser returns a single team member.
func (c *Client) GetUser(ctx context.Context, userID string) (*User, annotations.Annotations, error) {
	query := url.Values{}
	query.Set("include", "visibleApps")
	query.Set("limit[visibleApps]", strconv.Itoa(maxIncludedVisibleApps))

	var response userResponse
	annos, err := c.doRequest(ctx, http.MethodGet, c.url("/v1/users/"+url.PathEscape(userID), query), nil, &response)
	if err != nil {
		return nil, annos, err
	}

	return &response.Data, annos, nil
}

// ListApps returns one page of the team's apps.
func (c *Client) ListApps(ctx context.Context, nextURL string) ([]App, string, annotations.Annotations, error) {
	requestURL := nextURL
	if requestURL == "" {
		query := url.Values{}
		query.Set("limit", strconv.Itoa(DefaultPageSize))
		requestURL = c.url("/v1/apps", query)
	}

	var response appsResponse
	annos, err := c.doRequest(ctx, http.MethodGet, requestURL, nil, &response)
	if err != nil {
		return nil, "", annos, err
	}

	return response.Data, response.Links.Next, annos, nil
}

// ListUserVisibleApps returns every app a user can see. This is only needed for users whose
// inlined visibleApps relationship was truncated; the common case is served by ListUsers.
func (c *Client) ListUserVisibleApps(ctx context.Context, userID string) ([]App, annotations.Annotations, error) {
	query := url.Values{}
	query.Set("limit", strconv.Itoa(DefaultPageSize))

	requestURL := c.url("/v1/users/"+url.PathEscape(userID)+"/visibleApps", query)

	var (
		apps  []App
		annos annotations.Annotations
	)
	for requestURL != "" {
		var response appsResponse
		pageAnnos, err := c.doRequest(ctx, http.MethodGet, requestURL, nil, &response)
		annos = mergeAnnotations(annos, pageAnnos)
		if err != nil {
			return nil, annos, err
		}

		apps = append(apps, response.Data...)
		requestURL = response.Links.Next
	}

	return apps, annos, nil
}

// UpdateUser applies a PATCH to a team member. Apple's update semantics are full-replace on both
// the roles array and the visibleApps relationship, so update must be given the complete desired
// state. Callers that are changing one role are expected to read the current state first; that
// read-modify-write is racy against a concurrent change made in the Apple UI or by another grant.
func (c *Client) UpdateUser(ctx context.Context, userID string, update UserUpdate) (*User, annotations.Annotations, error) {
	attributes := map[string]any{}
	if update.Roles != nil {
		attributes["roles"] = update.Roles
	}
	if update.AllAppsVisible != nil {
		attributes["allAppsVisible"] = *update.AllAppsVisible
	}
	if update.ProvisioningAllowed != nil {
		attributes["provisioningAllowed"] = *update.ProvisioningAllowed
	}

	data := map[string]any{
		"type": "users",
		"id":   userID,
	}
	if len(attributes) > 0 {
		data["attributes"] = attributes
	}
	if update.SetVisibleApps {
		data["relationships"] = map[string]any{
			"visibleApps": map[string]any{
				"data": appIdentifiers(update.VisibleAppIDs),
			},
		}
	}

	body := map[string]any{"data": data}

	var response userResponse
	annos, err := c.doRequest(ctx, http.MethodPatch, c.url("/v1/users/"+url.PathEscape(userID), nil), body, &response)
	if err != nil {
		return nil, annos, err
	}

	return &response.Data, annos, nil
}

// DeleteUser removes a member from the team.
func (c *Client) DeleteUser(ctx context.Context, userID string) (annotations.Annotations, error) {
	return c.doRequest(ctx, http.MethodDelete, c.url("/v1/users/"+url.PathEscape(userID), nil), nil, nil)
}

// ListUserInvitations returns one page of outstanding invitations.
func (c *Client) ListUserInvitations(ctx context.Context, nextURL string) ([]UserInvitation, string, annotations.Annotations, error) {
	requestURL := nextURL
	if requestURL == "" {
		query := url.Values{}
		query.Set("limit", strconv.Itoa(DefaultPageSize))
		query.Set("include", "visibleApps")
		query.Set("limit[visibleApps]", strconv.Itoa(maxIncludedVisibleApps))
		requestURL = c.url("/v1/userInvitations", query)
	}

	var response userInvitationsResponse
	annos, err := c.doRequest(ctx, http.MethodGet, requestURL, nil, &response)
	if err != nil {
		return nil, "", annos, err
	}

	return response.Data, response.Links.Next, annos, nil
}

// CreateUserInvitation invites someone to the team. App Store Connect has no way to create an
// account directly: the invitee only becomes a user once they accept with their Apple ID.
func (c *Client) CreateUserInvitation(ctx context.Context, request UserInvitationRequest) (*UserInvitation, annotations.Annotations, error) {
	attributes := map[string]any{
		"email":               request.Email,
		"firstName":           request.FirstName,
		"lastName":            request.LastName,
		"roles":               request.Roles,
		"allAppsVisible":      request.AllAppsVisible,
		"provisioningAllowed": request.ProvisioningAllowed,
	}
	if request.Roles == nil {
		attributes["roles"] = []string{}
	}

	data := map[string]any{
		"type":       "userInvitations",
		"attributes": attributes,
	}
	if !request.AllAppsVisible && len(request.VisibleAppIDs) > 0 {
		data["relationships"] = map[string]any{
			"visibleApps": map[string]any{
				"data": appIdentifiers(request.VisibleAppIDs),
			},
		}
	}

	var response userInvitationResponse
	annos, err := c.doRequest(ctx, http.MethodPost, c.url("/v1/userInvitations", nil), map[string]any{"data": data}, &response)
	if err != nil {
		return nil, annos, err
	}

	return &response.Data, annos, nil
}

// DeleteUserInvitation cancels an outstanding invitation.
func (c *Client) DeleteUserInvitation(ctx context.Context, invitationID string) (annotations.Annotations, error) {
	return c.doRequest(ctx, http.MethodDelete, c.url("/v1/userInvitations/"+url.PathEscape(invitationID), nil), nil, nil)
}

// url resolves a path against the configured base URL.
func (c *Client) url(path string, query url.Values) string {
	resolved := *c.baseURL
	resolved.Path = strings.TrimSuffix(resolved.Path, "/") + path
	if len(query) > 0 {
		resolved.RawQuery = query.Encode()
	}
	return resolved.String()
}

// doRequest performs an authenticated request and decodes the response document.
func (c *Client) doRequest(
	ctx context.Context,
	method string,
	requestURL string,
	body any,
	response any,
) (annotations.Annotations, error) {
	parsedURL, err := url.Parse(requestURL)
	if err != nil {
		return nil, err
	}

	// Mint (or reuse) a token per request rather than per client: Apple caps token lifetime at 20
	// minutes, which is well short of a large sync.
	token, err := c.tokens.Token()
	if err != nil {
		return nil, err
	}

	requestOptions := []uhttp.RequestOption{
		uhttp.WithBearerToken(token),
		uhttp.WithAcceptJSONHeader(),
	}
	if body != nil {
		requestOptions = append(requestOptions, uhttp.WithContentTypeJSONHeader(), uhttp.WithJSONBody(body))
	}

	request, err := c.NewRequest(ctx, method, parsedURL, requestOptions...)
	if err != nil {
		return nil, err
	}

	errorDocument := &ErrorDocument{}
	doOptions := []uhttp.DoOption{uhttp.WithErrorResponse(errorDocument)}
	if response != nil {
		doOptions = append(doOptions, uhttp.WithJSONResponse(response))
	}

	resp, err := c.Do(request, doOptions...)
	if resp != nil {
		defer resp.Body.Close()
	}

	var annos annotations.Annotations
	if resp != nil {
		if rateLimit := extractRateLimitData(resp); rateLimit != nil {
			annos.WithRateLimiting(rateLimit)
		}
	}

	if err != nil {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		return annos, &APIError{StatusCode: statusCode, Document: errorDocument, err: err}
	}

	return annos, nil
}

// APIError carries the HTTP status alongside Apple's error document. uhttp maps every 4xx onto
// codes.InvalidArgument, which throws away the distinction between "gone" and "rejected"; callers
// that need to tell a 404 from a 409 look here instead.
type APIError struct {
	StatusCode int
	Document   *ErrorDocument
	err        error
}

// Error implements error.
func (e *APIError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	if e.Document != nil {
		return e.Document.Message()
	}
	return fmt.Sprintf("App Store Connect API request failed with status %d", e.StatusCode)
}

// Unwrap exposes the underlying uhttp error so gRPC status inspection keeps working.
func (e *APIError) Unwrap() error {
	return e.err
}

// IsNotFound reports whether the API said the record does not exist.
func IsNotFound(err error) bool {
	return hasStatus(err, http.StatusNotFound)
}

// IsConflict reports whether the API rejected the request because the record already exists or is
// in a conflicting state.
func IsConflict(err error) bool {
	return hasStatus(err, http.StatusConflict)
}

// IsForbidden reports whether the API key lacks the access the request needs.
func IsForbidden(err error) bool {
	return hasStatus(err, http.StatusForbidden)
}

// hasStatus reports whether err (or anything it wraps) is an APIError with the given status.
func hasStatus(err error, statusCode int) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.StatusCode == statusCode
}

// HasErrorCode reports whether Apple attached the given machine-readable error code.
func HasErrorCode(err error, code string) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Document == nil {
		return false
	}

	for _, reported := range apiErr.Document.Codes() {
		if reported == code {
			return true
		}
	}
	return false
}

// appIdentifiers converts app IDs into JSON:API resource identifiers. A nil slice becomes an empty
// array so that "no visible apps" is sent explicitly instead of being dropped from the payload.
func appIdentifiers(appIDs []string) []ResourceIdentifier {
	identifiers := make([]ResourceIdentifier, 0, len(appIDs))
	for _, appID := range appIDs {
		identifiers = append(identifiers, ResourceIdentifier{Type: "apps", ID: appID})
	}
	return identifiers
}

// mergeAnnotations appends src onto dst, tolerating a nil dst.
func mergeAnnotations(dst, src annotations.Annotations) annotations.Annotations {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = annotations.Annotations{}
	}
	dst.Merge(src...)
	return dst
}

// extractRateLimitData converts Apple's `x-rate-limit` header into a rate limit annotation. The
// header looks like "user-hour-lim:3600;user-hour-rem:3599;" and carries no reset timestamp, so the
// end of the widest possible window is reported as the reset hint. Returns nil when the header is
// absent and the response is not a 429, since there is nothing useful to report.
func extractRateLimitData(response *http.Response) *v2.RateLimitDescription {
	if response == nil {
		return nil
	}

	header := response.Header.Get(rateLimitHeader)
	overLimit := response.StatusCode == http.StatusTooManyRequests
	if header == "" && !overLimit {
		return nil
	}

	var limit, remaining int64
	for _, part := range strings.Split(header, ";") {
		key, value, found := strings.Cut(strings.TrimSpace(part), ":")
		if !found {
			continue
		}

		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			continue
		}

		switch strings.TrimSpace(strings.ToLower(key)) {
		case rateLimitKeyLimit:
			limit = parsed
		case rateLimitKeyRemaining:
			remaining = parsed
		}
	}

	description := v2.RateLimitDescription_builder{
		Status:    v2.RateLimitDescription_STATUS_OK,
		Limit:     limit,
		Remaining: remaining,
	}.Build()

	if overLimit {
		description.SetStatus(v2.RateLimitDescription_STATUS_OVERLIMIT)
		description.SetRemaining(0)
	}

	if resetAt := resetHint(response, overLimit); resetAt != nil {
		description.SetResetAt(resetAt)
	}

	return description
}

// resetHint prefers an explicit Retry-After and otherwise falls back to the end of Apple's hourly
// budget window, which is the longest a caller could have to wait.
func resetHint(response *http.Response, overLimit bool) *timestamppb.Timestamp {
	if retryAfter := response.Header.Get("Retry-After"); retryAfter != "" {
		if seconds, err := strconv.ParseInt(retryAfter, 10, 64); err == nil {
			return timestamppb.New(time.Now().Add(time.Duration(seconds) * time.Second))
		}
		if at, err := http.ParseTime(retryAfter); err == nil {
			return timestamppb.New(at)
		}
	}

	if overLimit {
		return timestamppb.New(time.Now().Add(rateLimitWindow))
	}

	return nil
}
