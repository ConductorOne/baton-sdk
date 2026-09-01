package uhttp

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Parses the link header and returns a map of rel values to URLs.
func TestParseLinkHeader(t *testing.T) {
	//nolint:revive // This is fine
	// Example link header value: <https://api.github.com/repositories/1300192/issues?page=2>; rel="prev", <https://api.github.com/repositories/1300192/issues?page=4>; rel="next", <https://api.github.com/repositories/1300192/issues?page=515>; rel="last", <https://api.github.com/repositories/1300192/issues?page=1>; rel="first"

	//nolint:revive // This is fine
	header := `<https://api.github.com/repositories/1300192/issues?page=2>; rel="prev", <https://api.github.com/repositories/1300192/issues?page=4>; rel="next", <https://api.github.com/repositories/1300192/issues?page=515>; rel="last", <https://api.github.com/repositories/1300192/issues?page=1>; rel="first"`

	links, err := parseLinkHeader(header)
	require.Nil(t, err)
	require.Equal(t, "https://api.github.com/repositories/1300192/issues?page=2", links["prev"])
	require.Equal(t, "https://api.github.com/repositories/1300192/issues?page=4", links["next"])
	require.Equal(t, "https://api.github.com/repositories/1300192/issues?page=515", links["last"])
	require.Equal(t, "https://api.github.com/repositories/1300192/issues?page=1", links["first"])
}

// The pagination object is a pointer, so it is nil only when the API omitted it.
type pagedResponse struct {
	XMLName    xml.Name    `json:"-" xml:"response"`
	Items      []string    `json:"items" xml:"items"`
	Pagination *pageCursor `json:"pagination" xml:"pagination"`
}

type pageCursor struct {
	NextCursor string `json:"next_cursor" xml:"next_cursor"`
}

func (p *pagedResponse) HasPaginationData() bool {
	return p.Pagination != nil
}

func newPaginationResponse(statusCode int, contentType string, body string) *WrapperResponse {
	header := http.Header{}
	if contentType != "" {
		header.Set(ContentType, contentType)
	}
	return &WrapperResponse{
		Header:     header,
		Status:     http.StatusText(statusCode),
		StatusCode: statusCode,
		Body:       []byte(body),
	}
}

func TestWithPaginationData_DecodesWhenPresent(t *testing.T) {
	var target pagedResponse
	resp := newPaginationResponse(http.StatusOK, applicationJSON, `{"items":["a"],"pagination":{"next_cursor":"abc"}}`)

	require.NoError(t, WithPaginationData(&target)(resp))
	require.Equal(t, []string{"a"}, target.Items)
	require.Equal(t, "abc", target.Pagination.NextCursor)
}

// The last page is not an error: the object is there, its cursor is just empty.
func TestWithPaginationData_LastPageIsNotAnError(t *testing.T) {
	var target pagedResponse
	resp := newPaginationResponse(http.StatusOK, applicationJSON, `{"items":["a"],"pagination":{"next_cursor":""}}`)

	require.NoError(t, WithPaginationData(&target)(resp))
	require.Equal(t, "", target.Pagination.NextCursor)
}

// The case this option exists for: still 200 with items, but no pagination data.
func TestWithPaginationData_MissingPaginationErrors(t *testing.T) {
	var target pagedResponse
	resp := newPaginationResponse(http.StatusOK, applicationJSON, `{"items":["a"]}`)

	err := WithPaginationData(&target)(resp)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMissingPaginationData)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, []string{"a"}, target.Items, "the body should still be decoded")
}

// Error responses carry no pagination data by design; the HTTP error is the real
// failure and must not be buried under a pagination error.
func TestWithPaginationData_SkipsErrorResponses(t *testing.T) {
	for _, statusCode := range []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusFound} {
		var target pagedResponse
		resp := newPaginationResponse(statusCode, applicationJSON, `{"message":"nope"}`)
		require.NoError(t, WithPaginationData(&target)(resp), "status %d", statusCode)
	}
}

func TestWithPaginationData_XML(t *testing.T) {
	const withCursor = `<response><items>a</items><pagination><next_cursor>abc</next_cursor></pagination></response>`
	const withoutPagination = `<response><items>a</items></response>`

	var target pagedResponse
	require.NoError(t, WithPaginationData(&target)(newPaginationResponse(http.StatusOK, applicationXML, withCursor)))
	require.Equal(t, []string{"a"}, target.Items)
	require.Equal(t, "abc", target.Pagination.NextCursor)

	var missing pagedResponse
	err := WithPaginationData(&missing)(newPaginationResponse(http.StatusOK, applicationXML, withoutPagination))
	require.ErrorIs(t, err, ErrMissingPaginationData)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, []string{"a"}, missing.Items, "the body should still be decoded")
}

// Neither JSON nor XML: still an error, but not reported as missing pagination.
func TestWithPaginationData_UnsupportedContentTypeErrors(t *testing.T) {
	var target pagedResponse
	resp := newPaginationResponse(http.StatusOK, "text/html", `<html></html>`)

	err := WithPaginationData(&target)(resp)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrMissingPaginationData, "a content-type change should not be reported as missing pagination")
}

func TestWithPaginationData_NilResponse(t *testing.T) {
	resp := newPaginationResponse(http.StatusOK, applicationJSON, `{}`)

	err := WithPaginationData(nil)(resp)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// End to end through Do.
func TestWithPaginationData_ThroughDo(t *testing.T) {
	var body string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ContentType, applicationJSON)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer ts.Close()

	client, err := NewBaseHttpClientWithContext(ctx, http.DefaultClient)
	require.NoError(t, err)
	u, err := url.Parse(ts.URL)
	require.NoError(t, err)

	do := func() error {
		req, err := client.NewRequest(ctx, http.MethodPost, u, WithAcceptJSONHeader())
		require.NoError(t, err)
		var target pagedResponse
		resp, err := client.Do(req, WithPaginationData(&target))
		if resp != nil {
			defer resp.Body.Close()
		}
		return err
	}

	body = `{"items":["a"]}`
	require.ErrorIs(t, do(), ErrMissingPaginationData)

	body = `{"items":["a"],"pagination":{"next_cursor":"abc"}}`
	require.NoError(t, do())
}

// A value receiver, so a non-pointer of this type still satisfies PaginatedResponse.
type valuePagedResponse struct {
	Pagination *pageCursor `json:"pagination"`
}

func (p valuePagedResponse) HasPaginationData() bool {
	return p.Pagination != nil
}

// json.Unmarshal leaves a field untouched when its key is absent, so a response declared
// outside the page loop would report the previous page's cursor as its own. The reuse is
// the caller error, not the missing cursor, so it must not be reported as one.
func TestWithPaginationData_ReusedResponseRejected(t *testing.T) {
	var target pagedResponse

	first := newPaginationResponse(http.StatusOK, applicationJSON, `{"items":["a"],"pagination":{"next_cursor":"abc"}}`)
	require.NoError(t, WithPaginationData(&target)(first))

	second := newPaginationResponse(http.StatusOK, applicationJSON, `{"items":["b"]}`)
	err := WithPaginationData(&target)(second)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotErrorIs(t, err, ErrMissingPaginationData)
	require.Equal(t, []string{"a"}, target.Items, "the rejected request must not decode into the target")
}

// xml.Unmarshal appends to existing slices rather than replacing them, so a reused
// response silently accumulates items on top of inheriting pagination data.
func TestWithPaginationData_ReusedResponseRejectedXML(t *testing.T) {
	const body = `<response><items>a</items><items>b</items><pagination><next_cursor>abc</next_cursor></pagination></response>`

	var target pagedResponse
	require.NoError(t, WithPaginationData(&target)(newPaginationResponse(http.StatusOK, applicationXML, body)))

	err := WithPaginationData(&target)(newPaginationResponse(http.StatusOK, applicationXML, body))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, []string{"a", "b"}, target.Items, "the rejected request must not append to the target")
}

// Reuse is the common case, but any dirty target is refused: the option cannot tell a
// stale cursor apart from one the caller set deliberately.
func TestWithPaginationData_PrePopulatedResponseRejected(t *testing.T) {
	target := pagedResponse{Items: []string{"kept"}}
	resp := newPaginationResponse(http.StatusOK, applicationJSON, `{"items":["a"],"pagination":{"next_cursor":"abc"}}`)

	err := WithPaginationData(&target)(resp)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, []string{"kept"}, target.Items)
}

// A dirty target is a caller bug regardless of what the server said, so the zero-value
// check runs ahead of the status check and reports it even on an error response.
func TestWithPaginationData_ReusedResponseRejectedOnErrorResponses(t *testing.T) {
	target := pagedResponse{Pagination: &pageCursor{NextCursor: "abc"}}
	resp := newPaginationResponse(http.StatusInternalServerError, applicationJSON, `{"message":"nope"}`)

	err := WithPaginationData(&target)(resp)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotErrorIs(t, err, ErrMissingPaginationData)
}

// A typed nil is not caught by an `any == nil` check, since the interface still carries a type.
func TestWithPaginationData_TypedNilResponse(t *testing.T) {
	resp := newPaginationResponse(http.StatusOK, applicationJSON, `{}`)

	err := WithPaginationData((*pagedResponse)(nil))(resp)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotErrorIs(t, err, ErrMissingPaginationData)
}

// Nothing can be decoded into a non-pointer, so reject it rather than reporting the
// zero value's missing pagination.
func TestWithPaginationData_NonPointerResponse(t *testing.T) {
	resp := newPaginationResponse(http.StatusOK, applicationJSON, `{"pagination":{"next_cursor":"abc"}}`)

	err := WithPaginationData(valuePagedResponse{})(resp)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotErrorIs(t, err, ErrMissingPaginationData)
}
