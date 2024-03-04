package uhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

type example struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestWrapper_NewBaseHttpClient(t *testing.T) {
	httpClient := http.DefaultClient
	client := NewBaseHttpClient(httpClient)

	require.Equal(t, httpClient, client.HttpClient)
}

func TestWrapper_WithJSONBody(t *testing.T) {
	exampleBody := example{
		Name: "John",
		Age:  30,
	}
	exampleBodyBuffer := new(bytes.Buffer)
	err := json.NewEncoder(exampleBodyBuffer).Encode(exampleBody)
	if err != nil {
		t.Fatal(err)
	}

	option := WithJSONBody(exampleBody)
	buffer, _, err := option()

	require.Nil(t, err)
	require.Equal(t, exampleBodyBuffer, buffer)
}

func TestWrapper_WithAcceptJSONHeader(t *testing.T) {
	option := WithAcceptJSONHeader()
	buffer, headers, err := option()

	require.Nil(t, err)
	require.Nil(t, buffer)
	require.Contains(t, headers, "Accept")
	require.Equal(t, "application/json", headers["Accept"])
}

func TestWrapper_WithContentTypeJSONHeader(t *testing.T) {
	option := WithContentTypeJSONHeader()
	buffer, headers, err := option()

	require.Nil(t, err)
	require.Nil(t, buffer)
	require.Contains(t, headers, "Content-Type")
	require.Equal(t, "application/json", headers["Content-Type"])
}

func TestWrapper_WithJSONResponse(t *testing.T) {
	exampleResponse := example{
		Name: "John",
		Age:  30,
	}
	exampleResponseBuffer := new(bytes.Buffer)
	err := json.NewEncoder(exampleResponseBuffer).Encode(exampleResponse)
	if err != nil {
		t.Fatal(err)
	}

	resp := http.Response{}

	responseBody := example{}
	option := WithJSONResponse(&responseBody)
	wrapperResp := WrapperResponse{
		Header:     resp.Header,
		Body:       exampleResponseBuffer.Bytes(),
		StatusCode: 200,
		Status:     "200 OK",
	}
	err = option(&wrapperResp)

	require.Nil(t, err)
	require.Equal(t, exampleResponse, responseBody)
}

type ErrResponse struct {
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func (e *ErrResponse) Message() string {
	return fmt.Sprintf("%s: %s", e.Title, e.Detail)
}

func TestWrapper_WithErrorResponse(t *testing.T) {
	resp := http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(bytes.NewBufferString(`{"title": "not found", "detail": "resource not found"}`)),
	}

	var errResp ErrResponse
	err := WithErrorResponse(&errResp)(&resp)

	require.NotNil(t, err)
	require.Contains(t, errResp.Message(), "not found")
	require.Contains(t, err.Error(), "not found")
}

func TestWrapper_WithRateLimitData(t *testing.T) {
	n := time.Now()
	resp := &http.Response{
		Header: map[string][]string{
			"X-Ratelimit-Limit":     {"100"},
			"X-Ratelimit-Remaining": {"50"},
			"X-Ratelimit-Reset":     {"60"},
		},
	}

	rldata := &v2.RateLimitDescription{}
	option := WithRatelimitData(rldata)
	err := option(resp)

	require.Nil(t, err)
	require.Equal(t, int64(100), rldata.Limit)
	require.Equal(t, int64(50), rldata.Remaining)
	require.Equal(t, n.Add(time.Second*60).Unix(), rldata.ResetAt.AsTime().Unix())
}

func TestWrapper_NewRequest(t *testing.T) {
	type expected struct {
		method  string
		url     string
		headers http.Header
		body    io.ReadCloser
		err     error
	}

	exampleBody := example{Name: "John", Age: 30}
	exampleBodyBuffer := new(bytes.Buffer)
	err := json.NewEncoder(exampleBodyBuffer).Encode(exampleBody)
	if err != nil {
		t.Fatal(err)
	}

	test := []struct {
		name     string
		method   string
		url      string
		options  []RequestOption
		expected expected
	}{
		{
			name:    "GET request with no options",
			method:  http.MethodGet,
			url:     "http://example.com",
			options: nil,
			expected: expected{
				method:  http.MethodGet,
				url:     "http://example.com",
				headers: http.Header{},
				body:    nil,
				err:     nil,
			},
		},
		{
			name:    "POST request with JSON body",
			method:  http.MethodPost,
			url:     "http://example.com",
			options: []RequestOption{WithJSONBody(exampleBody), WithAcceptJSONHeader()},
			expected: expected{
				method:  http.MethodPost,
				url:     "http://example.com",
				headers: http.Header{"Accept": []string{"application/json"}, "Content-Type": []string{"application/json"}},
				body:    io.NopCloser(exampleBodyBuffer),
				err:     nil,
			},
		},
	}

	for _, tc := range test {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.url)
			if err != nil {
				t.Fatal(err)
			}

			client := NewBaseHttpClient(http.DefaultClient)

			req, err := client.NewRequest(context.Background(), tc.method, u, tc.options...)
			require.Equal(t, tc.expected.err, err)
			require.Equal(t, tc.expected.method, req.Method)
			require.Equal(t, tc.expected.url, req.URL.String())
			require.Equal(t, tc.expected.headers, req.Header)
			require.Equal(t, tc.expected.body, req.Body)
		})
	}
}
