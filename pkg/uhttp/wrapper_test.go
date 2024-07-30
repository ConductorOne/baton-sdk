package uhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type example struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

var ctx = context.Background()

func TestWrapper_NewBaseHttpClient(t *testing.T) {
	httpClient := http.DefaultClient
	client, err := NewBaseHttpClient(ctx, httpClient)
	require.NoError(t, err)
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

func TestWrapper_WithAcceptXMLHeader(t *testing.T) {
	option := WithAcceptXMLHeader()
	buffer, headers, err := option()

	require.Nil(t, err)
	require.Nil(t, buffer)
	require.Contains(t, headers, "Accept")
	require.Equal(t, "application/xml", headers["Accept"])
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

	responseBody := example{}
	option := WithJSONResponse(&responseBody)

	t.Run("should marshal a JSON response", func(t *testing.T) {
		resp := http.Response{
			Header: map[string][]string{
				"Content-Type": {"application/json"},
			},
		}

		wrapperResp := WrapperResponse{
			Header:     resp.Header,
			Body:       exampleResponseBuffer.Bytes(),
			StatusCode: 200,
			Status:     "200 OK",
		}
		err = option(&wrapperResp)

		require.Nil(t, err)
		require.Equal(t, exampleResponse, responseBody)
	})

	t.Run("should return an error when the response is not JSON", func(t *testing.T) {
		resp := http.Response{
			Header: map[string][]string{
				"Content-Type": {"application/xml"},
			},
		}

		wrapperResp := WrapperResponse{
			Header:     resp.Header,
			Body:       exampleResponseBuffer.Bytes(),
			StatusCode: 200,
			Status:     "200 OK",
		}
		err = option(&wrapperResp)

		require.NotNil(t, err)
	})

	t.Run("should not marshal when the JSON response is empty (HTTP 204)", func(t *testing.T) {
		wrapperResp := WrapperResponse{
			Header: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body:       []byte(""),
			StatusCode: 204,
			Status:     "204 No Content",
		}
		err = WithJSONResponse(nil)(&wrapperResp)

		require.Nil(t, err)
	})

	t.Run("should marshal an empty JSON response (\"{}\")", func(t *testing.T) {
		type empty struct{}

		wrapperResp := WrapperResponse{
			Header: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body:       []byte("{}"),
			StatusCode: 204,
			Status:     "204 No Content",
		}
		responseBody := empty{}
		err = WithJSONResponse(&responseBody)(&wrapperResp)

		require.Nil(t, err)
		require.Equal(t, empty{}, responseBody)
	})
}

func TestWrapper_WithXMLResponse(t *testing.T) {
	exampleResponse := example{
		Name: "John",
		Age:  30,
	}
	exampleResponseBuffer := new(bytes.Buffer)
	err := xml.NewEncoder(exampleResponseBuffer).Encode(exampleResponse)
	if err != nil {
		t.Fatal(err)
	}

	resp := http.Response{
		Header: map[string][]string{
			"Content-Type": {"application/xml"},
		},
	}

	t.Run("should marshal an XML response", func(t *testing.T) {
		responseBody := example{}
		option := WithXMLResponse(&responseBody)
		wrapperResp := WrapperResponse{
			Header:     resp.Header,
			Body:       exampleResponseBuffer.Bytes(),
			StatusCode: 200,
			Status:     "200 OK",
		}
		err = option(&wrapperResp)

		require.Nil(t, err)
		require.Equal(t, exampleResponse, responseBody)
	})

	t.Run("should return an error when the response is not XML", func(t *testing.T) {
		responseBody := example{}
		option := WithXMLResponse(&responseBody)
		wrapperResp := WrapperResponse{
			Header: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body:       exampleResponseBuffer.Bytes(),
			StatusCode: 200,
			Status:     "200 OK",
		}
		err = option(&wrapperResp)

		require.NotNil(t, err)
	})

	t.Run("should not marshal when the XML response is empty (HTTP 204)", func(t *testing.T) {
		wrapperResp := WrapperResponse{
			Header: map[string][]string{
				"Content-Type": {"application/xml"},
			},
			Body:       []byte(""),
			StatusCode: 204,
			Status:     "204 No Content",
		}
		err = WithXMLResponse(nil)(&wrapperResp)

		require.Nil(t, err)
	})
}

func TestWrapper_WithResponse(t *testing.T) {
	exampleResponse := example{
		Name: "John",
		Age:  30,
	}
	exampleResponseBuffer := new(bytes.Buffer)
	err := xml.NewEncoder(exampleResponseBuffer).Encode(exampleResponse)
	if err != nil {
		t.Fatal(err)
	}

	resp := http.Response{
		Header: map[string][]string{
			"Content-Type": {"application/xml"},
		},
	}

	responseBody := example{}
	option := WithResponse(&responseBody)
	wrapperResp := WrapperResponse{
		Header:     resp.Header,
		Body:       exampleResponseBuffer.Bytes(),
		StatusCode: 200,
		Status:     "200 OK",
	}
	err = option(&wrapperResp)

	require.Nil(t, err)
	require.Equal(t, exampleResponse, responseBody)

	exampleResponseBuffer = new(bytes.Buffer)
	err = json.NewEncoder(exampleResponseBuffer).Encode(exampleResponse)
	if err != nil {
		t.Fatal(err)
	}

	resp.Header = map[string][]string{
		"Content-Type": {"application/json"},
	}

	responseBody = example{}
	option = WithResponse(&responseBody)
	wrapperResp = WrapperResponse{
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
	header := http.Header{}
	header.Add("Content-Type", "application/json")
	resp := WrapperResponse{
		Header:     header,
		StatusCode: http.StatusNotFound,
		Body:       bytes.NewBufferString(`{"title": "not found", "detail": "resource not found"}`).Bytes(),
	}

	var errResp ErrResponse
	err := WithErrorResponse(&errResp)(&resp)

	require.NotNil(t, err)
	require.Contains(t, errResp.Message(), "not found")
	require.Contains(t, err.Error(), "not found")

	resp.StatusCode = http.StatusOK
	err = WithErrorResponse(&errResp)(&resp)
	require.Nil(t, err)
}

func TestWrapper_WithRateLimitData(t *testing.T) {
	n := time.Now()
	resp := &WrapperResponse{
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

			cc := CacheConfig{
				logDebug:     true,
				cacheTTL:     int32(1000),
				cacheMaxSize: 1024,
			}
			ctx = context.WithValue(ctx, ContextKey{}, cc)
			client, err := NewBaseHttpClient(ctx, http.DefaultClient)
			assert.Nil(t, err)

			req, err := client.NewRequest(context.Background(), tc.method, u, tc.options...)
			require.Equal(t, tc.expected.err, err)
			require.Equal(t, tc.expected.method, req.Method)
			require.Equal(t, tc.expected.url, req.URL.String())
			require.Equal(t, tc.expected.headers, req.Header)
			require.Equal(t, tc.expected.body, req.Body)
		})
	}
}

func TestWrapperConfig(t *testing.T) {
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
		cc       CacheConfig
		expected expected
	}{
		{
			name:    "GET request with no options",
			method:  http.MethodGet,
			url:     "http://example.com",
			options: nil,
			cc: CacheConfig{
				logDebug:     true,
				cacheTTL:     int32(1000),
				cacheMaxSize: 1024,
			},
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
			cc: CacheConfig{
				logDebug:     true,
				cacheTTL:     int32(2000),
				cacheMaxSize: 0,
			},
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

			ctx = context.WithValue(ctx, ContextKey{}, tc.cc)
			client, err := NewBaseHttpClient(ctx, http.DefaultClient)
			require.NoError(t, err)

			req, err := client.NewRequest(context.Background(), tc.method, u, tc.options...)
			require.Equal(t, tc.expected.err, err)
			require.Equal(t, tc.expected.method, req.Method)
			require.Equal(t, tc.expected.url, req.URL.String())
			require.Equal(t, tc.expected.headers, req.Header)
			require.Equal(t, tc.expected.body, req.Body)
		})
	}
}
