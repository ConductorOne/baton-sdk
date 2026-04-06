package ratelimit

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExtractRateLimitData_WeightedHeaders(t *testing.T) {
	n := time.Now()

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"X-Ratelimit-Limit":     {"500, 500;w=1, 20000;w=60"},
			"X-Ratelimit-Remaining": {"499, 499;w=1, 19999;w=60"},
			"X-Ratelimit-Reset":     {"30"},
		},
	}

	rl, err := ExtractRateLimitData(resp.StatusCode, &resp.Header)
	require.NoError(t, err)
	require.Equal(t, int64(500), rl.GetLimit())
	require.Equal(t, int64(499), rl.GetRemaining())
	require.Equal(t, n.Add(time.Second*30).Unix(), rl.GetResetAt().AsTime().Unix())

	resp = &http.Response{
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"X-Ratelimit-Limit":     {"100;w=60"},
			"X-Ratelimit-Remaining": {"42;w=60"},
			"X-Ratelimit-Reset":     {"30"},
		},
	}

	rl, err = ExtractRateLimitData(resp.StatusCode, &resp.Header)
	require.NoError(t, err)
	require.Equal(t, int64(100), rl.GetLimit())
	require.Equal(t, int64(42), rl.GetRemaining())
}

func TestHelpers_ExtractRateLimitData(t *testing.T) {
	n := time.Now()

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"X-Ratelimit-Limit":     {"100"},
			"X-Ratelimit-Remaining": {"50"},
			"X-Ratelimit-Reset":     {"30"},
		},
	}

	rl, err := ExtractRateLimitData(resp.StatusCode, &resp.Header)
	require.NoError(t, err)
	require.Equal(t, int64(100), rl.GetLimit())
	require.Equal(t, int64(50), rl.GetRemaining())
	require.Equal(t, n.Add(time.Second*30).Unix(), rl.GetResetAt().AsTime().Unix())

	resp = &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     map[string][]string{},
	}

	rl, err = ExtractRateLimitData(resp.StatusCode, &resp.Header)
	require.NoError(t, err)
	require.Equal(t, int64(1), rl.GetLimit())
	require.Equal(t, int64(0), rl.GetRemaining())
	require.Equal(t, n.Add(time.Second*60).Unix(), rl.GetResetAt().AsTime().Unix())
}
