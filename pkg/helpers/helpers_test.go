package helpers

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHelpers_SplitFullName(t *testing.T) {
	firstName, lastName := SplitFullName("Prince")
	require.Equal(t, "Prince", firstName)
	require.Equal(t, "", lastName)

	firstName, lastName = SplitFullName("John Smith")
	require.Equal(t, "John", firstName)
	require.Equal(t, "Smith", lastName)

	firstName, lastName = SplitFullName("John Jacob Jingleheimer Schmidt")
	require.Equal(t, "John", firstName)
	require.Equal(t, "Jacob Jingleheimer Schmidt", lastName)
}

func TestHelpers_ExtractRateLimitData(t *testing.T) {
	n := time.Now()

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"X-Ratelimit-Limit":     {"100"},
			"X-Ratelimit-Remaining": {"50"},
			"X-Ratelimit-Reset":     {"60"},
		},
	}

	rl, err := ExtractRateLimitData(resp.StatusCode, &resp.Header)
	require.NoError(t, err)
	require.Equal(t, int64(100), rl.Limit)
	require.Equal(t, int64(50), rl.Remaining)
	require.Equal(t, n.Add(time.Second*60).Unix(), rl.ResetAt.AsTime().Unix())

	resp = &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     map[string][]string{},
	}

	rl, err = ExtractRateLimitData(resp.StatusCode, &resp.Header)
	require.NoError(t, err)
	require.Equal(t, int64(1), rl.Limit)
	require.Equal(t, int64(0), rl.Remaining)
	require.Equal(t, n.Add(time.Second*60).Unix(), rl.ResetAt.AsTime().Unix())
}

func TestHelpers_IsJSONContentType_Success(t *testing.T) {
	resp := &http.Response{
		Header: map[string][]string{
			"Content-Type": {"application/vdn+json"},
		},
	}
	h := resp.Header.Get("Content-Type")
	require.True(t, IsJSONContentType(h))
}

func TestHelpers_IsJSONContentType_Failure(t *testing.T) {
	resp := &http.Response{
		Header: map[string][]string{
			"Content-Type": {"application/xml"},
		},
	}
	h := resp.Header.Get("Content-Type")
	require.False(t, IsJSONContentType(h))
}
