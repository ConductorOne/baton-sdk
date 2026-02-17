package uhttp

import (
	"net/http"
	"testing"
	"time"

	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

var urlTest = "https://jsonplaceholder.typicode.com/posts/1/comments"

func TestDBCacheGettersAndSetters(t *testing.T) {
	cli := &http.Client{}
	fc, err := getDBCacheForTesting()
	require.Nil(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlTest, nil)
	require.Nil(t, err)
	require.NotNil(t, req)

	//nolint:gosec // integration test intentionally performs an external HTTP request.
	resp, err := cli.Do(req)
	require.Nil(t, err)
	require.NotNil(t, resp)
	defer resp.Body.Close()

	var ic icache = &DBCache{
		db: fc.db,
	}
	cKey, err := CreateCacheKey(resp.Request)
	require.Nil(t, err)
	require.NotEmpty(t, cKey)

	err = ic.Set(req, resp)
	require.Nil(t, err)

	res, err := ic.Get(req)
	require.Nil(t, err)
	require.NotNil(t, res)
	require.Equal(t, resp.StatusCode, res.StatusCode)
	require.Equal(t, resp.ContentLength, res.ContentLength)
	require.EqualValues(t, resp.Header, res.Header)

	err = ic.Set(req, resp)
	require.Nil(t, err, "Setting same cache key again should not error")

	defer res.Body.Close()
}

func TestDBCache(t *testing.T) {
	fc, err := getDBCacheForTesting()
	require.Nil(t, err)

	data := []byte("Testing 123")

	err = fc.insert(ctx, "urlTest", data, "http://example.com")
	require.Nil(t, err)

	res, err := fc.pick(ctx, "urlTest")
	require.Nil(t, err)
	require.NotNil(t, res)

	require.Equal(t, data, res)
}

func getDBCacheForTesting() (*DBCache, error) {
	fc, err := NewDBCache(ctx, CacheConfig{
		TTL: 3600 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return fc, nil
}
