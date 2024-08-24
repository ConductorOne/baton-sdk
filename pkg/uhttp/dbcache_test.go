package uhttp

import (
	"encoding/json"
	"net/http"
	"testing"

	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

func TestDBCacheGettersAndSetters(t *testing.T) {
	var (
		err error
		url = "https://jsonplaceholder.typicode.com/posts/1/comments"
	)
	fc, err := NewDBCache(ctx)
	require.Nil(t, err)

	d := &DBCache{
		db: fc.db,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.Nil(t, err)
	require.NotNil(t, req)

	cli := &http.Client{}
	resp, err := cli.Do(req)
	require.Nil(t, err)
	require.NotNil(t, resp)
	defer resp.Body.Close()

	cKey, err := GenerateCacheKey(resp.Request)
	require.Nil(t, err)

	err = fc.Set(ctx, cKey, resp)
	require.Nil(t, err)

	res, err := d.Get(ctx, cKey)
	require.Nil(t, err)
	require.NotNil(t, res)
	defer res.Body.Close()
}

func TestDBCache(t *testing.T) {
	var url = "https://jsonplaceholder.typicode.com/posts/1/comments"
	fc, err := NewDBCache(ctx)
	require.Nil(t, err)

	err = fc.Insert(ctx, "url", url)
	require.Nil(t, err)

	res, err := fc.Select(ctx, "url")
	require.Nil(t, err)
	require.NotNil(t, res)

	var val string
	err = json.Unmarshal(res, &val)
	require.Nil(t, err)
	require.Equal(t, val, url)
}
