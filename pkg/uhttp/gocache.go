package uhttp

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httputil"
	"time"

	bigcache "github.com/allegro/bigcache/v3"
)

type GoCache struct {
	ttl         time.Duration
	rootLibrary *bigcache.BigCache
}

func NewGoCache(ctx context.Context, ttl int32) (GoCache, error) {
	c, err := bigcache.New(ctx, bigcache.DefaultConfig(time.Duration(ttl)*time.Minute))
	if err != nil {
		return GoCache{}, err
	}

	gc := GoCache{
		ttl:         time.Duration(ttl) * time.Minute,
		rootLibrary: c,
	}

	return gc, nil
}

func GetCacheKey(req *http.Request) string {
	return req.URL.String()
}

func CopyResponse(resp *http.Response) *http.Response {
	c := *resp
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp
	}
	// Replace resp with a no-op closer so nobody has to worry about closing the reader.
	c.Body = io.NopCloser(bytes.NewBuffer(respBody))

	return &c
}

func (c GoCache) Get(key string) *http.Response {
	entry, found := c.rootLibrary.Get(key)
	if found == nil {
		r := bufio.NewReader(bytes.NewReader(entry))
		resp, _ := http.ReadResponse(r, nil)
		return resp
	}

	return nil
}

func (c GoCache) Set(key string, value *http.Response) error {
	cacheableResponse, _ := httputil.DumpResponse(value, true)
	err := c.rootLibrary.Set(key, cacheableResponse)
	if err != nil {
		return err
	}

	return nil
}

func (c GoCache) GetString(key string) string {
	entry, found := c.rootLibrary.Get(key)
	if found == nil {
		return string(entry)
	}

	return ""
}

func (c GoCache) SetString(key string, value string) error {
	err := c.rootLibrary.Set(key, []byte(value))
	if err != nil {
		return err
	}

	return nil
}

func (c GoCache) SetBytes(key string, value []byte) error {
	err := c.rootLibrary.Set(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (c GoCache) Delete(key string) error {
	err := c.rootLibrary.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (c GoCache) Clear() error {
	err := c.rootLibrary.Reset()
	if err != nil {
		return err
	}

	return nil
}

func (c GoCache) Has(key string) bool {
	_, found := c.rootLibrary.Get(key)
	return found == nil
}
