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
	eviction := time.Duration(ttl) * time.Minute
	c, err := bigcache.New(ctx, bigcache.DefaultConfig(eviction))
	if err != nil {
		return GoCache{}, err
	}

	gc := GoCache{
		ttl:         eviction,
		rootLibrary: c,
	}

	return gc, nil
}

func GetCacheKey(req *http.Request) string {
	return req.URL.String()
}

func CopyResponse(resp *http.Response) *http.Response {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp
	}

	c := *resp
	// Replace resp with a no-op closer so nobody has to worry about closing the reader.
	c.Body = io.NopCloser(bytes.NewBuffer(respBody))

	return &c
}

func isOk(e error) bool {
	return e == nil
}

func (g *GoCache) Get(key string) (*http.Response, error) {
	entry, found := g.rootLibrary.Get(key)
	if isOk(found) {
		r := bufio.NewReader(bytes.NewReader(entry))
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			return resp, err
		}

		return resp, nil
	}

	return nil, nil
}

func (g *GoCache) Set(key string, value *http.Response) error {
	cacheableResponse, _ := httputil.DumpResponse(value, true)
	err := g.rootLibrary.Set(key, cacheableResponse)
	if err != nil {
		return err
	}

	return nil
}

func (g *GoCache) GetString(key string) string {
	entry, found := g.rootLibrary.Get(key)
	if found == nil {
		return string(entry)
	}

	return ""
}

func (c *GoCache) SetString(key string, value string) error {
	err := c.rootLibrary.Set(key, []byte(value))
	if err != nil {
		return err
	}

	return nil
}

func (g *GoCache) SetBytes(key string, value []byte) error {
	err := g.rootLibrary.Set(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (g *GoCache) Delete(key string) error {
	err := g.rootLibrary.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (g *GoCache) Clear() error {
	err := g.rootLibrary.Reset()
	if err != nil {
		return err
	}

	return nil
}

func (g *GoCache) Has(key string) bool {
	_, found := g.rootLibrary.Get(key)
	return found == nil
}
