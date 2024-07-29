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
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type GoCache struct {
	ttl         time.Duration
	rootLibrary *bigcache.BigCache
}

func NewGoCache(ctx context.Context, ttl int32, cacheMaxSize int, isLogLevel bool) (GoCache, error) {
	l := ctxzap.Extract(ctx)
	config := bigcache.Config{
		// number of shards (must be a power of 2)
		Shards: 1024,

		// time after which entry can be evicted
		LifeWindow: time.Duration(ttl) * time.Second,

		// Interval between removing expired entries (clean up).
		// If set to <= 0 then no action is performed.
		// Setting to < 1 second is counterproductive — bigcache has a one second resolution.
		CleanWindow: 5 * time.Minute,

		// rps * lifeWindow, used only in initial memory allocation
		MaxEntriesInWindow: 1000 * 10 * 60,

		// max entry size in bytes, used only in initial memory allocation
		MaxEntrySize: 500,

		// prints information about additional memory allocation
		Verbose: isLogLevel,

		// cache will not allocate more memory than this limit, value in MB
		// if value is reached then the oldest entries can be overridden for the new ones
		// 0 value means no size limit
		// Default value "GB eq 2048MB
		HardMaxCacheSize: cacheMaxSize, // BATON_CACHE_MAX_SIZE

		// callback fired when the oldest entry is removed because of its expiration time or no space left
		// for the new entry, or because delete was called. A bitmask representing the reason will be returned.
		// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
		OnRemove: nil,

		// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
		// for the new entry, or because delete was called. A constant representing the reason will be passed through.
		// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
		// Ignored if OnRemove is specified.
		OnRemoveWithReason: nil,
	}
	cache, initErr := bigcache.New(ctx, config)
	if initErr != nil {
		l.Error("in-memory cache error", zap.Any("NewGoCache", initErr))
		return GoCache{}, initErr
	}

	gc := GoCache{
		ttl:         config.LifeWindow,
		rootLibrary: cache,
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
