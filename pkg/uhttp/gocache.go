package uhttp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httputil"
	"sort"
	"strings"
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

	l.Debug("in-memory cache config", zap.Any("config", config))
	gc := GoCache{
		ttl:         config.LifeWindow,
		rootLibrary: cache,
	}

	return gc, nil
}

func CreateCacheKey(req *http.Request) (string, error) {
	// Normalize the URL path
	path := strings.ToLower(req.URL.Path)

	// Combine the path with sorted query parameters
	queryParams := req.URL.Query()
	var sortedParams []string
	for k, v := range queryParams {
		for _, value := range v {
			sortedParams = append(sortedParams, fmt.Sprintf("%s=%s", k, value))
		}
	}
	sort.Strings(sortedParams)
	queryString := strings.Join(sortedParams, "&")

	// Include relevant headers in the cache key
	var headerParts []string
	for key, values := range req.Header {
		for _, value := range values {
			headerParts = append(headerParts, fmt.Sprintf("%s=%s", key, value))
		}
	}
	sort.Strings(headerParts)
	headersString := strings.Join(headerParts, "&")

	// Create a unique string for the cache key
	cacheString := fmt.Sprintf("%s?%s&headers=%s", path, queryString, headersString)

	// Hash the cache string to create a key
	hash := sha256.New()
	_, err := hash.Write([]byte(cacheString))
	if err != nil {
		return "", err
	}

	cacheKey := fmt.Sprintf("%x", hash.Sum(nil))
	return cacheKey, nil
}

func (g *GoCache) Get(key string) (*http.Response, error) {
	entry, err := g.rootLibrary.Get(key)
	if err == nil {
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
	cacheableResponse, err := httputil.DumpResponse(value, true)
	if err != nil {
		return err
	}

	err = g.rootLibrary.Set(key, cacheableResponse)
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
