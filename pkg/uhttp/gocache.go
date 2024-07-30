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
	rootLibrary *bigcache.BigCache
}

func NewGoCache(ctx context.Context, ttl int32, cacheMaxSize int, isLogLevel bool) (GoCache, error) {
	l := ctxzap.Extract(ctx)
	config := bigcache.DefaultConfig(time.Duration(ttl) * time.Second)
	// prints information about additional memory allocation
	config.Verbose = isLogLevel
	// number of shards (must be a power of 2)
	config.Shards = 4
	// cache will not allocate more memory than this limit, value in MB
	// 0 value means no size limit
	config.HardMaxCacheSize = cacheMaxSize
	cache, initErr := bigcache.New(ctx, config)
	if initErr != nil {
		l.Error("in-memory cache error", zap.Any("NewGoCache", initErr))
		return GoCache{}, initErr
	}

	l.Debug("in-memory cache config", zap.Any("config", config))
	gc := GoCache{
		rootLibrary: cache,
	}

	return gc, nil
}

func (g *GoCache) Statistics() bigcache.Stats {
	return g.rootLibrary.Stats()
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
