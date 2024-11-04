package uhttp

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"time"

	bigCache "github.com/allegro/bigcache/v3"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const (
	cacheTTLMaximum  = 31536000 // 31536000 seconds = one year
	cacheTTLDefault  = 3600     // 3600 seconds = one hour
	defaultCacheSize = 50       // MB
)

type CacheConfig struct {
	LogDebug     bool
	CacheTTL     int64 // If 0, cache is disabled
	CacheMaxSize int
}
type ContextKey struct{}

type GoCache struct {
	rootLibrary *bigCache.BigCache
}

type NoopCache struct{}

func NewNoopCache(ctx context.Context) *NoopCache {
	return &NoopCache{}
}

func (g *NoopCache) Get(req *http.Request) (*http.Response, error) {
	return nil, nil
}

func (n *NoopCache) Set(req *http.Request, value *http.Response) error {
	return nil
}

func (n *NoopCache) Clear(ctx context.Context) error {
	return nil
}

func (cc *CacheConfig) ToString() string {
	return fmt.Sprintf("CacheTTL: %d, CacheMaxSize: %d, LogDebug: %t", cc.CacheTTL, cc.CacheMaxSize, cc.LogDebug)
}

func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		CacheTTL:     cacheTTLDefault,
		CacheMaxSize: defaultCacheSize,
		LogDebug:     false,
	}
}

func NewCacheConfigFromEnv() *CacheConfig {
	config := DefaultCacheConfig()

	cacheMaxSize, err := strconv.ParseInt(os.Getenv("BATON_HTTP_CACHE_MAX_SIZE"), 10, 64)
	if err == nil {
		config.CacheMaxSize = int(cacheMaxSize)
	}

	// read the `BATON_HTTP_CACHE_TTL` environment variable and return
	// the value as a number of seconds between 0 and an arbitrary maximum. Note:
	// this means that passing a value of `-1` will set the TTL to zero rather than
	// infinity.
	cacheTTL, err := strconv.ParseInt(os.Getenv("BATON_HTTP_CACHE_TTL"), 10, 64)
	if err == nil {
		config.CacheTTL = min(cacheTTLMaximum, max(0, cacheTTL))
	}

	return &config
}

func NewCacheConfigFromCtx(ctx context.Context) (*CacheConfig, error) {
	defaultConfig := DefaultCacheConfig()
	if v := ctx.Value(ContextKey{}); v != nil {
		ctxConfig, ok := v.(CacheConfig)
		if !ok {
			return nil, fmt.Errorf("error casting config values from context")
		}
		return &ctxConfig, nil
	}
	return &defaultConfig, nil
}

func NewHttpCache(ctx context.Context, config *CacheConfig) (icache, error) {
	l := ctxzap.Extract(ctx)

	var cache icache = &NoopCache{}

	if config == nil {
		config = NewCacheConfigFromEnv()
	}

	if config.CacheTTL <= 0 {
		l.Debug("CacheTTL is <=0, disabling cache.", zap.Int64("CacheTTL", config.CacheTTL))
		return cache, nil
	}

	disableCache, err := strconv.ParseBool(os.Getenv("BATON_DISABLE_HTTP_CACHE"))
	if err != nil {
		disableCache = false
	}
	if disableCache {
		l.Debug("BATON_DISABLE_HTTP_CACHE set, disabling cache.")
		return cache, nil
	}

	cacheBackend := os.Getenv("BATON_HTTP_CACHE_BACKEND")
	if cacheBackend == "" {
		l.Debug("defaulting to db-cache")
		cacheBackend = "db"
	}

	switch cacheBackend {
	case "memory":
		l.Debug("Using in-memory cache")
		memCache, err := NewGoCache(ctx, *config)
		if err != nil {
			l.Error("error creating http cache (in-memory)", zap.Error(err))
			return nil, err
		}
		cache = memCache
	case "db":
		l.Debug("Using db cache")
		dbCache, err := NewDBCache(ctx, *config)
		if err != nil {
			l.Error("error creating http cache (db-cache)", zap.Error(err))
			return nil, err
		}
		cache = dbCache
	}

	return cache, nil
}

func NewGoCache(ctx context.Context, cfg CacheConfig) (*GoCache, error) {
	l := ctxzap.Extract(ctx)
	gc := GoCache{}
	config := bigCache.DefaultConfig(time.Duration(cfg.CacheTTL) * time.Second)
	config.Verbose = cfg.LogDebug
	config.Shards = 4
	config.HardMaxCacheSize = cfg.CacheMaxSize // value in MB, 0 value means no size limit
	cache, err := bigCache.New(ctx, config)
	if err != nil {
		l.Error("http cache initialization error", zap.Error(err))
		return nil, err
	}

	l.Debug("http cache config",
		zap.Dict("config",
			zap.Int("Shards", config.Shards),
			zap.Duration("LifeWindow", config.LifeWindow),
			zap.Duration("CleanWindow", config.CleanWindow),
			zap.Int("MaxEntriesInWindow", config.MaxEntriesInWindow),
			zap.Int("MaxEntrySize", config.MaxEntrySize),
			zap.Bool("StatsEnabled", config.StatsEnabled),
			zap.Bool("Verbose", config.Verbose),
			zap.Int("HardMaxCacheSize", config.HardMaxCacheSize),
		))
	gc.rootLibrary = cache

	return &gc, nil
}

func (g *GoCache) Statistics() bigCache.Stats {
	if g.rootLibrary == nil {
		return bigCache.Stats{}
	}

	return g.rootLibrary.Stats()
}

func (g *GoCache) Get(req *http.Request) (*http.Response, error) {
	if g.rootLibrary == nil {
		return nil, nil
	}

	key, err := CreateCacheKey(req)
	if err != nil {
		return nil, err
	}

	entry, err := g.rootLibrary.Get(key)
	if err == nil {
		r := bufio.NewReader(bytes.NewReader(entry))
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			return nil, err
		}

		return resp, nil
	}

	return nil, nil
}

func (g *GoCache) Set(req *http.Request, value *http.Response) error {
	if g.rootLibrary == nil {
		return nil
	}

	key, err := CreateCacheKey(req)
	if err != nil {
		return err
	}

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
	if g.rootLibrary == nil {
		return nil
	}

	err := g.rootLibrary.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (g *GoCache) Clear(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	if g.rootLibrary == nil {
		l.Debug("clear: rootLibrary is nil")
		return nil
	}

	err := g.rootLibrary.Reset()
	if err != nil {
		return err
	}
	err = g.rootLibrary.ResetStats()
	if err != nil {
		return err
	}

	l.Debug("reset cache")
	return nil
}

func (g *GoCache) Has(key string) bool {
	if g.rootLibrary == nil {
		return false
	}
	_, found := g.rootLibrary.Get(key)
	return found == nil
}
