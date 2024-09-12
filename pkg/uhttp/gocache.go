package uhttp

import (
	"bufio"
	"bytes"
	"context"
	"net/http"
	"net/http/httputil"
	"time"

	bigCache "github.com/allegro/bigcache/v3"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type GoCache struct {
	rootLibrary *bigCache.BigCache
}

func NewGoCache(ctx context.Context, cfg CacheConfig) (GoCache, error) {
	l := ctxzap.Extract(ctx)
	if cfg.DisableCache {
		l.Debug("http cache disabled")
		return GoCache{}, nil
	}
	config := bigCache.DefaultConfig(time.Duration(cfg.CacheTTL) * time.Second)
	config.Verbose = cfg.LogDebug
	config.Shards = 4
	config.HardMaxCacheSize = cfg.CacheMaxSize // value in MB, 0 value means no size limit
	cache, err := bigCache.New(ctx, config)
	if err != nil {
		l.Error("http cache initialization error", zap.Error(err))
		return GoCache{}, err
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
	gc := GoCache{
		rootLibrary: cache,
	}

	return gc, nil
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
