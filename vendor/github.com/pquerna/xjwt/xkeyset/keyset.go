package xkeyset

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"fmt"

	jose "github.com/go-jose/go-jose/v4"

	"github.com/pquerna/cachecontrol"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/context/ctxhttp"
)

const (
	xjwtOpenTracingTag = "com.scaleft.xjwt"
)

type Options struct {
	UserAgent        string
	URL              string
	Client           *http.Client
	MinCacheDuration time.Duration
	MaxCacheDuration time.Duration
	RefreshWarning   func(err error)
	// RefreshContext is used during background refreshing of the Keyset. If unset, context.Background() is used.
	RefreshContext context.Context
}

// New creates a JSON Keyset that is cached in memory.
//
// On creation, it will do a block HTTP request to load the initial keyset.
//
// After initial load, it will refresh the cached keyset, any consumers may see
// older versions of the cache until the refresh is complete.
//
// New emits opentracing spans on the supplied context when fetching the keyset.
func New(initCtx context.Context, opts Options) (*RemoteKeyset, error) {
	rctx := opts.RefreshContext
	if rctx == nil {
		rctx = context.Background()
	}
	rctx, cfunc := context.WithCancel(rctx)
	rk := &RemoteKeyset{
		ctx:    rctx,
		cfunc:  cfunc,
		opts:   opts,
		client: opts.Client,
		now:    time.Now,
	}

	if rk.client == nil {
		rk.client = &http.Client{
			Timeout: time.Second * 30,
		}
	}

	if rk.opts.MinCacheDuration == 0 {
		rk.opts.MinCacheDuration = time.Minute * 5
	}

	if rk.opts.MaxCacheDuration == 0 {
		rk.opts.MaxCacheDuration = time.Hour * 12
	}

	if rk.opts.RefreshWarning == nil {
		rk.opts.RefreshWarning = func(err error) {}
	}
	if span := trace.SpanFromContext(initCtx); span.SpanContext().IsValid() {
		rk.tracerProvider = span.TracerProvider()
	} else {
		rk.tracerProvider = otel.GetTracerProvider()
	}

	rk.tracer = rk.tracerProvider.Tracer(
		xjwtOpenTracingTag,
	)
	err := rk.init(initCtx)
	if err != nil {
		return nil, err
	}
	return rk, nil
}

type RemoteKeyset struct {
	opts           Options
	tracerProvider trace.TracerProvider
	tracer         trace.Tracer

	ctx   context.Context
	cfunc context.CancelFunc

	client *http.Client
	now    func() time.Time

	current atomic.Value
}

func (rk *RemoteKeyset) init(ctx context.Context) error {
	rk.current.Store(&jose.JSONWebKeySet{})
	rv, refresh, err := rk.fetchKeyset(ctx)
	if err != nil {
		return err
	}
	rk.current.Store(rv)

	go rk.refreshKeySet(refresh)

	return nil
}

func (rk *RemoteKeyset) refreshKeySet(refresh time.Duration) {
	select {
	case <-rk.ctx.Done():
		return
	case <-time.After(refresh):
		rv, d, err := rk.fetchKeyset(rk.ctx)
		if err != nil {
			rk.opts.RefreshWarning(err)
			go rk.refreshKeySet(refresh)
			return
		}
		rk.current.Store(rv)
		go rk.refreshKeySet(d)
		return
	}
}

func (rk *RemoteKeyset) fetchKeyset(ctx context.Context) (*jose.JSONWebKeySet, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	req, err := http.NewRequest("GET", rk.opts.URL, nil)
	if err != nil {
		return nil, time.Duration(0), err
	}

	req.Header.Set("Accept", "application/json")

	if rk.opts.UserAgent != "" {
		req.Header.Set("User-Agent", rk.opts.UserAgent)
	} else {
		req.Header.Set("User-Agent", "xjwt-keyset.go/0.1.0")
	}

	ctx, spn := rk.tracer.Start(
		ctx,
		"xjwt.keyset.fetch",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.HTTPRequestMethodKey.String(req.Method),
			semconv.URLFull(req.URL.String()),
		),
	)
	defer spn.End()

	resp, err := ctxhttp.Do(ctx, rk.client, req)
	if err != nil {
		spn.RecordError(err)
		return nil, time.Duration(0), err
	}
	spn.SetAttributes(semconv.HTTPResponseStatusCode(resp.StatusCode))
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		keys, exp, err := rk.parseResponse(req, resp)
		if err != nil {
			spn.RecordError(err)
			return nil, time.Duration(0), err
		}
		return keys, exp, nil
	}

	return nil, time.Duration(0), fmt.Errorf("xjwt.keyset: Fetch returned HTTP Status Code '%d' for '%s'", resp.StatusCode, rk.opts.URL)
}

func (rk *RemoteKeyset) parseResponse(req *http.Request, resp *http.Response) (*jose.JSONWebKeySet, time.Duration, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, time.Duration(0), fmt.Errorf("xjwt.keyset: Error reading response '%s': %v", rk.opts.URL, err)
	}

	rv := &jose.JSONWebKeySet{}

	err = json.Unmarshal(data, rv)
	if err != nil {
		return nil, time.Duration(0), fmt.Errorf("xjwt.keyset: Error parsing response '%s': %v", rk.opts.URL, err)
	}

	_, cacheExpires, err := cachecontrol.CachableResponse(req, resp, cachecontrol.Options{})
	if err != nil {
		return nil, time.Duration(0), fmt.Errorf("xjwt.keyset: Error parsing cache control header '%s': %v", rk.opts.URL, err)
	}

	n := rk.now()

	exp := cacheExpires.Sub(n)
	if exp > rk.opts.MaxCacheDuration {
		return rv, rk.opts.MaxCacheDuration, nil
	} else if exp < rk.opts.MinCacheDuration {
		return rv, rk.opts.MinCacheDuration, nil
	}

	return rv, exp, nil
}

func (rk *RemoteKeyset) Get() (*jose.JSONWebKeySet, error) {
	return rk.current.Load().(*jose.JSONWebKeySet), nil
}

func (rk *RemoteKeyset) Close() {
	rk.cfunc()
}
