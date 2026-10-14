package uotel

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

const (
	// errorLogWindow is how long an identical otel error is suppressed after
	// it has been logged.
	errorLogWindow = 5 * time.Minute
	// maxErrorKeys bounds the distinct error messages tracked; the oldest
	// is evicted when it is full.
	maxErrorKeys = 64
	// maxErrorKeyLen truncates error messages used as dedup keys.
	maxErrorKeyLen = 256
)

// dedupErrorHandler routes otel errors (failed exports, dropped data, ...)
// to zap at Warn, logging each distinct message at most once per window and
// reporting how many repeats were suppressed in the next line for it.
type dedupErrorHandler struct {
	// active counts initialized configs; with none, errors go to the
	// standard logger like otel's default handler.
	active atomic.Int32

	mtx     sync.Mutex
	now     func() time.Time
	window  time.Duration
	entries map[string]*errorEntry
}

type errorEntry struct {
	lastLogged time.Time
	suppressed int
}

func newDedupErrorHandler() *dedupErrorHandler {
	return &dedupErrorHandler{now: time.Now, window: errorLogWindow, entries: make(map[string]*errorEntry)}
}

var (
	errorHandler        = newDedupErrorHandler()
	errorHandlerInstall sync.Once
)

// installErrorHandler makes errorHandler the otel global error handler (once
// per process) and marks it active until the matching uninstallErrorHandler.
func installErrorHandler() {
	errorHandlerInstall.Do(func() { otel.SetErrorHandler(errorHandler) })
	errorHandler.active.Add(1)
}

func uninstallErrorHandler() {
	errorHandler.active.Add(-1)
}

func (h *dedupErrorHandler) Handle(err error) {
	if err == nil {
		return
	}
	if h.active.Load() <= 0 {
		log.Print(err)
		return
	}
	suppressed, ok := h.record(err.Error())
	if !ok {
		return
	}
	fields := []zap.Field{zap.Error(err)}
	if suppressed > 0 {
		fields = append(fields, zap.Int("suppressed_repeats", suppressed))
	}
	zap.L().Warn("otel: telemetry export error", fields...)
}

// record notes an occurrence of msg and reports whether it should be logged
// now, with the number of occurrences suppressed since it was last logged.
func (h *dedupErrorHandler) record(msg string) (int, bool) {
	if len(msg) > maxErrorKeyLen {
		msg = msg[:maxErrorKeyLen]
	}
	h.mtx.Lock()
	defer h.mtx.Unlock()
	now := h.now()
	e, ok := h.entries[msg]
	if ok && now.Sub(e.lastLogged) < h.window {
		e.suppressed++
		return 0, false
	}
	if !ok {
		if len(h.entries) >= maxErrorKeys {
			h.evictOldest()
		}
		e = &errorEntry{}
		h.entries[msg] = e
	}
	suppressed := e.suppressed
	e.lastLogged = now
	e.suppressed = 0
	return suppressed, true
}

// evictOldest drops the entry logged longest ago. precondition: h.mtx is locked.
func (h *dedupErrorHandler) evictOldest() {
	var oldestKey string
	var oldest time.Time
	first := true
	for k, e := range h.entries {
		if first || e.lastLogged.Before(oldest) {
			oldestKey, oldest, first = k, e.lastLogged, false
		}
	}
	delete(h.entries, oldestKey)
}
