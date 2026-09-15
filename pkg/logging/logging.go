package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	LogFormatJSON    = "json"
	LogFormatConsole = "console"
)

// Option configures the zap.Config used to build the logger. Its signature is
// intentionally kept as func(*zap.Config) so downstream connectors that define
// custom options keep compiling. Optional file rotation is passed separately
// via InitWithRotation / InitWithCoreAndRotation rather than as an Option, to
// avoid changing this exported type.
type Option func(*zap.Config)

// RotationConfig bounds the growth of file entries in OutputPaths. The zero
// value (MaxSizeMB <= 0) disables rotation entirely, which is the default and
// leaves logging behaving exactly as it did before rotation existed. It is a
// struct rather than loose ints so the two sizes can't be transposed at a call
// site.
type RotationConfig struct {
	// MaxSizeMB is the size in whole MB a log file may reach before it is
	// rotated. <= 0 disables rotation.
	MaxSizeMB int
	// MaxBackups is how many rotated files to retain. 0 keeps none; negative
	// values behave the same but the CLI rejects them (log-max-backups >= 0).
	MaxBackups int
	// ErrSink receives rotation diagnostics (failed rename, failed prune).
	// These cannot go through zap - rotation runs inside a zap write - so they
	// default to os.Stderr, which on a Windows service goes nowhere; callers
	// that have a real sink (the Windows event log) should pass it here.
	// ErrSink need not itself be safe for concurrent use - the rotator
	// serializes its own writes to it.
	ErrSink io.Writer
}

// ErrPreviousRotatorClose reports that a *previous* logger's rotating file
// writer failed to close. The logger returned alongside it is fully live, so
// callers on a startup path must not treat this as fatal.
var ErrPreviousRotatorClose = errors.New("failed to close previous log rotators")

var (
	activeLevelMu sync.RWMutex
	activeLevel   *zap.AtomicLevel

	// activeRotators tracks the rotating file writer per canonical log path, so
	// a re-Init (e.g. a Windows service re-initializing its logger) reuses the
	// existing writer for a path it still logs to and closes the ones it no
	// longer does. One writer per file is the invariant that matters: two live
	// rotators rename and prune each other's backups, and closing the old one
	// instead would silence any logger still holding it - which on the Windows
	// service path is the logger the connector actually uses.
	activeRotatorsMu sync.Mutex
	activeRotators   = map[string]*rotatingWriter{}

	// activeRotatorPaths mirrors activeRotators' keys for prune() to read
	// lock-free: prune runs under a rotatingWriter's own mu, and taking
	// activeRotatorsMu there would invert the activeRotatorsMu -> mu order. It
	// is briefly a superset rather than an exact mirror while adoptRotators is
	// adopting new paths - see publishCandidateRotatorPaths - which is safe
	// because prune() only ever consults it to decide what to spare.
	activeRotatorPaths atomic.Pointer[map[string]struct{}]
)

// publishActiveRotatorPaths refreshes the lock-free snapshot prune() consults
// so it can avoid deleting another rotator's active file. Callers must hold
// activeRotatorsMu.
func publishActiveRotatorPaths() {
	paths := make(map[string]struct{}, len(activeRotators))
	for key := range activeRotators {
		paths[key] = struct{}{}
	}
	activeRotatorPaths.Store(&paths)
}

// publishCandidateRotatorPaths extends the published snapshot with paths an
// in-progress adoptRotators is about to open, so prune() never observes one
// of them on disk without also seeing it protected here. Being a superset of
// the current registry is intentional and safe: buildLogger republishes the
// exact set with publishActiveRotatorPaths once adoption finishes (whether or
// not it succeeded), so this is only ever wider than accurate for the
// duration of one adoptRotators call. Callers must hold activeRotatorsMu.
func publishCandidateRotatorPaths(candidates []string) {
	paths := make(map[string]struct{}, len(activeRotators)+len(candidates))
	for key := range activeRotators {
		paths[key] = struct{}{}
	}
	for _, p := range candidates {
		paths[canonicalOutputPath(p)] = struct{}{}
	}
	activeRotatorPaths.Store(&paths)
}

// isActiveRotatorPath reports whether path is the active file of some
// rotator currently registered in this process, under its canonical key.
func isActiveRotatorPath(path string) bool {
	paths := activeRotatorPaths.Load()
	if paths == nil {
		return false
	}
	_, ok := (*paths)[canonicalOutputPath(path)]
	return ok
}

func WithLogLevel(level string) Option {
	return func(c *zap.Config) {
		ll, err := ParseLogLevel(level)
		if err != nil {
			return
		}
		c.Level.SetLevel(ll)
	}
}

func ParseLogLevel(level string) (zapcore.Level, error) {
	level = strings.TrimSpace(level)
	if level == "" {
		level = "info"
	}
	var parsed zapcore.Level
	if err := parsed.Set(level); err != nil {
		return zapcore.InfoLevel, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	return parsed, nil
}

func NormalizeLogLevel(level string) (string, error) {
	parsed, err := ParseLogLevel(level)
	if err != nil {
		return "", err
	}
	return parsed.String(), nil
}

// SetLogLevel sets the log level for the active logger.
// Currently only used by lambda connectors.
func SetLogLevel(level string) error {
	parsed, err := ParseLogLevel(level)
	if err != nil {
		return err
	}

	activeLevelMu.RLock()
	levelHandle := activeLevel
	activeLevelMu.RUnlock()
	if levelHandle == nil {
		return nil
	}
	levelHandle.SetLevel(parsed)
	return nil
}

func WithLogFormat(format string) Option {
	return func(c *zap.Config) {
		switch format {
		case LogFormatJSON:
			c.Encoding = LogFormatJSON
		case LogFormatConsole:
			c.Encoding = LogFormatConsole
			c.EncoderConfig = zap.NewDevelopmentEncoderConfig()
		default:
			c.Encoding = LogFormatJSON
		}
	}
}

func WithOutputPaths(paths []string) Option {
	return func(c *zap.Config) {
		c.OutputPaths = dedupeOutputPaths(paths)
	}
}

var (
	startupCwdOnce sync.Once
	startupCwd     string
)

// startupWorkingDir caches the process's working directory as of the first
// call, so a relative --log-path keys the same way across an os.Chdir between
// two Inits instead of re-resolving against a live (and now different) cwd.
func startupWorkingDir() string {
	startupCwdOnce.Do(func() {
		startupCwd, _ = os.Getwd()
	})
	return startupCwd
}

// canonicalOutputPath is the identity used to decide that two OutputPaths
// entries name the same sink. --log-path is a hand-written string slice, so
// "/var/log/./baton.log", "/var/log//baton.log" and (on Windows) "C:/logs" vs
// "C:\Logs" are all spellings operators actually produce.
//
// It approximates "same file", it does not decide it: symlinks are resolved
// only when the path already exists, and Windows 8.3 short names are not
// expanded. Two spellings that slip past it get two rotators on one file, so
// the registry below is a strong default rather than a guarantee.
func canonicalOutputPath(p string) string {
	if p == "stdout" || p == "stderr" {
		return p
	}
	canonical := p
	if !filepath.IsAbs(canonical) {
		if wd := startupWorkingDir(); wd != "" {
			canonical = filepath.Join(wd, canonical)
		}
	}
	if abs, err := filepath.Abs(canonical); err == nil {
		canonical = abs
	} else {
		canonical = filepath.Clean(canonical)
	}
	// Best effort: EvalSymlinks fails when the log file doesn't exist yet, which
	// is the normal first-run case. adoptRotators registers a rotator only after
	// its file exists, so lookups on later Inits resolve to the same key.
	if resolved, evalErr := filepath.EvalSymlinks(canonical); evalErr == nil {
		canonical = resolved
	}
	if runtime.GOOS == "windows" {
		canonical = strings.ToLower(canonical)
	}
	return canonical
}

// dedupeOutputPaths drops entries naming the same sink, keeping the first-seen
// spelling and order. A repeated path used to only double-log each line; with
// rotation it would also give one file two rotators that rename and prune each
// other's backups.
func dedupeOutputPaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	deduped := make([]string, 0, len(paths))
	for _, p := range paths {
		key := canonicalOutputPath(p)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, p)
	}
	return deduped
}

// WithInitialFields allows the logger to be configured with static fields at creation time.
// This is useful for setting fields that are constant across all log messages.
func WithInitialFields(fields map[string]interface{}) Option {
	return func(c *zap.Config) {
		c.InitialFields = fields
	}
}

// buildConfig returns the zap configuration used by the Init functions.
func buildConfig(opts ...Option) zap.Config {
	zc := zap.NewProductionConfig()
	zc.Sampling = nil
	zc.DisableStacktrace = true

	for _, opt := range opts {
		opt(&zc)
	}

	return zc
}

// encoderFromConfig returns the zapcore.Encoder matching zc's configured
// Encoding, so extra cores (rotation, event log, ...) format entries
// identically to the base logger.
func encoderFromConfig(zc zap.Config) zapcore.Encoder {
	if zc.Encoding == LogFormatConsole {
		return zapcore.NewConsoleEncoder(zc.EncoderConfig)
	}
	return zapcore.NewJSONEncoder(zc.EncoderConfig)
}

// initialFieldsCore applies zc.InitialFields to core the same way zap's own
// Config.Build does (vendor/go.uber.org/zap/config.go): sorted keys, zap.Any
// per entry. rotateCore is built directly from zc rather than via zc.Build(),
// so without this it silently drops every initial field the base core has.
func initialFieldsCore(core zapcore.Core, fields map[string]interface{}) zapcore.Core {
	if len(fields) == 0 {
		return core
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fs := make([]zap.Field, 0, len(fields))
	for _, k := range keys {
		fs = append(fs, zap.Any(k, fields[k]))
	}
	return core.With(fs)
}

// hasSinkScheme reports whether p names a zap sink by URL rather than a plain
// filesystem path (zap resolves "file://" and any scheme registered via
// RegisterSink). Rotating those would MkdirAll the raw string and create a
// literal "file:" directory, so they are left to zap. A single-character
// scheme is a Windows drive letter, not a URL.
func hasSinkScheme(p string) bool {
	u, err := url.Parse(p)
	return err == nil && len(u.Scheme) > 1
}

// splitOutputPaths separates OutputPaths entries into the ones zap should keep
// handling itself (its built-in stdout/stderr sinks) and real file paths that
// the caller wants rotated instead. Duplicates are dropped so no path ever gets
// two rotators.
func splitOutputPaths(paths []string) ([]string, []string) {
	var kept, files []string
	for _, p := range dedupeOutputPaths(paths) {
		if p == "stdout" || p == "stderr" || hasSinkScheme(p) {
			kept = append(kept, p)
		} else {
			files = append(files, p)
		}
	}
	return kept, files
}

// initFromConfig builds the base logger, downgrading a previous-rotator close
// failure to a log line. See buildLogger for the rest.
func initFromConfig(ctx context.Context, zc zap.Config, rotation RotationConfig) (context.Context, *zap.Logger, error) {
	ctx, l, err := buildLogger(ctx, zc, rotation)
	// Deviating from the review ask to surface this error to callers: returning
	// it aborts Windows service startup over a leaked descriptor. It is surfaced
	// loudly here and stays matchable via ErrPreviousRotatorClose. The nil check
	// is belt and braces - buildLogger documents that l is live for this error.
	if errors.Is(err, ErrPreviousRotatorClose) && l != nil {
		l.Error("Logger initialized, but a previous log rotator failed to close.", zap.Error(err))
		return ctx, l, nil
	}
	return ctx, l, err
}

// buildLogger builds the base logger. When rotation.MaxSizeMB > 0, real file
// paths in OutputPaths are pulled out and served by a rotating core instead of
// zap's plain file sink; otherwise OutputPaths handling is left exactly as zap's
// default (no rotation, no behavior change). The returned context and logger are
// usable even when the error is ErrPreviousRotatorClose: a late failure to close
// a retired rotator does not invalidate the new logger.
func buildLogger(ctx context.Context, zc zap.Config, rotation RotationConfig) (context.Context, *zap.Logger, error) {
	// Rotation off means the registry is not touched at all: a plain Init must
	// not close rotators some other, rotating logger is still writing to.
	rotationOn := rotation.MaxSizeMB > 0

	var filePaths []string
	if rotationOn {
		kept, files := splitOutputPaths(zc.OutputPaths)
		if len(files) > 0 {
			zc.OutputPaths = kept
			filePaths = files
		}
	}

	l, err := zc.Build()
	if err != nil {
		return nil, nil, err
	}

	var retired []*rotatingWriter
	if rotationOn {
		// Adopt and unregister under a single hold of the registry lock. Dropping
		// it between the two lets a concurrent Init retire the rotator this one
		// just adopted, after which the next Init opens a second rotator on a file
		// the first logger is still writing to. Blocking I/O (MkdirAll/OpenFile)
		// happens under the lock as a result; nothing else takes it, and Init is
		// not on a hot path.
		activeRotatorsMu.Lock()
		rotators, redundant, adoptErr := adoptRotators(filePaths, rotation)
		if adoptErr == nil {
			retired = unregisterUnusedRotators(rotators)
			// redundant holds writers adoptRotators created but didn't register
			// (see its doc comment); close them the same way as retired ones,
			// after the new logger is installed.
			retired = append(retired, redundant...)
		}
		// Republished unconditionally, success or failure: adoptRotators may have
		// published a wider, candidate-inclusive snapshot (see
		// publishCandidateRotatorPaths) before hitting adoptErr, and the registry
		// itself is untouched on failure, so this always just restates the truth.
		publishActiveRotatorPaths()
		activeRotatorsMu.Unlock()
		if adoptErr != nil {
			return nil, nil, adoptErr
		}

		for _, rw := range rotators {
			rotateCore := zapcore.NewCore(encoderFromConfig(zc), zapcore.AddSync(rw), zc.Level)
			rotateCore = initialFieldsCore(rotateCore, zc.InitialFields)
			l = l.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewTee(core, rotateCore)
			}))
		}
	}

	activeLevelMu.Lock()
	activeLevel = &zc.Level
	activeLevelMu.Unlock()

	zap.ReplaceGlobals(l)

	// Close retired rotators only after the new logger has replaced the global
	// one, so no still-installed logger can write to a closed handle.
	closeErr := closeRotators(retired)

	l.Debug("Logger created!", zap.String("log_level", zc.Level.String()))
	ctx = ctxzap.ToContext(ctx, l)

	if closeErr != nil {
		return ctx, l, fmt.Errorf("%w: %w", ErrPreviousRotatorClose, closeErr)
	}
	return ctx, l, nil
}

// adoptRotators returns one rotating writer per path, reusing the writer an
// earlier Init already opened for that file. Reuse rather than replacement is
// what keeps a re-Init from leaving two rotators on one file, or from closing
// the writer a still-running logger holds. Callers must hold activeRotatorsMu.
//
// The second return value holds writers this call created but isn't
// registering, because the post-creation key (recomputed below, once the file
// exists) turned out to collide with one already registered - the caller must
// close them; adoptRotators only holds activeRotatorsMu, and closing is
// blocking I/O.
func adoptRotators(paths []string, rotation RotationConfig) ([]*rotatingWriter, []*rotatingWriter, error) {
	// Wrapped once, here, and reused for every path below: adoptRotators hands
	// one RotationConfig.ErrSink to every rotator it adopts in this call, so
	// they must all share the one lock wrapErrSink attaches, not a lock each
	// (see rotatingWriter.errSink's doc comment).
	errSink := wrapErrSink(rotation.ErrSink)

	// Publish the paths this call intends to adopt before opening any of
	// them: prune() reads this snapshot lock-free (see activeRotatorPaths) and
	// runs fully concurrently with this function. Without this, a file opened
	// below can exist on disk - and be mistaken by a sibling rotator's prune()
	// for an unowned backup - before the registration loop further down adds
	// it to activeRotators. A superset of the eventual registry is fine here:
	// it only ever makes prune() more conservative.
	publishCandidateRotatorPaths(paths)

	rotators := make([]*rotatingWriter, 0, len(paths))
	var created []*rotatingWriter
	for _, p := range paths {
		if rw, ok := activeRotators[canonicalOutputPath(p)]; ok {
			rotators = append(rotators, rw)
			continue
		}

		rw, err := newRotatingWriter(p, rotation.MaxSizeMB, rotation.MaxBackups, errSink)
		if err != nil {
			errs := []error{fmt.Errorf("failed to create rotating log writer for %q: %w", p, err)}
			for _, c := range created {
				if closeErr := c.Close(); closeErr != nil {
					errs = append(errs, closeErr)
				}
			}
			return nil, nil, errors.Join(errs...)
		}
		created = append(created, rw)
		rotators = append(rotators, rw)
	}

	// Registered and reconfigured only once every path succeeded, so a rejected
	// config never changes the live settings of a rotator that survives it. The
	// key is recomputed here rather than reused from the lookup above because the
	// file now exists, which is what lets canonicalOutputPath resolve symlinks.
	var redundant []*rotatingWriter
	for i, rw := range rotators {
		key := canonicalOutputPath(rw.path)
		if existing, ok := activeRotators[key]; ok && existing != rw {
			// Adopt the survivor instead of overwriting its entry: that would
			// drop it from the map unclosed and unreachable, while a logger
			// still holding it keeps writing through the now-orphaned handle.
			redundant = append(redundant, rw)
			existing.reconfigure(rotation.MaxSizeMB, rotation.MaxBackups, errSink)
			rotators[i] = existing
			continue
		}
		rw.reconfigure(rotation.MaxSizeMB, rotation.MaxBackups, errSink)
		activeRotators[key] = rw
	}
	// Two input paths can collide onto one rotator (the lookup hit above, or the
	// adopt-the-survivor branch just above), leaving the same *rotatingWriter at
	// two indices. buildLogger tees one core per slice entry, so an undeduped
	// repeat would write every line to that file twice.
	return dedupeRotators(rotators), redundant, nil
}

// dedupeRotators drops repeated entries by pointer identity, keeping the
// first occurrence's position. See adoptRotators for why a repeat can occur.
func dedupeRotators(rotators []*rotatingWriter) []*rotatingWriter {
	seen := make(map[*rotatingWriter]struct{}, len(rotators))
	deduped := rotators[:0]
	for _, rw := range rotators {
		if _, ok := seen[rw]; ok {
			continue
		}
		seen[rw] = struct{}{}
		deduped = append(deduped, rw)
	}
	return deduped
}

// unregisterUnusedRotators removes the rotators the new logger does not use from
// the registry and returns them for the caller to close, so a dropped log path
// doesn't leak its descriptor. Closing is left to the caller: it is blocking I/O
// (fsync) and must happen after the new logger is installed. Callers must hold
// activeRotatorsMu.
func unregisterUnusedRotators(inUse []*rotatingWriter) []*rotatingWriter {
	keep := make(map[*rotatingWriter]struct{}, len(inUse))
	for _, rw := range inUse {
		keep[rw] = struct{}{}
	}

	var retired []*rotatingWriter
	for key, rw := range activeRotators {
		if _, ok := keep[rw]; ok {
			continue
		}
		delete(activeRotators, key)
		retired = append(retired, rw)
	}
	return retired
}

// closeRotators closes retired rotators, joining any failures.
func closeRotators(retired []*rotatingWriter) error {
	var errs []error
	for _, rw := range retired {
		if err := rw.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Init creates a new zap logger and attaches it to the provided context.
func Init(ctx context.Context, opts ...Option) (context.Context, error) {
	ctx, _, err := initFromConfig(ctx, buildConfig(opts...), RotationConfig{})
	return ctx, err
}

// InitWithRotation is like Init but also enables size-based rotation of any
// plain file paths in OutputPaths. A zero RotationConfig disables rotation
// entirely, behaving exactly like Init.
func InitWithRotation(ctx context.Context, rotation RotationConfig, opts ...Option) (context.Context, error) {
	ctx, _, err := initFromConfig(ctx, buildConfig(opts...), rotation)
	return ctx, err
}

// InitWithCore creates a new zap logger and tees an additional core onto it.
//
// Deprecated: use InitWithCoreAndRotation. Kept at its original signature
// because it has shipped unchanged since v0.17.0 and nothing in CI catches a Go
// API break (buf-breaking covers proto only).
func InitWithCore(ctx context.Context, buildCore func(zap.Config) zapcore.Core, opts ...Option) (context.Context, error) {
	return InitWithCoreAndRotation(ctx, buildCore, RotationConfig{}, opts...)
}

// InitWithCoreAndRotation creates a new zap logger, tees an additional core onto
// it, and applies the same optional file rotation as InitWithRotation. buildCore
// receives the logger config so the extra core uses the same level filtering
// and encoder settings as Init.
func InitWithCoreAndRotation(ctx context.Context, buildCore func(zap.Config) zapcore.Core, rotation RotationConfig, opts ...Option) (context.Context, error) {
	zc := buildConfig(opts...)
	ctx, l, err := initFromConfig(ctx, zc, rotation)
	if err != nil {
		return nil, err
	}

	l = l.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewTee(core, buildCore(zc))
	}))
	zap.ReplaceGlobals(l)

	return ctxzap.ToContext(ctx, l), nil
}
