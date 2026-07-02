package pebble

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// SDKPebbleFormat is the on-disk format version this SDK release
// writes. Bumping it is a deliberate SDK compatibility decision, not
// a transitive consequence of a go.mod upgrade. The reader
// compares against pebble.FormatNewest at open and refuses files
// newer than what the binary supports.
const SDKPebbleFormat = pebble.FormatNewest

// Durability controls how aggressively the engine fsyncs writes. The
// default for production is DurabilitySync; the fresh-sync fast path
// (which uses pebble.NoSync for in-flight grant batches) falls under
// DurabilityNoSync because the sync workflow can replay from the
// connector if the host crashes before checkpoint.
type Durability int

const (
	DurabilityDefault Durability = iota // == DurabilitySync
	DurabilitySync                      // fsync per batch commit
	DurabilityNoSync                    // buffered by the OS; faster, weaker guarantees
)

// Options configures an Engine. Construct via the WithXxx functional
// options passed to Open.
type Options struct {
	sharedCache *pebble.Cache
	durability  Durability

	// slowQueryThreshold logs iterators that live longer than this.
	// Default 5 s; matches the SQLite engine's behavior.
	slowQueryThreshold time.Duration

	// readOnly opens the engine without write permission. Save is
	// disallowed in this mode.
	readOnly bool
}

// Option is a functional option passed to Open.
type Option func(*Options)

// WithSharedCache reuses a single *pebble.Cache across multiple
// engine instances. Mandatory for any caller that opens >1 engine in
// the same process (e.g. C1's per-app ReaderCache); without it the
// per-engine cache cost compounds linearly.
//
// Cache lifecycle: the caller owns the cache and is responsible for
// calling cache.Unref() when no engines using it remain. If
// WithSharedCache is not used, the engine mints its own cache and
// Unrefs it in Engine.Close.
func WithSharedCache(c *pebble.Cache) Option { return func(o *Options) { o.sharedCache = c } }

// WithDurability selects the fsync policy for writes. Default is
// DurabilitySync.
func WithDurability(d Durability) Option { return func(o *Options) { o.durability = d } }

// WithReadOnly opens the engine in read-only mode. Save is disallowed.
func WithReadOnly(readOnly bool) Option { return func(o *Options) { o.readOnly = readOnly } }

// WithSlowQueryThreshold overrides the default 5 s threshold for
// slow-iterator logging.
func WithSlowQueryThreshold(d time.Duration) Option {
	return func(o *Options) { o.slowQueryThreshold = d }
}

// newPebbleOptions builds the *pebble.Options for the Engine. The
// returned struct is consumed once at pebble.Open; the caller does
// not retain a reference.
func newPebbleOptions(o *Options) *pebble.Options {
	opts := &pebble.Options{
		FormatMajorVersion:          SDKPebbleFormat,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		MaxOpenFiles:                1024,

		// L0 tuning matches the RFC v4 Appendix C.6 values.
		// L0CompactionThreshold raised 2→8 (autoresearch P1.2b):
		// L0=2 made the compactor wake on every L0 add, stealing CPU
		// from the write path during bulk syncs. Letting ~8 L0 files
		// accumulate frees that CPU at the cost of a slightly deeper
		// L0 read fan-out (still well under L0StopWritesThreshold).
		L0CompactionThreshold:     8,
		L0CompactionFileThreshold: 500,
		L0StopWritesThreshold:     20,
		FlushSplitBytes:           2 << 20,
		LBaseMaxBytes:             256 << 20,

		CompactionConcurrencyRange: func() (int, int) {
			upper := runtime.GOMAXPROCS(0) / 4
			if upper < 2 {
				upper = 2
			}
			if upper > 8 {
				upper = 8
			}
			return 2, upper
		},

		DisableWAL: false,
		Comparer:   pebble.DefaultComparer,
		ReadOnly:   o.readOnly,
		Logger:     discardPebbleLogger{},
	}
	// Pausable variant of pebble's default ConcurrencyLimitScheduler so the
	// engine can suppress automatic compactions during the EndSync-to-close
	// window, where their output never survives to the saved artifact (see
	// pausableCompactionScheduler).
	opts.Experimental.CompactionScheduler = newPausableCompactionScheduler()
	// L0 BlockSize tuning lands per-level below; bloom-filter wiring can
	// be added after benchmark data justifies it.
	opts.Levels[0].BlockSize = 32 << 10

	if o.sharedCache == nil {
		opts.Cache = pebble.NewCache(256 << 20)
	} else {
		opts.Cache = o.sharedCache
	}

	return opts
}

// writeOpts returns the pebble.WriteOptions for normal writes per
// the engine's Durability setting.
func writeOpts(d Durability) *pebble.WriteOptions {
	if d == DurabilityNoSync {
		return pebble.NoSync
	}
	return pebble.Sync
}

func defaultOptions() *Options {
	return &Options{
		durability:         DurabilitySync,
		slowQueryThreshold: 5 * time.Second,
	}
}

// discardPebbleLogger silences Pebble's WAL discovery / compaction
// chatter on Infof, surfaces Errorf to stderr (compaction errors and
// WAL recovery warnings are operationally significant — silencing them
// would hide real problems), and terminates on Fatalf via os.Exit(1)
// to match Pebble's default-logger semantics. We deliberately do NOT
// panic on Fatalf: a recover() in a gRPC interceptor or HTTP framework
// could otherwise swallow it and let the program continue on a
// potentially-corrupted storage engine.
type discardPebbleLogger struct{}

func (discardPebbleLogger) Infof(format string, args ...interface{}) {}
func (discardPebbleLogger) Errorf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "pebble: "+format+"\n", args...)
}

func (discardPebbleLogger) Fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "pebble FATAL: "+format+"\n", args...)
	os.Exit(1)
}
