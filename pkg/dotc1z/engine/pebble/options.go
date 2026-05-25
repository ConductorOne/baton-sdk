package pebble

import (
	"fmt"
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

// Preset selects per-deployment defaults for engine tuning. Set once
// at engine-open via WithPreset; never changes for the engine's
// lifetime.
type Preset int

const (
	// PresetBackendInfra is the production default: 256 MiB block cache,
	// per-level compression presets tuned for write-once-read-many sync
	// payloads, MaxConcurrentCompactions ≤ 4.
	PresetBackendInfra Preset = iota

	// PresetBackendInfraHot is for hosts that run many engines in the
	// same process (e.g. C1's per-app ReaderCache). 4 GiB block cache;
	// otherwise identical to PresetBackendInfra. Use WithSharedCache to
	// share one cache across engines.
	PresetBackendInfraHot

	// PresetCLI is for one-shot tooling. 32 MiB cache, low compaction
	// concurrency. Optimized for fast open/close rather than steady-
	// state throughput.
	PresetCLI
)

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
	preset      Preset
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

// WithPreset picks one of PresetBackendInfra (default),
// PresetBackendInfraHot, or PresetCLI.
func WithPreset(p Preset) Option { return func(o *Options) { o.preset = p } }

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
		L0CompactionThreshold:     2,
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
	// L0 BlockSize tuning lands per-level below; bloom-filter wiring can
	// be added after benchmark data justifies it.
	opts.Levels[0].BlockSize = 32 << 10

	// Cache: shared or minted per preset.
	if o.sharedCache != nil {
		opts.Cache = o.sharedCache
	} else {
		switch o.preset {
		case PresetBackendInfraHot:
			opts.Cache = pebble.NewCache(4 << 30)
		case PresetCLI:
			opts.Cache = pebble.NewCache(32 << 20)
		default:
			opts.Cache = pebble.NewCache(256 << 20)
		}
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
		preset:             PresetBackendInfra,
		durability:         DurabilitySync,
		slowQueryThreshold: 5 * time.Second,
	}
}

type discardPebbleLogger struct{}

func (discardPebbleLogger) Infof(format string, args ...interface{})  {}
func (discardPebbleLogger) Errorf(format string, args ...interface{}) {}
func (discardPebbleLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}
