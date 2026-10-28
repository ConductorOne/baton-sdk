package dotc1z

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

// decoderPuts counts decoders handed back to the pool.
//
// It exists because sync.Pool cannot answer "did that code return its codec?".
// A later Get is allowed to miss for reasons that have nothing to do with the
// code under test: a GC empties the pool, and under the race detector Put
// discards one item in four by design (sync/pool.go), so a test that concludes
// "not pooled" from a missed Get is wrong a quarter of the time. The counter
// increments before the Put reaches the pool, so it survives that drop.
var decoderPuts atomic.Int64

// poolDisabled is an operational kill-switch. Set BATON_ZSTD_POOL_DISABLE=1
// in the environment to bypass the decoder pool entirely and let callers
// fall back to fresh decoder creation. Zero behavior change when unset.
// Useful for rolling back pool behavior without a revert + re-release cycle
// if a latent Reset or Close bug surfaces in prod.
var poolDisabled = os.Getenv("BATON_ZSTD_POOL_DISABLE") == "1"

// decoderPool manages reusable zstd.Decoder instances to reduce allocation overhead.
// All pooled decoders are configured with concurrency=1 (single-threaded) and low memory mode.
var decoderPool = &sync.Pool{}

// getDecoder retrieves a zstd decoder from the pool or creates a new one.
// The returned decoder is NOT bound to any reader - call Reset(r) before use.
// Returns the decoder and a boolean indicating if it came from the pool.
func getDecoder() (*zstd.Decoder, bool) {
	if poolDisabled {
		return nil, false
	}
	if dec, ok := decoderPool.Get().(*zstd.Decoder); ok && dec != nil {
		return dec, true
	}

	// Create new decoder with default options matching decoder.go defaults.
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(defaultDecoderMaxMemory),
	)
	if err != nil {
		// Fallback: return nil and let caller create decoder with their options
		return nil, false
	}
	return dec, false
}

// putDecoder returns a zstd decoder to the pool for reuse.
// The decoder is reset to release any reference to the previous reader.
func putDecoder(dec *zstd.Decoder) {
	if dec == nil {
		return
	}
	if poolDisabled {
		return
	}
	// Reset to nil reader to release reference to previous input.
	// If Reset fails (bad state), don't return to pool.
	if err := dec.Reset(nil); err != nil {
		return
	}
	decoderPuts.Add(1)
	decoderPool.Put(dec)
}
