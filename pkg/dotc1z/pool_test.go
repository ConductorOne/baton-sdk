package dotc1z

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeV1C1Z writes data as a v1 c1z (C1ZFileHeader followed by one zstd
// frame) so the v1 decoder has something to read.
func writeV1C1Z(tb testing.TB, path string, data []byte) {
	tb.Helper()
	var buf bytes.Buffer
	buf.Write(C1ZFileHeader)
	enc, err := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
	require.NoError(tb, err)
	_, err = enc.Write(data)
	require.NoError(tb, err)
	require.NoError(tb, enc.Close())
	require.NoError(tb, os.WriteFile(path, buf.Bytes(), 0o600))
}

func TestDecoderPool(t *testing.T) {
	withIsolatedPools(t)
	// Create some test compressed data
	createCompressedData := func(t *testing.T, data []byte) []byte {
		t.Helper()
		var buf bytes.Buffer
		enc, err := zstd.NewWriter(&buf)
		require.NoError(t, err, "failed to create zstd writer")
		n, err := enc.Write(data)
		require.NoError(t, err, "failed to write data")
		require.Equal(t, len(data), n, "short write")
		require.NoError(t, enc.Close(), "failed to close encoder")
		return buf.Bytes()
	}

	t.Run("get returns valid decoder", func(t *testing.T) {
		dec, fromPool := getDecoder()
		require.NotNil(t, dec)
		require.False(t, fromPool) // First call, pool is empty

		// Retry, and do not simplify this to a single put/get: under the race
		// detector sync.Pool.Put drops one item in four on the floor on
		// purpose (`runtime_randn(4) == 0` in sync/pool.go), so a lone
		// assertion fails a quarter of the time in the nightly race shard.
		// withIsolatedPools cannot help — a fresh pool, a disabled GC, and one
		// P still leave the drop, which was measured at ~25% of puts. A real
		// regression, where nothing is ever pooled, still fails every attempt.
		var fromPool2 bool
		for attempt := 0; attempt < 100 && !fromPool2; attempt++ {
			putDecoder(dec)
			dec, fromPool2 = getDecoder()
			require.NotNil(t, dec)
		}
		require.True(t, fromPool2, "getDecoder never reported a decoder from the pool")
		putDecoder(dec)
	})

	t.Run("pooled decoder produces correct output", func(t *testing.T) {
		testData := []byte("test data for decompression with pooled decoder")
		compressed := createCompressedData(t, testData)

		dec, _ := getDecoder()
		require.NotNil(t, dec)

		err := dec.Reset(bytes.NewReader(compressed))
		require.NoError(t, err)

		decoded, err := io.ReadAll(dec)
		require.NoError(t, err)
		require.Equal(t, testData, decoded)

		putDecoder(dec)
	})

	t.Run("concurrent decoder pool access", func(t *testing.T) {
		testData := []byte("concurrent decoder test data")
		compressed := createCompressedData(t, testData)

		const numGoroutines = 10
		const iterations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					dec, _ := getDecoder()
					if !assert.NotNil(t, dec) {
						return
					}

					err := dec.Reset(bytes.NewReader(compressed))
					if !assert.NoError(t, err) {
						return
					}

					decoded, err := io.ReadAll(dec)
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, testData, decoded) {
						return
					}

					putDecoder(dec)
				}
			}()
		}

		wg.Wait()
	})
}

// withIsolatedPools swaps the package-global decoder pool with a fresh
// sync.Pool for the duration of the test. Needed because sync.Pool can
// evict entries via GC between Gets, and because concurrent tests in the same
// package could otherwise race with exact-membership assertions.
//
// Also disables GC during the test. sync.Pool drops items when the runtime
// runs a GC cycle between Put and Get, which makes exact-membership
// assertions (e.g. "did putDecoder put one in the pool?") flaky under CI
// memory pressure. Restoring the prior GOGC on teardown keeps the rest of
// the package suite unaffected.
//
// Also pins GOMAXPROCS to 1. sync.Pool.Put parks the item in the current
// P's private slot, and Get can steal only from other Ps' shared queues —
// another P's private slot is unreachable. If the goroutine migrates Ps
// between Put and Get (routine after blocking file syscalls — Sync, Close,
// Rename — especially on slow Windows CI filesystems), the item is parked
// where Get cannot see it and exact-membership assertions fail. One P means
// one slot; the stdlib's own sync.Pool tests use the same trick.
//
// None of this makes Put→Get deterministic under the race detector, and no
// helper can: that build of sync.Pool.Put throws one item in four away
// deliberately. A test that needs to observe a pooled item has to retry the
// Get; a test that only needs to know the code returned its codec should count
// the Put instead (see decoderPuts).
func withIsolatedPools(t *testing.T) {
	t.Helper()
	origDec := decoderPool
	decoderPool = &sync.Pool{}
	origGCPercent := debug.SetGCPercent(-1)
	origProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(origProcs)
		debug.SetGCPercent(origGCPercent)
		decoderPool = origDec
	})
}

// TestPoolDisabled verifies the BATON_ZSTD_POOL_DISABLE kill-switch:
// getDecoder returns (nil, false) so callers fall through to fresh
// creation, and putDecoder does not populate the pool.
func TestPoolDisabled(t *testing.T) {
	withIsolatedPools(t)

	orig := poolDisabled
	poolDisabled = true
	t.Cleanup(func() { poolDisabled = orig })

	dec, fromPool := getDecoder()
	require.Nil(t, dec, "getDecoder should return nil when pool is disabled")
	require.False(t, fromPool)

	freshDec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	putDecoder(freshDec) // should be a no-op when disabled
	freshDec.Close()

	// Now flip the switch back and confirm the pool is still empty —
	// putDecoder must not have added anything while disabled.
	poolDisabled = false
	dec2, fromPool2 := getDecoder()
	require.NotNil(t, dec2)
	require.False(t, fromPool2, "pool should be empty: putDecoder was a no-op while disabled")
	dec2.Close()
}

// TestPoolGrowsFromDecoder verifies that NewDecoder populates the decoder pool
// even when starting with an empty pool.
func TestPoolGrowsFromDecoder(t *testing.T) {
	withIsolatedPools(t)

	dec, fromPool := getDecoder()
	require.False(t, fromPool, "isolated pool should be empty")
	dec.Close()

	c1zFile := filepath.Join(t.TempDir(), "test.c1z")
	writeV1C1Z(t, c1zFile, bytes.Repeat([]byte("test data "), 100))

	// Now use NewDecoder which should populate the decoder pool.
	f, err := os.Open(c1zFile)
	require.NoError(t, err)

	putsBefore := decoderPuts.Load()
	decoder, err := NewDecoder(f)
	require.NoError(t, err)

	_, err = io.ReadAll(decoder)
	require.NoError(t, err)

	err = decoder.Close()
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// Count the put rather than reading the pool through a get. This is the
	// assertion that was failing the nightly race shard when it read the pool
	// through a get: the detector's one-in-four drop, not a decoder that went
	// unpooled.
	require.Equal(t, putsBefore+1, decoderPuts.Load(), "NewDecoder.Close should have returned exactly its own decoder to the pool")
}

// BenchmarkDecoderPoolAllocs measures decoder allocations.
func BenchmarkDecoderPoolAllocs(b *testing.B) {
	c1zFile := filepath.Join(b.TempDir(), "bench.c1z")
	writeV1C1Z(b, c1zFile, bytes.Repeat([]byte("benchmark data "), 1000))

	b.Run("pooled_decoder", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, err := os.Open(c1zFile)
			if err != nil {
				b.Fatal(err)
			}
			dec, err := NewDecoder(f)
			if err != nil {
				f.Close()
				b.Fatal(err)
			}
			if _, err := io.ReadAll(dec); err != nil {
				dec.Close()
				f.Close()
				b.Fatal(err)
			}
			if err := dec.Close(); err != nil {
				f.Close()
				b.Fatal(err)
			}
			if err := f.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("new_decoder_each_time", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, err := os.Open(c1zFile)
			if err != nil {
				b.Fatal(err)
			}

			// Skip header manually
			headerBuf := make([]byte, len(C1ZFileHeader))
			if _, err := f.Read(headerBuf); err != nil {
				f.Close()
				b.Fatal(err)
			}

			// Create new decoder each time (simulates old behavior)
			dec, err := zstd.NewReader(f,
				zstd.WithDecoderConcurrency(1),
				zstd.WithDecoderLowmem(true),
				zstd.WithDecoderMaxMemory(defaultDecoderMaxMemory),
			)
			if err != nil {
				f.Close()
				b.Fatal(err)
			}
			if _, err := io.ReadAll(dec); err != nil {
				dec.Close()
				f.Close()
				b.Fatal(err)
			}
			dec.Close()
			if err := f.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
