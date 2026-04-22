package dotc1z

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoderPool(t *testing.T) {
	withIsolatedPools(t)
	t.Run("get returns valid encoder", func(t *testing.T) {
		enc, fromPool := getEncoder()
		require.NotNil(t, enc)
		// First call won't be from pool (pool is empty)
		require.False(t, fromPool)

		// Return to pool and get again
		putEncoder(enc)

		enc2, fromPool2 := getEncoder()
		require.NotNil(t, enc2)
		require.True(t, fromPool2)
		putEncoder(enc2)
	})

	t.Run("pooled encoder produces correct output", func(t *testing.T) {
		testData := []byte("test data for compression with pooled encoder")

		// Get encoder from pool
		enc, _ := getEncoder()
		require.NotNil(t, enc)

		var buf bytes.Buffer
		enc.Reset(&buf)

		_, err := enc.Write(testData)
		require.NoError(t, err)

		err = enc.Close()
		require.NoError(t, err)

		putEncoder(enc)

		// Verify we can decompress
		dec, err := zstd.NewReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		defer dec.Close()

		decoded, err := io.ReadAll(dec)
		require.NoError(t, err)
		require.Equal(t, testData, decoded)
	})

	t.Run("concurrent pool access", func(t *testing.T) {
		const numGoroutines = 10
		const iterations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					enc, _ := getEncoder()
					if !assert.NotNil(t, enc) {
						return
					}

					var buf bytes.Buffer
					enc.Reset(&buf)

					data := []byte("concurrent test data")
					_, err := enc.Write(data)
					if !assert.NoError(t, err) {
						return
					}
					if !assert.NoError(t, enc.Close()) {
						return
					}

					putEncoder(enc)
				}
			}(i)
		}

		wg.Wait()
	})
}

func TestDecoderPool(t *testing.T) {
	withIsolatedPools(t)
	// Create some test compressed data
	createCompressedData := func(t *testing.T, data []byte) []byte {
		t.Helper()
		var buf bytes.Buffer
		enc, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatalf("failed to create zstd writer: %v", err)
		}
		n, err := enc.Write(data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
		if n != len(data) {
			t.Fatalf("short write: wrote %d of %d bytes", n, len(data))
		}
		if err := enc.Close(); err != nil {
			t.Fatalf("failed to close encoder: %v", err)
		}
		return buf.Bytes()
	}

	t.Run("get returns valid decoder", func(t *testing.T) {
		dec, fromPool := getDecoder()
		require.NotNil(t, dec)
		require.False(t, fromPool) // First call, pool is empty

		putDecoder(dec)

		dec2, fromPool2 := getDecoder()
		require.NotNil(t, dec2)
		require.True(t, fromPool2)
		putDecoder(dec2)
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

// withIsolatedPools swaps the package-global encoder/decoder pools with fresh
// sync.Pool values for the duration of the test. Needed because sync.Pool can
// evict entries via GC between Gets, and because concurrent tests in the same
// package could otherwise race with exact-membership assertions.
//
// Also disables GC during the test. sync.Pool drops items when the runtime
// runs a GC cycle between Put and Get, which makes exact-membership
// assertions (e.g. "did putEncoder put one in the pool?") flaky under CI
// memory pressure. Restoring the prior GOGC on teardown keeps the rest of
// the package suite unaffected.
func withIsolatedPools(t *testing.T) {
	t.Helper()
	origEnc := encoderPool
	origDec := decoderPool
	encoderPool = &sync.Pool{}
	decoderPool = &sync.Pool{}
	origGCPercent := debug.SetGCPercent(-1)
	t.Cleanup(func() {
		debug.SetGCPercent(origGCPercent)
		encoderPool = origEnc
		decoderPool = origDec
	})
}

// TestPoolDisabled verifies the BATON_ZSTD_POOL_DISABLE kill-switch:
// getEncoder and getDecoder return (nil, false) so callers fall through to
// fresh creation, and putEncoder/putDecoder do not populate the pool.
func TestPoolDisabled(t *testing.T) {
	withIsolatedPools(t)

	orig := poolDisabled
	poolDisabled = true
	t.Cleanup(func() { poolDisabled = orig })

	enc, fromPool := getEncoder()
	require.Nil(t, enc, "getEncoder should return nil when pool is disabled")
	require.False(t, fromPool)

	// Create an encoder the caller's way and verify putEncoder doesn't pool it.
	freshEnc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(pooledEncoderConcurrency))
	require.NoError(t, err)
	require.NoError(t, freshEnc.Close())
	putEncoder(freshEnc) // should be a no-op when disabled

	// Decoder: same shape.
	dec, fromPool := getDecoder()
	require.Nil(t, dec, "getDecoder should return nil when pool is disabled")
	require.False(t, fromPool)

	freshDec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	putDecoder(freshDec) // should be a no-op when disabled
	freshDec.Close()

	// Now flip the switch back and confirm the pools are still empty —
	// putEncoder/putDecoder must not have added anything while disabled.
	poolDisabled = false
	enc2, fromPool2 := getEncoder()
	require.NotNil(t, enc2)
	require.False(t, fromPool2, "pool should be empty: put* were no-ops while disabled")
	_ = enc2.Close()

	dec2, fromPool2 := getDecoder()
	require.NotNil(t, dec2)
	require.False(t, fromPool2, "pool should be empty: put* were no-ops while disabled")
	dec2.Close()
}

// TestPoolGrowsFromSaveC1z verifies that saveC1z populates the encoder pool
// even when starting with an empty pool. This was a bug where only encoders
// that came FROM the pool were returned TO the pool.
func TestPoolGrowsFromSaveC1z(t *testing.T) {
	withIsolatedPools(t)

	// Pool is freshly empty (see withIsolatedPools).
	enc, fromPool := getEncoder()
	require.False(t, fromPool, "isolated pool should be empty")
	_ = enc.Close() // Drop it; don't return to pool.

	// Now use saveC1z which should populate the pool.
	tmpDir := t.TempDir()
	testData := bytes.Repeat([]byte("test data "), 100)

	dbFile := filepath.Join(tmpDir, "test.db")
	err := os.WriteFile(dbFile, testData, 0600)
	require.NoError(t, err)

	c1zFile := filepath.Join(tmpDir, "test.c1z")
	err = saveC1z(dbFile, c1zFile, pooledEncoderConcurrency)
	require.NoError(t, err)

	// Now the pool should have an encoder.
	enc2, fromPool2 := getEncoder()
	require.True(t, fromPool2, "saveC1z should have returned encoder to pool")
	putEncoder(enc2)
}

// TestPoolGrowsFromDecoder verifies that NewDecoder populates the decoder pool
// even when starting with an empty pool.
func TestPoolGrowsFromDecoder(t *testing.T) {
	withIsolatedPools(t)

	dec, fromPool := getDecoder()
	require.False(t, fromPool, "isolated pool should be empty")
	dec.Close()

	// Create a c1z file to decode.
	tmpDir := t.TempDir()
	testData := bytes.Repeat([]byte("test data "), 100)

	dbFile := filepath.Join(tmpDir, "test.db")
	err := os.WriteFile(dbFile, testData, 0600)
	require.NoError(t, err)

	c1zFile := filepath.Join(tmpDir, "test.c1z")
	err = saveC1z(dbFile, c1zFile, pooledEncoderConcurrency)
	require.NoError(t, err)

	// Drain encoder pool (saveC1z added one) so we're only asserting on the decoder pool below.
	enc, _ := getEncoder()
	_ = enc.Close()

	// Now use NewDecoder which should populate the decoder pool.
	f, err := os.Open(c1zFile)
	require.NoError(t, err)

	decoder, err := NewDecoder(f)
	require.NoError(t, err)

	_, err = io.ReadAll(decoder)
	require.NoError(t, err)

	err = decoder.Close()
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// Now the decoder pool should have a decoder.
	dec2, fromPool2 := getDecoder()
	require.True(t, fromPool2, "NewDecoder.Close should have returned decoder to pool")
	putDecoder(dec2)
}

func TestPooledRoundTrip(t *testing.T) {
	t.Run("encode decode round trip with pooled codecs", func(t *testing.T) {
		tmpDir := t.TempDir()
		testData := bytes.Repeat([]byte("test data for round trip "), 1000)

		// Write test db file
		dbFile := filepath.Join(tmpDir, "test.db")
		err := os.WriteFile(dbFile, testData, 0600)
		require.NoError(t, err)

		// Save using pooled encoder.
		c1zFile := filepath.Join(tmpDir, "test.c1z")
		err = saveC1z(dbFile, c1zFile, pooledEncoderConcurrency)
		require.NoError(t, err)

		// Load using pooled decoder
		f, err := os.Open(c1zFile)
		require.NoError(t, err)
		defer f.Close()

		decoder, err := NewDecoder(f)
		require.NoError(t, err)
		defer decoder.Close()

		decoded, err := io.ReadAll(decoder)
		require.NoError(t, err)
		require.Equal(t, testData, decoded)
	})

	t.Run("multiple round trips reuse pool", func(t *testing.T) {
		tmpDir := t.TempDir()

		for i := 0; i < 10; i++ {
			testData := bytes.Repeat([]byte("iteration data "), 100*(i+1))

			dbFile := filepath.Join(tmpDir, "test.db")
			err := os.WriteFile(dbFile, testData, 0600)
			require.NoError(t, err)

			c1zFile := filepath.Join(tmpDir, "test.c1z")
			err = saveC1z(dbFile, c1zFile, pooledEncoderConcurrency)
			require.NoError(t, err)

			f, err := os.Open(c1zFile)
			require.NoError(t, err)

			decoder, err := NewDecoder(f)
			require.NoError(t, err)

			decoded, err := io.ReadAll(decoder)
			require.NoError(t, err)
			require.Equal(t, testData, decoded)

			err = decoder.Close()
			require.NoError(t, err)
			err = f.Close()
			require.NoError(t, err)
		}
	})
}

// BenchmarkEncoderPoolAllocs measures allocations with and without pooling.
// Run with: go test -bench=BenchmarkEncoderPoolAllocs -benchmem.
func BenchmarkEncoderPoolAllocs(b *testing.B) {
	testData := bytes.Repeat([]byte("benchmark data "), 1000)
	tmpDir := b.TempDir()

	dbFile := filepath.Join(tmpDir, "bench.db")
	err := os.WriteFile(dbFile, testData, 0600)
	require.NoError(b, err)

	b.Run("pooled_encoder", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c1zFile := filepath.Join(tmpDir, "bench.c1z")
			err := saveC1z(dbFile, c1zFile, pooledEncoderConcurrency)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("new_encoder_each_time", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c1zFile := filepath.Join(tmpDir, "bench2.c1z")

			dbF, err := os.Open(dbFile)
			if err != nil {
				b.Fatal(err)
			}
			outF, err := os.Create(c1zFile)
			if err != nil {
				dbF.Close()
				b.Fatal(err)
			}

			if _, err := outF.Write(C1ZFileHeader); err != nil {
				outF.Close()
				dbF.Close()
				b.Fatal(err)
			}

			// Apples-to-apples with pooled_encoder above (concurrency=1).
			enc, err := zstd.NewWriter(outF, zstd.WithEncoderConcurrency(pooledEncoderConcurrency))
			if err != nil {
				outF.Close()
				dbF.Close()
				b.Fatal(err)
			}
			if _, err := io.Copy(enc, dbF); err != nil {
				enc.Close()
				outF.Close()
				dbF.Close()
				b.Fatal(err)
			}
			if err := enc.Flush(); err != nil {
				enc.Close()
				outF.Close()
				dbF.Close()
				b.Fatal(err)
			}
			enc.Close()

			if err := outF.Sync(); err != nil {
				outF.Close()
				dbF.Close()
				b.Fatal(err)
			}
			if err := outF.Close(); err != nil {
				dbF.Close()
				b.Fatal(err)
			}
			if err := dbF.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkEncoderAllocationOnly isolates encoder allocation overhead.
// This shows the direct benefit of pooling without file I/O noise.
func BenchmarkEncoderAllocationOnly(b *testing.B) {
	testData := []byte("small test data for encoder benchmark")

	b.Run("pooled", func(b *testing.B) {
		// Warm up the pool
		enc, _ := getEncoder()
		putEncoder(enc)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			enc, _ := getEncoder()
			if enc == nil {
				b.Fatal("getEncoder returned nil")
			}
			var buf bytes.Buffer
			enc.Reset(&buf)
			if _, err := enc.Write(testData); err != nil {
				b.Fatal(err)
			}
			enc.Close()
			putEncoder(enc)
		}
	})

	b.Run("new_each_time", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			enc, err := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(pooledEncoderConcurrency))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := enc.Write(testData); err != nil {
				b.Fatal(err)
			}
			enc.Close()
		}
	})
}

// BenchmarkDecoderPoolAllocs measures decoder allocations.
func BenchmarkDecoderPoolAllocs(b *testing.B) {
	// Create test c1z file
	tmpDir := b.TempDir()
	testData := bytes.Repeat([]byte("benchmark data "), 1000)

	dbFile := filepath.Join(tmpDir, "bench.db")
	err := os.WriteFile(dbFile, testData, 0600)
	require.NoError(b, err)

	c1zFile := filepath.Join(tmpDir, "bench.c1z")
	err = saveC1z(dbFile, c1zFile, pooledEncoderConcurrency)
	require.NoError(b, err)

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
