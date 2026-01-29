package dotc1z

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestEncoderPool(t *testing.T) {
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
					require.NotNil(t, enc)

					var buf bytes.Buffer
					enc.Reset(&buf)

					data := []byte("concurrent test data")
					_, err := enc.Write(data)
					require.NoError(t, err)
					require.NoError(t, enc.Close())

					putEncoder(enc)
				}
			}(i)
		}

		wg.Wait()
	})
}

func TestDecoderPool(t *testing.T) {
	// Create some test compressed data
	createCompressedData := func(data []byte) []byte {
		var buf bytes.Buffer
		enc, _ := zstd.NewWriter(&buf)
		_, _ = enc.Write(data)
		_ = enc.Close()
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
		compressed := createCompressedData(testData)

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
		compressed := createCompressedData(testData)

		const numGoroutines = 10
		const iterations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					dec, _ := getDecoder()
					require.NotNil(t, dec)

					err := dec.Reset(bytes.NewReader(compressed))
					require.NoError(t, err)

					decoded, err := io.ReadAll(dec)
					require.NoError(t, err)
					require.Equal(t, testData, decoded)

					putDecoder(dec)
				}
			}()
		}

		wg.Wait()
	})
}

// TestPoolGrowsFromSaveC1z verifies that saveC1z populates the encoder pool
// even when starting with an empty pool. This was a bug where only encoders
// that came FROM the pool were returned TO the pool.
func TestPoolGrowsFromSaveC1z(t *testing.T) {
	// Clear any existing pool state by getting and not returning
	for {
		enc, fromPool := getEncoder()
		if !fromPool {
			// This was a fresh encoder, pool is now empty
			// Don't return it - let it be GC'd
			_ = enc.Close()
			break
		}
		_ = enc.Close() // Don't return to pool
	}

	// Verify pool is empty
	enc, fromPool := getEncoder()
	require.False(t, fromPool, "pool should be empty after draining")
	_ = enc.Close() // Don't return

	// Now use saveC1z which should populate the pool
	tmpDir := t.TempDir()
	testData := bytes.Repeat([]byte("test data "), 100)

	dbFile := filepath.Join(tmpDir, "test.db")
	err := os.WriteFile(dbFile, testData, 0600)
	require.NoError(t, err)

	c1zFile := filepath.Join(tmpDir, "test.c1z")
	err = saveC1z(dbFile, c1zFile, 0)
	require.NoError(t, err)

	// Now the pool should have an encoder
	enc2, fromPool2 := getEncoder()
	require.True(t, fromPool2, "saveC1z should have returned encoder to pool")
	putEncoder(enc2)
}

// TestPoolGrowsFromDecoder verifies that NewDecoder populates the decoder pool
// even when starting with an empty pool.
func TestPoolGrowsFromDecoder(t *testing.T) {
	// Clear any existing pool state
	for {
		dec, fromPool := getDecoder()
		if !fromPool {
			dec.Close() // zstd.Decoder.Close() returns nothing
			break
		}
		dec.Close() // Don't return to pool
	}

	// Verify pool is empty
	dec, fromPool := getDecoder()
	require.False(t, fromPool, "pool should be empty after draining")
	dec.Close()

	// Create a c1z file to decode
	tmpDir := t.TempDir()
	testData := bytes.Repeat([]byte("test data "), 100)

	dbFile := filepath.Join(tmpDir, "test.db")
	err := os.WriteFile(dbFile, testData, 0600)
	require.NoError(t, err)

	c1zFile := filepath.Join(tmpDir, "test.c1z")
	err = saveC1z(dbFile, c1zFile, 0)
	require.NoError(t, err)

	// Drain encoder pool (saveC1z added one)
	enc, _ := getEncoder()
	_ = enc.Close()

	// Now use NewDecoder which should populate the decoder pool
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

	// Now the decoder pool should have a decoder
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

		// Save using pooled encoder
		c1zFile := filepath.Join(tmpDir, "test.c1z")
		err = saveC1z(dbFile, c1zFile, 0)
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
			err = saveC1z(dbFile, c1zFile, 0)
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
		for i := 0; i < b.N; i++ {
			c1zFile := filepath.Join(tmpDir, "bench.c1z")
			err := saveC1z(dbFile, c1zFile, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("new_encoder_each_time", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c1zFile := filepath.Join(tmpDir, "bench2.c1z")

			dbF, _ := os.Open(dbFile)
			outF, _ := os.Create(c1zFile)

			_, _ = outF.Write(C1ZFileHeader)

			// Create new encoder each time (simulates old behavior)
			enc, _ := zstd.NewWriter(outF, zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
			_, _ = io.Copy(enc, dbF)
			_ = enc.Flush()
			_ = enc.Close()

			_ = outF.Sync()
			_ = outF.Close()
			_ = dbF.Close()
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
			var buf bytes.Buffer
			enc.Reset(&buf)
			_, _ = enc.Write(testData)
			_ = enc.Close()
			putEncoder(enc)
		}
	})

	b.Run("new_each_time", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			enc, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
			_, _ = enc.Write(testData)
			_ = enc.Close()
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
	err = saveC1z(dbFile, c1zFile, 0)
	require.NoError(b, err)

	b.Run("pooled_decoder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			f, _ := os.Open(c1zFile)
			dec, _ := NewDecoder(f)
			_, _ = io.ReadAll(dec)
			_ = dec.Close()
			_ = f.Close()
		}
	})

	b.Run("new_decoder_each_time", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			f, _ := os.Open(c1zFile)

			// Skip header manually
			headerBuf := make([]byte, len(C1ZFileHeader))
			_, _ = f.Read(headerBuf)

			// Create new decoder each time (simulates old behavior)
			dec, _ := zstd.NewReader(f,
				zstd.WithDecoderConcurrency(1),
				zstd.WithDecoderLowmem(true),
				zstd.WithDecoderMaxMemory(defaultDecoderMaxMemory),
			)
			_, _ = io.ReadAll(dec)
			dec.Close()
			_ = f.Close()
		}
	})
}
