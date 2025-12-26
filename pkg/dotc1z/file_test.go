package dotc1z

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkSaveC1z(b *testing.B) {
	tmpDir := b.TempDir()

	// Create test data of various sizes
	sizes := []int{1024, 100 * 1024, 1024 * 1024} // 1KB, 100KB, 1MB

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			testData := make([]byte, size)
			for i := range testData {
				testData[i] = byte(i % 256)
			}
			dbFile := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.db", size))
			err := os.WriteFile(dbFile, testData, 0600)
			if err != nil {
				b.Fatal(err)
			}

			outputFile := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.c1z", size))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := saveC1z(dbFile, outputFile, 1)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSaveC1z(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("save valid db file", func(t *testing.T) {
		testData := []byte("test database content for saving")
		dbFile := filepath.Join(tmpDir, "save_test.db")
		err := os.WriteFile(dbFile, testData, 0600)
		require.NoError(t, err)
		defer os.Remove(dbFile)

		outputFile := filepath.Join(tmpDir, "save_test.c1z")
		err = saveC1z(dbFile, outputFile, 1)
		require.NoError(t, err)
		require.FileExists(t, outputFile)
		defer os.Remove(outputFile)

		// Verify the file has the correct header
		fileData, err := os.ReadFile(outputFile)
		require.NoError(t, err)
		require.True(t, len(fileData) >= len(C1ZFileHeader))
		require.Equal(t, C1ZFileHeader, fileData[:len(C1ZFileHeader)])

		// Verify we can decode it
		f, err := os.Open(outputFile)
		require.NoError(t, err)
		defer f.Close()

		decoder, err := NewDecoder(f)
		require.NoError(t, err)
		defer decoder.Close()

		decodedData, err := io.ReadAll(decoder)
		require.NoError(t, err)
		require.Equal(t, testData, decodedData)
	})
}

// TestSaveC1zAtomicWrite verifies that saveC1z uses atomic writes:
// 1. Output file is never partially written (either old data or new data, never corrupt).
// 2. Temp files are cleaned up on failure.
// 3. Existing output file is preserved if saveC1z fails.
func TestSaveC1zAtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("existing output preserved on failure", func(t *testing.T) {
		// Create initial valid c1z
		initialData := []byte("initial database content")
		dbFile := filepath.Join(tmpDir, "initial.db")
		err := os.WriteFile(dbFile, initialData, 0600)
		require.NoError(t, err)

		outputFile := filepath.Join(tmpDir, "output.c1z")
		err = saveC1z(dbFile, outputFile, 1)
		require.NoError(t, err)

		// Read the valid output
		originalContent, err := os.ReadFile(outputFile)
		require.NoError(t, err)
		require.NotEmpty(t, originalContent)

		// Now try to saveC1z with non-existent source - should fail
		nonExistentDb := filepath.Join(tmpDir, "does_not_exist.db")
		err = saveC1z(nonExistentDb, outputFile, 1)
		require.Error(t, err)

		// Output file should be UNCHANGED (still has original content)
		afterContent, err := os.ReadFile(outputFile)
		require.NoError(t, err)
		require.Equal(t, originalContent, afterContent, "output file should be unchanged after failed saveC1z")

		// Verify it's still valid
		_, err = loadC1z(outputFile, tmpDir)
		require.NoError(t, err, "output file should still be loadable after failed saveC1z")
	})

	t.Run("no temp file left on failure", func(t *testing.T) {
		// Try to save with non-existent source
		nonExistentDb := filepath.Join(tmpDir, "does_not_exist.db")
		outputFile := filepath.Join(tmpDir, "output2.c1z")

		err := saveC1z(nonExistentDb, outputFile, 1)
		require.Error(t, err)

		// Check no temp files left behind
		matches, err := filepath.Glob(filepath.Join(tmpDir, "*.tmp-*"))
		require.NoError(t, err)
		require.Empty(t, matches, "no temp files should be left after failed saveC1z")
	})

	t.Run("no output file created on failure", func(t *testing.T) {
		nonExistentDb := filepath.Join(tmpDir, "does_not_exist.db")
		outputFile := filepath.Join(tmpDir, "should_not_exist.c1z")

		err := saveC1z(nonExistentDb, outputFile, 1)
		require.Error(t, err)

		// Output file should not exist
		_, statErr := os.Stat(outputFile)
		require.True(t, os.IsNotExist(statErr), "output file should not exist after saveC1z error")
	})
}
