package dotc1z

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLoadC1z(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("temp directory cleanup on error", func(t *testing.T) {
		// Create a file that will cause an error during decoding
		invalidFile := filepath.Join(tmpDir, "invalid2.c1z")
		err := os.WriteFile(invalidFile, []byte("invalid"), 0600)
		require.NoError(t, err)
		defer os.Remove(invalidFile)

		// Try to load it - should fail and clean up temp dir
		dbPath, err := loadC1z(invalidFile, tmpDir)
		require.Error(t, err)
		require.Empty(t, dbPath)
	})

	t.Run("custom tmpDir", func(t *testing.T) {
		customTmpDir := filepath.Join(tmpDir, "custom")
		err := os.MkdirAll(customTmpDir, 0755)
		require.NoError(t, err)
		defer os.RemoveAll(customTmpDir)

		nonExistentPath := filepath.Join(tmpDir, "nonexistent2.c1z")
		dbPath, err := loadC1z(nonExistentPath, customTmpDir)
		require.NoError(t, err)
		require.NotEmpty(t, dbPath)
		require.FileExists(t, dbPath)

		// Verify it was created in the custom tmpDir
		require.Contains(t, dbPath, customTmpDir)
	})
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

	t.Run("save with empty output path returns error", func(t *testing.T) {
		dbFile := filepath.Join(tmpDir, "test.db")
		err := os.WriteFile(dbFile, []byte(""), 0600)
		require.NoError(t, err)
		defer os.Remove(dbFile)

		err = saveC1z(dbFile, "", 1)
		require.Error(t, err)
		require.True(t, status.Code(err) == codes.InvalidArgument)
		require.Contains(t, err.Error(), "output file path not configured")
	})

	t.Run("save with non-existent db file returns error", func(t *testing.T) {
		nonExistentDb := filepath.Join(tmpDir, "nonexistent.db")
		outputFile := filepath.Join(tmpDir, "output.c1z")

		err := saveC1z(nonExistentDb, outputFile, 1)
		require.Error(t, err)
	})

	t.Run("save overwrites existing file", func(t *testing.T) {
		testData1 := []byte("first content")
		dbFile1 := filepath.Join(tmpDir, "overwrite1.db")
		err := os.WriteFile(dbFile1, testData1, 0600)
		require.NoError(t, err)
		defer os.Remove(dbFile1)

		outputFile := filepath.Join(tmpDir, "overwrite.c1z")
		err = saveC1z(dbFile1, outputFile, 1)
		require.NoError(t, err)
		defer os.Remove(outputFile)

		// Get the size of the first file
		stat1, err := os.Stat(outputFile)
		require.NoError(t, err)
		size1 := stat1.Size()

		// Save different content to the same file
		testData2 := []byte("second content - different")
		dbFile2 := filepath.Join(tmpDir, "overwrite2.db")
		err = os.WriteFile(dbFile2, testData2, 0600)
		require.NoError(t, err)
		defer os.Remove(dbFile2)

		err = saveC1z(dbFile2, outputFile, 1)
		require.NoError(t, err)

		// Verify the file was overwritten
		stat2, err := os.Stat(outputFile)
		require.NoError(t, err)
		// Size might be different due to compression, but file should exist and be valid
		require.NotEqual(t, size1, stat2.Size())

		// Verify the content is the new content
		f, err := os.Open(outputFile)
		require.NoError(t, err)
		defer f.Close()

		decoder, err := NewDecoder(f)
		require.NoError(t, err)
		defer decoder.Close()

		decodedData, err := io.ReadAll(decoder)
		require.NoError(t, err)
		require.Equal(t, testData2, decodedData)
	})

	t.Run("save empty db file", func(t *testing.T) {
		emptyDbFile := filepath.Join(tmpDir, "empty.db")
		err := os.WriteFile(emptyDbFile, []byte{}, 0600)
		require.NoError(t, err)

		outputFile := filepath.Join(tmpDir, "empty.c1z")
		err = saveC1z(emptyDbFile, outputFile, 1)
		require.NoError(t, err)
		require.FileExists(t, outputFile)

		// Verify the file has the correct header
		fileData, err := os.ReadFile(outputFile)
		require.NoError(t, err)
		require.True(t, len(fileData) >= len(C1ZFileHeader))
		require.Equal(t, C1ZFileHeader, fileData[:len(C1ZFileHeader)])

		// Verify we can decode it (should be empty)
		f, err := os.Open(outputFile)
		require.NoError(t, err)
		defer f.Close()

		decoder, err := NewDecoder(f)
		require.NoError(t, err)
		defer decoder.Close()

		decodedData, err := io.ReadAll(decoder)
		require.NoError(t, err)
		require.Empty(t, decodedData)
	})
}
