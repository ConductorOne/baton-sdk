package v2

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

// C1ZV2FileHeader is the 5-byte header for v2 C1Z files
var C1ZV2FileHeader = []byte("C1Z2\x00")

// readV2Header reads and validates the v2 C1Z header from the reader.
// On return, the reader will be pointing to the first byte after the header.
func readV2Header(reader io.Reader) error {
	headerBytes := make([]byte, len(C1ZV2FileHeader))
	_, err := reader.Read(headerBytes)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if !bytes.Equal(headerBytes, C1ZV2FileHeader) {
		return fmt.Errorf("invalid v2 C1Z file header")
	}

	return nil
}

// writeV2Header writes the v2 C1Z header to the writer.
func writeV2Header(writer io.Writer) error {
	_, err := writer.Write(C1ZV2FileHeader)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	return nil
}

// loadC1zV2 extracts a compressed tar file to a temporary directory.
// If the filePath exists, it will be extracted to the working dir.
// The working dir is returned.
func loadC1zV2(filePath string, tmpDir string) (string, error) {
	var err error
	workingDir, err := os.MkdirTemp(tmpDir, "c1z-v2")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Check if file exists and has content
	if stat, err := os.Stat(filePath); err == nil && stat.Size() != 0 {
		err = extractTarGz(filePath, workingDir)
		if err != nil {
			_ = os.RemoveAll(workingDir)
			return "", fmt.Errorf("failed to extract tar file: %w", err)
		}
	}

	return workingDir, nil
}

// saveC1zV2 compresses a working directory back to a tar file.
func saveC1zV2(workingDir string, outputFilePath string) error {
	if outputFilePath == "" {
		return errors.New("c1z-v2: output file path not configured")
	}

	// Create output file
	outFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() {
		if closeErr := outFile.Close(); closeErr != nil {
			zap.L().Error("failed to close output file", zap.Error(closeErr))
		}
	}()

	// Write the v2 header
	err = writeV2Header(outFile)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Create tar.gz archive
	err = createTarGz(workingDir, outFile)
	if err != nil {
		return fmt.Errorf("failed to create tar archive: %w", err)
	}

	return nil
}

// extractTarGz extracts a gzipped tar file to the specified directory.
func extractTarGz(srcPath, destDir string) error {
	file, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open tar file: %w", err)
	}
	defer file.Close()

	// Read and validate the v2 header
	err = readV2Header(file)
	if err != nil {
		return fmt.Errorf("failed to validate header: %w", err)
	}

	gzr, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Validate file path to prevent directory traversal
		if !isValidPath(header.Name) {
			return fmt.Errorf("invalid file path in tar: %s", header.Name)
		}

		target := filepath.Join(destDir, header.Name)

		// Ensure target is within destDir
		if !strings.HasPrefix(target, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("invalid file path, outside destination directory: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create directory %s: %w", target, err)
			}
		case tar.TypeReg:
			err = extractFile(tr, target, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to extract file %s: %w", target, err)
			}
		default:
			return fmt.Errorf("unsupported file type %c in tar for file %s", header.Typeflag, header.Name)
		}
	}

	return nil
}

// createTarGz creates a gzipped tar archive from the specified directory.
func createTarGz(srcDir string, dest io.Writer) error {
	gzw := gzip.NewWriter(dest)
	defer func() {
		if closeErr := gzw.Close(); closeErr != nil {
			zap.L().Error("failed to close gzip writer", zap.Error(closeErr))
		}
	}()

	tw := tar.NewWriter(gzw)
	defer func() {
		if closeErr := tw.Close(); closeErr != nil {
			zap.L().Error("failed to close tar writer", zap.Error(closeErr))
		}
	}()

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk directory: %w", err)
		}

		// Get relative path from source directory
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		// Skip the root directory itself
		if relPath == "." {
			return nil
		}

		// Create tar header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("failed to create tar header: %w", err)
		}

		// Use forward slashes for tar paths
		header.Name = filepath.ToSlash(relPath)

		// Write header
		err = tw.WriteHeader(header)
		if err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		// Write file content if it's a regular file
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open file %s: %w", path, err)
			}
			defer file.Close()

			_, err = io.Copy(tw, file)
			if err != nil {
				return fmt.Errorf("failed to copy file content: %w", err)
			}
		}

		return nil
	})
}

// extractFile extracts a single file from the tar reader.
func extractFile(tr *tar.Reader, target string, mode os.FileMode) error {
	// Ensure parent directory exists
	err := os.MkdirAll(filepath.Dir(target), 0755)
	if err != nil {
		return fmt.Errorf("failed to create parent directory: %w", err)
	}

	// Create the file
	file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Copy content
	_, err = io.Copy(file, tr)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	return nil
}

// isValidPath checks if a path is valid and safe for extraction.
func isValidPath(path string) bool {
	// Reject paths with .. components or absolute paths
	if strings.Contains(path, "..") || filepath.IsAbs(path) {
		return false
	}
	return true
}
