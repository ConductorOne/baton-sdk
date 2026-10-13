package c1zsanitize

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
)

// SecretPath returns where the per-c1z HMAC secret is read from or
// written to: the explicit flag path when set, otherwise a file next
// to the sanitized output.
func SecretPath(flagPath, outPath string) string {
	if flagPath != "" {
		return flagPath
	}
	return outPath + ".secret"
}

// LoadOrGenerateSecret returns the per-c1z HMAC secret. When flagPath
// is set it loads and length-checks that file. The default path is reused
// with an existing output so an unfinished run can resume.
func LoadOrGenerateSecret(flagPath, outPath string) ([]byte, bool, error) {
	if flagPath != "" {
		b, err := os.ReadFile(flagPath)
		if err != nil {
			return nil, false, fmt.Errorf("read -secret-file: %w", err)
		}
		if len(b) < MinSecretBytes {
			return nil, false, fmt.Errorf("-secret-file %q is too short: got %d bytes, want at least %d", flagPath, len(b), MinSecretBytes)
		}
		return b, false, nil
	}
	path := SecretPath(flagPath, outPath)
	_, outErr := os.Stat(outPath)
	outputExists := outErr == nil
	if outErr != nil && !errors.Is(outErr, os.ErrNotExist) {
		return nil, false, fmt.Errorf("stat output path %q: %w", outPath, outErr)
	}
	if _, err := os.Stat(path); err == nil {
		if !outputExists {
			return nil, false, fmt.Errorf("default secret path %q exists without output; pass --secret-file to reuse it", path)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, false, fmt.Errorf("read default secret path %q: %w", path, err)
		}
		if len(b) < MinSecretBytes {
			return nil, false, fmt.Errorf("default secret path %q is too short: got %d bytes, want at least %d", path, len(b), MinSecretBytes)
		}
		return b, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, false, fmt.Errorf("stat default secret path %q: %w", path, err)
	}
	if outputExists {
		return nil, false, fmt.Errorf("default secret path %q is missing for existing output", path)
	}
	b := make([]byte, MinSecretBytes)
	if _, err := rand.Read(b); err != nil {
		return nil, false, fmt.Errorf("generate secret: %w", err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return nil, false, fmt.Errorf("write generated secret: %w", err)
	}
	return b, true, nil
}
