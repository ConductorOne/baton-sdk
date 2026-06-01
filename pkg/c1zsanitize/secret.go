package c1zsanitize

import (
	"crypto/rand"
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
// is set it loads and length-checks that file. Otherwise it mints a
// fresh CSPRNG secret and writes it next to outPath, refusing to
// clobber an existing one so a prior run's reversible mapping is never
// silently replaced. generated reports whether a new secret was minted
// so the caller can tell the operator to archive it.
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
	if _, err := os.Stat(path); err == nil {
		return nil, false, fmt.Errorf("default secret path %q already exists; pass -secret-file to reuse it", path)
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
