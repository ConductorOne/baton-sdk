package secretcontracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpstreamPinMatchesVendoredBundle(t *testing.T) {
	var pin struct {
		Commit string            `json:"commit"`
		Files  map[string]string `json:"files_sha256"`
	}
	require.NoError(t, json.Unmarshal(readFile(t, "UPSTREAM.json"), &pin))
	require.Equal(t, upstreamCommit, pin.Commit)
	var paths []string
	require.NoError(t, filepath.WalkDir("bundle", func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		relative, err := filepath.Rel("bundle", path)
		if err != nil {
			return err
		}
		paths = append(paths, filepath.ToSlash(relative))
		return nil
	}))
	sort.Strings(paths)
	require.Len(t, pin.Files, len(paths))
	for _, path := range paths {
		sum := sha256.Sum256(readFile(t, filepath.Join("bundle", path)))
		require.Equal(t, pin.Files[path], hex.EncodeToString(sum[:]), path)
	}
}

func TestEveryManifestRuleHasAGoImplementation(t *testing.T) {
	b, err := bundle()
	require.NoError(t, err)
	spanning := map[string]bool{"absolute_uri_items": true, "requires_private_key": true}
	seen := 0
	for _, c := range b.contracts {
		for _, rule := range c.SemanticRules {
			seen++
			_, isString := stringRules[rule.Rule]
			require.True(t, isString || spanning[rule.Rule], "%s.%s: %s has no Go check", c.ContentType, rule.Field, rule.Rule)
		}
	}
	require.NotZero(t, seen)
}

func TestBundleDefinesAllEightTypes(t *testing.T) {
	types, err := ContentTypes()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"password_v2", "hotp", "api_key_v2", "ssh_key", "oauth_client_secret_v2", "certificate", "note", "generic"}, types)
}

func TestErrorsNeverCarryValues(t *testing.T) {
	secret := "do-not-leak-" + strings.Repeat("x", 8)
	for _, raw := range []string{
		`{"password":"p","url":"` + secret + `"}`,
		`{"password":"p","` + secret + `":"v"}`,
		`{"password":"` + secret + `","password":"b"}`,
	} {
		err := ValidateProducer("password_v2", []byte(raw))
		require.Error(t, err)
		require.NotContains(t, err.Error(), secret)
	}
}

func TestExportedTypeIDsMatchBundle(t *testing.T) {
	bundled, err := ContentTypes()
	require.NoError(t, err)
	require.ElementsMatch(t, bundled, []string{TypePasswordV2, TypeAPIKeyV2, TypeSSHKey, TypeOAuthClientSecretV2, TypeCertificate, TypeHOTP, TypeNote, TypeGeneric})
}
