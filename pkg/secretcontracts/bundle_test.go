package secretcontracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBundleFilesMatchManifestHashes(t *testing.T) {
	raw := readFile(t, filepath.Join("bundle", "manifest.json"))
	var manifest struct {
		BundleSHA256 string            `json:"bundle_sha256"`
		Files        map[string]string `json:"files_sha256"`
	}
	require.NoError(t, json.Unmarshal(raw, &manifest))
	paths := make([]string, 0, len(manifest.Files))
	for path := range manifest.Files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	identity := sha256.New()
	for _, path := range paths {
		onDisk := readFile(t, filepath.Join("bundle", path))
		sum := sha256.Sum256(onDisk)
		require.Equal(t, manifest.Files[path], hex.EncodeToString(sum[:]), path)
		fmt.Fprintf(identity, "%s\x00%s\n", path, manifest.Files[path])
	}
	require.Equal(t, manifest.BundleSHA256, hex.EncodeToString(identity.Sum(nil)))

	var onDisk []string
	require.NoError(t, filepath.WalkDir("bundle", func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		relative, _ := filepath.Rel("bundle", path)
		if relative != "manifest.json" {
			onDisk = append(onDisk, filepath.ToSlash(relative))
		}
		return nil
	}))
	sort.Strings(onDisk)
	require.Equal(t, paths, onDisk, "bundle holds exactly the files the manifest lists")
}

func TestUpstreamPinMatchesVendoredBundle(t *testing.T) {
	var pin struct {
		Repository     string `json:"repository"`
		Commit         string `json:"commit"`
		SourceRevision string `json:"source_revision"`
		BundleSHA256   string `json:"bundle_sha256"`
		ManifestSHA256 string `json:"manifest_sha256"`
	}
	require.NoError(t, json.Unmarshal(readFile(t, "UPSTREAM.json"), &pin))
	require.Len(t, pin.Commit, 40)
	manifestBytes := readFile(t, filepath.Join("bundle", "manifest.json"))
	sum := sha256.Sum256(manifestBytes)
	require.Equal(t, pin.ManifestSHA256, hex.EncodeToString(sum[:]))
	b, err := bundle()
	require.NoError(t, err)
	require.Equal(t, pin.BundleSHA256, b.manifest.BundleSHA256)
	require.Equal(t, pin.SourceRevision, b.manifest.Source.Revision)
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
