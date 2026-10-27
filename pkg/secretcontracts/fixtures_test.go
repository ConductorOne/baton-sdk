package secretcontracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fixtureCase struct {
	ID          string `json:"id"`
	ContentType string `json:"secret_content_type"`
	File        string `json:"file"`
	Synthetic   *struct {
		Repeat string `json:"repeat"`
		Count  int    `json:"count"`
	} `json:"synthetic"`
	Producer struct {
		Accept bool    `json:"accept"`
		Layer  *string `json:"layer"`
	} `json:"producer"`
	Stored     string `json:"stored"`
	RustDecode struct {
		OK bool `json:"ok"`
	} `json:"rust_decode"`
	RustProducer *struct {
		OK bool `json:"ok"`
	} `json:"rust_producer_validation"`
}

func loadFixtures(t *testing.T) []fixtureCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("bundle", "fixtures", "index.json"))
	require.NoError(t, err)
	var index struct {
		Cases []fixtureCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &index))
	require.NotEmpty(t, index.Cases)
	return index.Cases
}

func (c fixtureCase) bytes(t *testing.T) []byte {
	t.Helper()
	if c.Synthetic != nil {
		return []byte(strings.Repeat(c.Synthetic.Repeat, c.Synthetic.Count))
	}
	raw, err := os.ReadFile(filepath.Join("bundle", c.File))
	require.NoError(t, err)
	return raw
}

func TestSharedFixturesProducerVerdict(t *testing.T) {
	for _, c := range loadFixtures(t) {
		t.Run(c.ID, func(t *testing.T) {
			err := ValidateProducer(c.ContentType, c.bytes(t))
			if c.Producer.Accept {
				require.NoError(t, err)
				return
			}
			var failure *Error
			require.ErrorAs(t, err, &failure)
			switch *c.Producer.Layer {
			case "document", "normalization":
				require.Equal(t, Layer(*c.Producer.Layer), failure.Layer)
			case "semantic":
				// A format keyword in the schema may reject first.
				require.Contains(t, []Layer{LayerSemantic, LayerLimit, LayerSchema}, failure.Layer)
			case "schema":
				require.Contains(t, []Layer{LayerSchema, LayerSemantic, LayerLimit}, failure.Layer)
			}
		})
	}
}

func TestSharedFixturesStoredVerdict(t *testing.T) {
	for _, c := range loadFixtures(t) {
		t.Run(c.ID, func(t *testing.T) {
			kind, err := ValidateStored(c.ContentType, c.bytes(t))
			switch c.Stored {
			case "typed":
				require.NoError(t, err)
				require.Equal(t, StoredTyped, kind)
			case "opaque":
				require.NoError(t, err)
				require.Equal(t, StoredOpaque, kind)
			default:
				require.Error(t, err)
			}
		})
	}
}

// A case the Rust receiver opens as typed must open as typed here, and one
// whose decode fails must not.
func TestSharedFixturesAgreeWithRustReceiver(t *testing.T) {
	for _, c := range loadFixtures(t) {
		t.Run(c.ID, func(t *testing.T) {
			if c.Stored == "opaque" {
				return
			}
			rustTyped := c.RustDecode.OK && c.RustProducer != nil && c.RustProducer.OK
			_, err := ValidateStored(c.ContentType, c.bytes(t))
			require.Equal(t, rustTyped, err == nil)
		})
	}
}

func TestEquivalentSpellingsDecodeToTheSameFields(t *testing.T) {
	for _, contentType := range []string{"password_v2", "api_key_v2", "ssh_key", "oauth_client_secret_v2", "certificate"} {
		dir := filepath.Join("bundle", "fixtures", "valid", contentType)
		full := mustParse(t, readFile(t, filepath.Join(dir, "full.json")))
		for _, variant := range []string{"reordered-keys", "whitespace", "escaped"} {
			require.Equal(t, full, mustParse(t, readFile(t, filepath.Join(dir, variant+".json"))), "%s/%s", contentType, variant)
		}
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}

func mustParse(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	document, perr := parseDocument(raw)
	require.Nil(t, perr)
	return document
}
