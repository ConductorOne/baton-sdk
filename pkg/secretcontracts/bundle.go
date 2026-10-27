package secretcontracts

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed bundle/contracts.json bundle/schemas/*.schema.json
var bundleFS embed.FS

type manifest struct {
	BundleFormat int `json:"bundle_format"`
	Source       struct {
		Revision string `json:"revision"`
	} `json:"source"`
	BundleSHA256 string `json:"bundle_sha256"`
	Limits       struct {
		MaxJSONDocumentBytes int `json:"max_json_document_bytes"`
		MaxValueBytes        int `json:"max_value_bytes"`
	} `json:"limits"`
	Types []manifestType `json:"types"`
}

type manifestType struct {
	ContentType   string         `json:"secret_content_type"`
	Codec         string         `json:"codec"`
	FieldOrder    []string       `json:"field_order"`
	Schema        *string        `json:"schema"`
	SchemaID      string         `json:"schema_id"`
	SemanticRules []semanticRule `json:"-"`
	TextCodec     *struct {
		SemanticRules []string `json:"semantic_rules"`
		Field         string   `json:"field"`
		MaxUTF8Bytes  int      `json:"max_utf8_bytes"`
		MaxCodePoints int      `json:"max_code_points"`
		RequiredPref  *string  `json:"required_prefix"`
	} `json:"text_codec"`
}

type semanticRule struct {
	Field string
	Rule  string
}

type property struct {
	isList       bool
	maxBytes     int
	itemMaxBytes int
}

// contract is one type's compiled contract.
type contract struct {
	manifestType
	schema     *jsonschema.Schema
	required   map[string]bool
	properties map[string]property
}

type loaded struct {
	manifest  manifest
	contracts map[string]*contract
}

var (
	loadOnce sync.Once
	loadedBy loaded
	loadErr  error
)

func bundle() (*loaded, error) {
	loadOnce.Do(func() { loadedBy, loadErr = loadBundle() })
	return &loadedBy, loadErr
}

func loadBundle() (loaded, error) {
	raw, err := bundleFS.ReadFile("bundle/contracts.json")
	if err != nil {
		return loaded{}, err
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return loaded{}, fmt.Errorf("secretcontracts: manifest: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	out := loaded{manifest: m, contracts: make(map[string]*contract, len(m.Types))}
	for _, t := range m.Types {
		t.FieldOrder = encoderOrder[t.ContentType]
		if t.TextCodec != nil {
			for _, rule := range t.TextCodec.SemanticRules {
				t.SemanticRules = append(t.SemanticRules, semanticRule{Field: t.TextCodec.Field, Rule: rule})
			}
		}
		c := &contract{manifestType: t, required: map[string]bool{}, properties: map[string]property{}}
		out.contracts[t.ContentType] = c
		if t.Schema == nil {
			continue
		}
		schemaBytes, err := bundleFS.ReadFile("bundle/" + *t.Schema)
		if err != nil {
			return loaded{}, err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaBytes))
		if err != nil {
			return loaded{}, fmt.Errorf("secretcontracts: %s: %w", *t.Schema, err)
		}
		var identity struct {
			ID string `json:"$id"`
		}
		if err := json.Unmarshal(schemaBytes, &identity); err != nil {
			return loaded{}, err
		}
		if err := compiler.AddResource(identity.ID, doc); err != nil {
			return loaded{}, err
		}
		if c.schema, err = compiler.Compile(identity.ID); err != nil {
			return loaded{}, fmt.Errorf("secretcontracts: %s: %w", *t.Schema, err)
		}
		var shape struct {
			Required   []string `json:"required"`
			Properties map[string]struct {
				Type     string   `json:"type"`
				MaxBytes int      `json:"x-max-utf8-bytes"`
				Semantic []string `json:"x-semantic"`
				Items    struct {
					MaxBytes int `json:"x-max-utf8-bytes"`
				} `json:"items"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(schemaBytes, &shape); err != nil {
			return loaded{}, err
		}
		for _, name := range shape.Required {
			c.required[name] = true
		}
		for name, p := range shape.Properties {
			for _, rule := range p.Semantic {
				c.SemanticRules = append(c.SemanticRules, semanticRule{Field: name, Rule: rule})
			}
			c.properties[name] = property{isList: p.Type == "array", maxBytes: p.MaxBytes, itemMaxBytes: p.Items.MaxBytes}
		}
	}
	return out, nil
}

// ContentTypes lists the types the bundle defines, in manifest order.
func ContentTypes() ([]string, error) {
	b, err := bundle()
	if err != nil {
		return nil, err
	}
	types := make([]string, len(b.manifest.Types))
	for i, t := range b.manifest.Types {
		types[i] = t.ContentType
	}
	return types, nil
}

// SourceRevision is the Multipass commit the bundle was generated from.
func SourceRevision() (string, error) {
	_, err := bundle()
	if err != nil {
		return "", err
	}
	return upstreamCommit, nil
}

// encoderOrder preserves existing output bytes; schemas do not require an order.
var encoderOrder = map[string][]string{
	"password_v2":            {"username", "password", "url", "otp_url"},
	"api_key_v2":             {"key_value", "provider", "base_url", "scopes", "key_id", "header_name", "expires_at"},
	"ssh_key":                {"private_key", "username", "passphrase", "public_key", "hosts", "ssh_certificate"},
	"oauth_client_secret_v2": {"client_id", "client_secret", "token_endpoint_url", "scopes", "issuer_url", "redirect_uri", "auth_endpoint_url", "audience", "grant_type"},
	"certificate":            {"certificate_pem", "private_key", "chain_pem", "passphrase"},
}

const upstreamCommit = "cabf67a9c65ea52b3c353a2d8c92c320cc8f5467"
