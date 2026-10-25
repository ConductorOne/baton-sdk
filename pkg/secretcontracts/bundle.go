package secretcontracts

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed bundle/manifest.json bundle/schemas/*.schema.json
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
	ContentType   string   `json:"content_type"`
	Codec         string   `json:"codec"`
	FieldOrder    []string `json:"field_order"`
	Schema        *string  `json:"schema"`
	SchemaID      string   `json:"schema_id"`
	SemanticRules []struct {
		Field string `json:"field"`
		Rule  string `json:"rule"`
	} `json:"semantic_rules"`
	TextCodec *struct {
		Field         string  `json:"field"`
		MaxUTF8Bytes  int     `json:"max_utf8_bytes"`
		MaxCodePoints int     `json:"max_code_points"`
		RequiredPref  *string `json:"required_prefix"`
	} `json:"text_codec"`
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
	raw, err := bundleFS.ReadFile("bundle/manifest.json")
	if err != nil {
		return loaded{}, err
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return loaded{}, fmt.Errorf("secretcontracts: manifest: %w", err)
	}
	if m.BundleFormat != 1 {
		return loaded{}, fmt.Errorf("secretcontracts: unsupported bundle format %d", m.BundleFormat)
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	out := loaded{manifest: m, contracts: make(map[string]*contract, len(m.Types))}
	for _, t := range m.Types {
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
		if err := compiler.AddResource(t.SchemaID, doc); err != nil {
			return loaded{}, err
		}
		if c.schema, err = compiler.Compile(t.SchemaID); err != nil {
			return loaded{}, fmt.Errorf("secretcontracts: %s: %w", *t.Schema, err)
		}
		var shape struct {
			Required   []string `json:"required"`
			Properties map[string]struct {
				Type     string `json:"type"`
				MaxBytes int    `json:"x-max-utf8-bytes"`
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
	b, err := bundle()
	if err != nil {
		return "", err
	}
	return b.manifest.Source.Revision, nil
}
