package secretcontracts

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// StoredKind says how the receiver treats a stored value that passed
// ValidateStored.
type StoredKind string

const (
	// StoredTyped opens as its declared type.
	StoredTyped StoredKind = "typed"
	// StoredOpaque is kept as an opaque value: a legacy generic that is not
	// valid text, or a content type the bundle does not define.
	StoredOpaque StoredKind = "opaque"
)

// ValidateProducer reports whether a connector may write raw as contentType.
// It refuses undefined types, blank optional values and legacy opaque bytes.
func ValidateProducer(contentType string, raw []byte) error {
	b, err := bundle()
	if err != nil {
		return err
	}
	c, ok := b.contracts[contentType]
	if !ok {
		return newError(LayerUnknownType, "unknown_content_type", "")
	}
	return c.check(&b.manifest, raw, true)
}

// ValidateStored reports whether the receiver opens raw stored as contentType.
// A defined type must pass its contract, where a blank optional string or an
// empty optional list counts as absent. Generic accepts any value up to the
// value cap, and a type the bundle does not define is opaque, so neither is
// rejected for its content.
func ValidateStored(contentType string, raw []byte) (StoredKind, error) {
	b, err := bundle()
	if err != nil {
		return "", err
	}
	if len(raw) > b.manifest.Limits.MaxValueBytes {
		return "", newError(LayerLimit, "value_too_large", "")
	}
	c, defined := b.contracts[contentType]
	if !defined {
		return StoredOpaque, nil
	}
	typedErr := c.check(&b.manifest, raw, false)
	switch {
	case typedErr == nil:
		return StoredTyped, nil
	case contentType == "generic":
		return StoredOpaque, nil
	default:
		return "", typedErr
	}
}

func (c *contract) check(m *manifest, raw []byte, producer bool) error {
	if c.Codec == "utf8_text_v1" {
		return c.checkText(raw)
	}
	return c.checkJSON(m, raw, producer)
}

func (c *contract) checkText(raw []byte) error {
	field := c.TextCodec.Field
	if !utf8.Valid(raw) {
		return newError(LayerDocument, "invalid_utf8", field)
	}
	value := string(raw)
	if strings.TrimSpace(value) == "" {
		return newError(LayerSemantic, "required", field)
	}
	if len(raw) > c.TextCodec.MaxUTF8Bytes {
		return newError(LayerLimit, "max_bytes", field)
	}
	if utf8.RuneCountInString(value) > c.TextCodec.MaxCodePoints {
		return newError(LayerSchema, "max_length", field)
	}
	if prefix := c.TextCodec.RequiredPref; prefix != nil && !strings.HasPrefix(value, *prefix) {
		return newError(LayerSchema, "pattern", field)
	}
	for _, rule := range c.SemanticRules {
		if !stringRules[rule.Rule](value) {
			return newError(LayerSemantic, rule.Rule, field)
		}
	}
	return nil
}

func (c *contract) checkJSON(m *manifest, raw []byte, producer bool) error {
	if len(raw) > m.Limits.MaxJSONDocumentBytes {
		return newError(LayerLimit, "document_too_large", "")
	}
	document, perr := parseDocument(raw)
	if perr != nil {
		return perr
	}
	for name, value := range document {
		if _, declared := c.properties[name]; !declared || c.required[name] || !isBlank(value) {
			continue
		}
		if producer {
			return newError(LayerNormalization, "blank_optional", name)
		}
		delete(document, name)
	}
	if err := c.schema.Validate(document); err != nil {
		return schemaError(err)
	}
	for name, value := range document {
		if err := c.checkLimits(name, value); err != nil {
			return err
		}
	}
	for _, rule := range c.SemanticRules {
		if err := c.applyRule(document, rule.Field, rule.Rule); err != nil {
			return err
		}
	}
	return nil
}

func isBlank(value any) bool {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) == ""
	case []any:
		return len(typed) == 0
	default:
		return false
	}
}

func (c *contract) checkLimits(name string, value any) *Error {
	property := c.properties[name]
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return newError(LayerSemantic, "required", name)
		}
		if len(typed) > property.maxBytes {
			return newError(LayerLimit, "max_bytes", name)
		}
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok && len(text) > property.itemMaxBytes {
				return newError(LayerLimit, "max_bytes", name)
			}
		}
	}
	return nil
}

func (c *contract) applyRule(document map[string]any, field, rule string) *Error {
	value, present := document[field]
	if !present {
		return nil
	}
	switch rule {
	case "absolute_uri_items":
		items, _ := value.([]any)
		for _, item := range items {
			if text, ok := item.(string); !ok || !absoluteURI(text) {
				return newError(LayerSemantic, rule, field)
			}
		}
	case "requires_private_key":
		if _, hasKey := document["private_key"]; !hasKey {
			return newError(LayerSemantic, rule, "private_key")
		}
	default:
		text, ok := value.(string)
		if ok && strings.TrimSpace(text) != "" && !stringRules[rule](text) {
			return newError(LayerSemantic, rule, field)
		}
	}
	return nil
}

// stringRules are the semantic checks on one string value, by manifest rule
// id. absolute_uri_items and requires_private_key span items or fields and
// are handled in applyRule. A test requires every id in the manifest to be
// either here or there.
var stringRules = map[string]func(string) bool{
	"http_url":           httpURL,
	"otp_uri":            func(v string) bool { return otpAuthURI(v, "") },
	"hotp_uri":           func(v string) bool { return otpAuthURI(v, "hotp") },
	"rfc3339_full_date":  rfc3339FullDate,
	"ssh_username":       sshUsername,
	"ssh_private_key":    sshPrivateKey,
	"ssh_public_key":     sshPublicKey,
	"ssh_certificate":    sshCertificate,
	"single_certificate": singleCertificate,
	"certificate_chain":  certificateChain,
	"pem_private_key":    pemPrivateKey,
}

// schemaError reports the first failing keyword and its field, never the value.
func schemaError(err error) *Error {
	var failure *jsonschema.ValidationError
	if !errors.As(err, &failure) {
		return newError(LayerSchema, "invalid", "")
	}
	for len(failure.Causes) > 0 {
		failure = failure.Causes[0]
	}
	field := ""
	if len(failure.InstanceLocation) > 0 {
		field = failure.InstanceLocation[0]
	}
	rule := "invalid"
	if path := failure.ErrorKind.KeywordPath(); len(path) > 0 {
		rule = path[len(path)-1]
	}
	return newError(LayerSchema, rule, field)
}
