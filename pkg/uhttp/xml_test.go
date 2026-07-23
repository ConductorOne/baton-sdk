package uhttp

import (
	"encoding/xml"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXMLMap_UnmarshalXML(t *testing.T) {
	t.Run("should return an error when the response is not XML", func(t *testing.T) {
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte("not XML"), xmlMap)
		require.Error(t, err)
		require.ErrorIs(t, err, io.EOF)

		err = xml.Unmarshal([]byte("<not XML></not XML>"), xmlMap)
		require.Error(t, err)
		var syntaxErr *xml.SyntaxError
		require.ErrorAs(t, err, &syntaxErr)
	})

	t.Run("should unmarshal an XML response with duplicate elements", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><items><item><name>John</name><age>30</age></item><item><name>Jane</name><age>25</age></item></items></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"items": []map[string]any{
				{"item": map[string]any{
					"name": "John",
					"age":  "30",
				}},
				{"item": map[string]any{
					"name": "Jane",
					"age":  "25",
				}},
			},
		}, xmlMap.data)
	})

	t.Run("should unmarshal an XML response with a single element", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><name>John</name></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{"name": "John"}, xmlMap.data)
	})

	t.Run("should unmarshal an XML response with no elements", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{}, xmlMap.data)
	})

	t.Run("should capture attributes on a container element as a sibling @attributes key", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><person id="42"><name>John</name></person></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"person": map[string]any{
				"name":        "John",
				"@attributes": map[string]any{"id": "42"},
			},
		}, xmlMap.data)
	})

	t.Run("should promote an attributed leaf element with text to a map with #text and @attributes", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><name lang="en">John</name></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"name": map[string]any{
				"#text":       "John",
				"@attributes": map[string]any{"lang": "en"},
			},
		}, xmlMap.data)
	})

	t.Run("should represent an attributed leaf element with no text as just @attributes", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><flag enabled="true"></flag></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"flag": map[string]any{
				"@attributes": map[string]any{"enabled": "true"},
			},
		}, xmlMap.data)
	})

	t.Run("should use the local name for a namespace-prefixed attribute", func(t *testing.T) {
		// The xmlns:xsi declaration lives on <response>, but it is a
		// namespace-declaration pseudo-attribute, not application data, so it
		// is filtered out of the root's @attributes (see attrMap) -- the root
		// element ends up with no @attributes at all here, since it carries
		// no other attributes.
		// The attribute under test is xsi:type on <name>, which must be
		// captured using its local name "type", not the namespace-qualified
		// form.
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><name xsi:type="string">John</name></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"name": map[string]any{
				"#text":       "John",
				"@attributes": map[string]any{"type": "string"},
			},
		}, xmlMap.data)
	})

	t.Run("should not treat a bare default-namespace declaration as an attribute", func(t *testing.T) {
		// A bare `xmlns="..."` default-namespace declaration on the root is
		// extremely common in real-world namespaced XML/SOAP documents, even
		// when the element carries no application-level attributes. It must
		// not cause a spurious "@attributes" key to appear, preserving the
		// byte-for-byte no-attribute invariant for ordinary namespaced XML.
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response xmlns="urn:example"><name>John</name></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{"name": "John"}, xmlMap.data)
	})

	t.Run("should disambiguate same-local-name attributes from different namespaces instead of dropping one", func(t *testing.T) {
		// Two attributes with the same local name ("id") but different
		// namespaces on the same element is a realistic SOAP/WS-* pattern.
		// Keying purely by local name would silently overwrite one with the
		// other; both must be preserved via namespace-qualified keys.
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response xmlns:a="urn:a" xmlns:b="urn:b"><elem a:id="1" b:id="2">text</elem></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"elem": map[string]any{
				"#text": "text",
				"@attributes": map[string]any{
					"urn:a:id": "1",
					"urn:b:id": "2",
				},
			},
		}, xmlMap.data)
	})

	t.Run("should give each duplicate-sibling entry an independent @attributes map", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><items count="2"><item>a</item><item>b</item></items></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)

		items, ok := xmlMap.data.(map[string]any)["items"].([]map[string]any)
		require.True(t, ok)
		require.Len(t, items, 2)

		firstAttrs, ok := items[0]["@attributes"].(map[string]any)
		require.True(t, ok)
		secondAttrs, ok := items[1]["@attributes"].(map[string]any)
		require.True(t, ok)

		// Mutating one sibling's @attributes map must not affect the other's.
		firstAttrs["count"] = "mutated"
		require.Equal(t, "2", secondAttrs["count"])
	})

	t.Run("should discard stray text alongside child elements regardless of attributes (pre-existing behavior)", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><person id="1">prefix<name>John</name></person></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"person": map[string]any{
				"name":        "John",
				"@attributes": map[string]any{"id": "1"},
			},
		}, xmlMap.data)
	})

	t.Run("should treat a self-closing attributed tag the same as an explicit open/close pair", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><flag enabled="true"/></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"flag": map[string]any{
				"@attributes": map[string]any{"enabled": "true"},
			},
		}, xmlMap.data)
	})

	t.Run("should capture attributes at every nesting level", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><outer id="1"><inner id="2">value</inner></outer></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"outer": map[string]any{
				"@attributes": map[string]any{"id": "1"},
				"inner": map[string]any{
					"#text":       "value",
					"@attributes": map[string]any{"id": "2"},
				},
			},
		}, xmlMap.data)
	})

	t.Run("should combine a container's attributes with duplicate child siblings", func(t *testing.T) {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response><items count="2"><item>a</item><item>b</item></items></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"items": []map[string]any{
				{"item": "a", "@attributes": map[string]any{"count": "2"}},
				{"item": "b", "@attributes": map[string]any{"count": "2"}},
			},
		}, xmlMap.data)
	})
}
