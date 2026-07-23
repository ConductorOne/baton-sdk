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
		// The xmlns:xsi declaration lives on <response>, so it becomes an
		// attribute of the root element itself (Go's encoding/xml surfaces
		// namespace declarations as ordinary Attr entries, keyed by prefix).
		// The attribute under test is xsi:type on <name>, which must be
		// captured using its local name "type", not the namespace-qualified
		// form.
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?><response xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><name xsi:type="string">John</name></response>`
		xmlMap := &xmlMap{}
		err := xml.Unmarshal([]byte(xmlResponse), xmlMap)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"@attributes": map[string]any{"xsi": "http://www.w3.org/2001/XMLSchema-instance"},
			"name": map[string]any{
				"#text":       "John",
				"@attributes": map[string]any{"type": "string"},
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
