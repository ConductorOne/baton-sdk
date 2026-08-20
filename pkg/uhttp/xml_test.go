package uhttp

import (
	"encoding/xml"
	"io"
	"strings"
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

	t.Run("should reject a body nested past the depth limit", func(t *testing.T) {
		// Without the limit this is not a returned error but a fatal stack
		// overflow, which recover() cannot catch, so the process dies rather
		// than the request failing. A few MB of open tags is enough.
		err := xml.Unmarshal([]byte(nestedXML(maxXMLDepth*2)), &xmlMap{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "xml nesting exceeds the maximum depth")
	})

	t.Run("should accept a body nested up to the depth limit", func(t *testing.T) {
		// The limit sits far above anything a real API returns, so it must not
		// turn deep-but-legitimate documents into errors.
		xm := &xmlMap{}
		require.NoError(t, xml.Unmarshal([]byte(nestedXML(maxXMLDepth)), xm))
		require.NotNil(t, xm.data)
	})
}

// nestedXML builds a document of depth levels of the same element, with text at
// the centre: <a><a>…x…</a></a>.
func nestedXML(depth int) string {
	var b strings.Builder
	b.Grow(depth*7 + 1)
	for range depth {
		_, _ = b.WriteString("<a>")
	}
	_, _ = b.WriteString("x")
	for range depth {
		_, _ = b.WriteString("</a>")
	}
	return b.String()
}
