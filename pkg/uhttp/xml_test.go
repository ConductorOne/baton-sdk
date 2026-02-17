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
}
