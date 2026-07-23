package uhttp

import (
	"encoding/xml"
	"strings"
)

// xmlMap implements xml.Unmarshaler and can unmarshal arbitrary XML into a
// map[string]any structure. Leaf elements become string values, and elements
// with children become nested maps.
type xmlMap struct {
	data any
}

func (x *xmlMap) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	result, err := unmarshalXMLElement(d, start.Attr)
	if err != nil {
		return err
	}
	x.data = result
	return nil
}

// attrMap converts xml.Attr entries into a map[string]any keyed by the
// attribute's local name (namespace prefixes are dropped), badgerfish-style.
// Returns nil if there are no attributes.
func attrMap(attrs []xml.Attr) map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]any, len(attrs))
	for _, a := range attrs {
		m[a.Name.Local] = a.Value
	}
	return m
}

// unmarshalXMLElement reads tokens from the decoder for the current element
// (after its start element has been consumed) until the matching end element.
// attrs holds the attributes of the current element itself (empty if none).
//
// It returns a map[string]any if there are child elements, a []map[string]any
// if there are duplicate child element names, or a string if the element
// contains only text and has no attributes.
//
// When the current element carries attributes (len(attrs) > 0), they are
// captured under an "@attributes" key (badgerfish/xml-js convention):
//   - element with children: "@attributes" is added as a sibling key in the
//     resulting map (purely additive; existing keys are unaffected).
//   - leaf element with attributes: the previously bare string value is
//     promoted to map[string]any{"#text": <trimmed text>, "@attributes": ...},
//     or just map[string]any{"@attributes": ...} if there is no text. This is
//     a deliberate type change (string -> map) at that path only.
//   - element with duplicate child names (returned as []map[string]any):
//     since the container itself has no single map to attach "@attributes"
//     to, the attribute map is added to every entry in the slice.
//
// Elements with no attributes are completely unaffected: behavior is
// byte-for-byte identical to before attribute support was added.
func unmarshalXMLElement(d *xml.Decoder, attrs []xml.Attr) (any, error) {
	type entry struct {
		key   string
		value any
	}
	var entries []entry
	seen := make(map[string]bool)
	hasDuplicates := false
	var charData strings.Builder

	selfAttrs := attrMap(attrs)

	for {
		t, err := d.Token()
		if err != nil {
			return nil, err
		}
		switch tt := t.(type) {
		case xml.StartElement:
			child, err := unmarshalXMLElement(d, tt.Attr)
			if err != nil {
				return nil, err
			}
			key := tt.Name.Local
			if seen[key] {
				hasDuplicates = true
			}
			seen[key] = true
			entries = append(entries, entry{key: key, value: child})
		case xml.CharData:
			_, err := charData.Write(tt)
			if err != nil {
				return nil, err
			}
		case xml.EndElement:
			if len(entries) == 0 {
				text := strings.TrimSpace(charData.String())
				if selfAttrs != nil {
					if text == "" {
						return map[string]any{"@attributes": selfAttrs}, nil
					}
					return map[string]any{"#text": text, "@attributes": selfAttrs}, nil
				}
				if text == "" {
					return make(map[string]any), nil
				}
				return text, nil
			}
			if hasDuplicates {
				result := make([]map[string]any, 0, len(entries))
				for _, e := range entries {
					m := map[string]any{e.key: e.value}
					if selfAttrs != nil {
						m["@attributes"] = selfAttrs
					}
					result = append(result, m)
				}
				return result, nil
			}
			result := make(map[string]any, len(entries))
			for _, e := range entries {
				result[e.key] = e.value
			}
			if selfAttrs != nil {
				result["@attributes"] = selfAttrs
			}
			return result, nil
		}
	}
}
