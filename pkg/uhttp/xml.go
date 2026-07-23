package uhttp

import (
	"encoding/xml"
	"maps"
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
// Returns nil if there are no (application-level) attributes.
//
// Two adjustments are made before keys are assigned:
//
//   - Namespace-declaration pseudo-attributes are dropped entirely. Go's
//     encoding/xml decoder surfaces both the bare default-namespace form
//     (xmlns="uri", decoded as Attr{Name:{Space:"", Local:"xmlns"}, Value: uri})
//     and the prefixed form (xmlns:foo="uri", decoded as
//     Attr{Name:{Space:"xmlns", Local:"foo"}, Value: uri}) as ordinary
//     attributes. Neither represents application data, and virtually every
//     namespaced XML/SOAP document declares one on its root element even when
//     it carries no real attributes, so counting them would spuriously add an
//     "@attributes" key to otherwise-unaffected elements. Genuine namespaced
//     attributes (e.g. xsi:type) are unaffected by this filter: the decoder
//     resolves their Name.Space to the namespace URI itself, never to the
//     literal string "xmlns".
//   - If, after filtering, two or more attributes share the same local name
//     but come from different namespaces (a realistic SOAP/WS-* pattern, e.g.
//     xsi:type alongside a custom-namespace "type"), keying purely by local
//     name would silently drop all but the last one. When such a collision is
//     detected, only the colliding entries are disambiguated by qualifying
//     their key with their namespace ("<namespace>:<local>"); attributes with
//     a unique local name keep their bare local-name key.
func attrMap(attrs []xml.Attr) map[string]any {
	filtered := make([]xml.Attr, 0, len(attrs))
	for _, a := range attrs {
		if a.Name.Space == "xmlns" || a.Name.Local == "xmlns" {
			continue
		}
		filtered = append(filtered, a)
	}
	if len(filtered) == 0 {
		return nil
	}

	localCount := make(map[string]int, len(filtered))
	for _, a := range filtered {
		localCount[a.Name.Local]++
	}

	m := make(map[string]any, len(filtered))
	for _, a := range filtered {
		key := a.Name.Local
		if localCount[key] > 1 && a.Name.Space != "" {
			key = a.Name.Space + ":" + a.Name.Local
		}
		m[key] = a.Value
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
// Elements with no application-level attributes are completely unaffected:
// behavior is byte-for-byte identical to before attribute support was added.
// This includes elements that carry only namespace declarations (xmlns="..."
// or xmlns:prefix="...") and no other attributes -- see attrMap.
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
						// Clone per entry so each sibling map owns an
						// independent "@attributes" map; without this, all
						// entries would alias the same underlying map and a
						// mutation to one sibling's attributes would silently
						// appear on every other sibling.
						m["@attributes"] = maps.Clone(selfAttrs)
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
