package uhttp

import (
	"encoding/xml"
	"fmt"
	"strings"
)

// maxXMLDepth bounds how deep unmarshalXMLElement will recurse. Without it, a
// body of a few MB of nested open tags exhausts the goroutine stack, and a Go
// stack overflow is a fatal runtime error rather than a panic, so recover()
// cannot turn it back into a failed request: the process dies mid-sync.
// Measured on the 1 GB default stack limit, ~1M levels is fatal and ~500k is
// not, which a 7 MB response reaches.
//
// The limit only has to sit below that ceiling while staying clear of real
// documents, which nest tens of levels at most. encoding/json caps nesting at
// 10000 for the same reason, so this matches the standard library.
const maxXMLDepth = 10_000

// xmlMap implements xml.Unmarshaler and can unmarshal arbitrary XML into a
// map[string]any structure. Leaf elements become string values, and elements
// with children become nested maps.
// The root element's own name is recorded but not used as a key, so a document's
// paths start at its root's children. Callers need it only when the root content
// has no map representation and has to be keyed by something.
type xmlMap struct {
	data any
	root string
}

func (x *xmlMap) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	result, err := unmarshalXMLElement(d, 0)
	if err != nil {
		return err
	}
	x.data = result
	x.root = start.Name.Local
	return nil
}

// unmarshalXMLElement reads tokens from the decoder for the current element
// (after its start element has been consumed) until the matching end element.
// It returns a map[string]any if there are child elements, a []map[string]any
// if there are duplicate child element names, or a string if the element
// contains only text.
//
// depth is the caller's nesting level, checked against maxXMLDepth so a
// pathologically nested body fails as a request error instead of taking the
// process down with a stack overflow.
func unmarshalXMLElement(d *xml.Decoder, depth int) (any, error) {
	if depth > maxXMLDepth {
		return nil, fmt.Errorf("xml nesting exceeds the maximum depth of %d", maxXMLDepth)
	}

	type entry struct {
		key   string
		value any
	}
	var entries []entry
	seen := make(map[string]bool)
	hasDuplicates := false
	var charData strings.Builder

	for {
		t, err := d.Token()
		if err != nil {
			return nil, err
		}
		switch tt := t.(type) {
		case xml.StartElement:
			child, err := unmarshalXMLElement(d, depth+1)
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
				if text == "" {
					return make(map[string]any), nil
				}
				return text, nil
			}
			if hasDuplicates {
				result := make([]map[string]any, 0, len(entries))
				for _, e := range entries {
					result = append(result, map[string]any{e.key: e.value})
				}
				return result, nil
			}
			result := make(map[string]any, len(entries))
			for _, e := range entries {
				result[e.key] = e.value
			}
			return result, nil
		}
	}
}
