package secretcontracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// parseDocument reads one JSON object. Unlike encoding/json it rejects a key
// that appears twice (compared after unescaping), an escaped lone surrogate,
// invalid UTF-8 and bytes after the object. Values are string, json.Number,
// bool, nil, []any or map[string]any.
func parseDocument(raw []byte) (map[string]any, *Error) {
	if !utf8.Valid(raw) {
		return nil, newError(LayerDocument, "invalid_utf8", "")
	}
	if hasLoneSurrogateEscape(raw) {
		return nil, newError(LayerDocument, "invalid_json", "")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return nil, newError(LayerDocument, "invalid_json", "")
	}
	if token != json.Delim('{') {
		return nil, newError(LayerDocument, "not_an_object", "")
	}
	object, perr := readObject(decoder)
	if perr != nil {
		return nil, perr
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, newError(LayerDocument, "invalid_json", "")
	}
	return object, nil
}

func readObject(decoder *json.Decoder) (map[string]any, *Error) {
	object := map[string]any{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, newError(LayerDocument, "invalid_json", "")
		}
		key, ok := token.(string)
		if !ok {
			return nil, newError(LayerDocument, "invalid_json", "")
		}
		if _, duplicate := object[key]; duplicate {
			return nil, newError(LayerDocument, "duplicate_key", key)
		}
		value, perr := readValue(decoder)
		if perr != nil {
			return nil, perr
		}
		object[key] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, newError(LayerDocument, "invalid_json", "")
	}
	return object, nil
}

func readValue(decoder *json.Decoder) (any, *Error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, newError(LayerDocument, "invalid_json", "")
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return token, nil
	}
	switch delim {
	case '{':
		return readObject(decoder)
	case '[':
		items := []any{}
		for decoder.More() {
			item, perr := readValue(decoder)
			if perr != nil {
				return nil, perr
			}
			items = append(items, item)
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return nil, newError(LayerDocument, "invalid_json", "")
		}
		return items, nil
	default:
		return nil, newError(LayerDocument, "invalid_json", "")
	}
}

// hasLoneSurrogateEscape reports a \uD800-\uDFFF escape inside a string that
// is not half of a high-then-low pair. encoding/json would replace it with
// U+FFFD; the receiver's parser rejects it.
func hasLoneSurrogateEscape(raw []byte) bool {
	inString := false
	for i := 0; i < len(raw); i++ {
		switch c := raw[i]; {
		case !inString:
			inString = c == '"'
		case c == '"':
			inString = false
		case c == '\\' && i+1 < len(raw):
			if raw[i+1] != 'u' {
				i++
				continue
			}
			unit, ok := hex4(raw, i+2)
			if !ok {
				return false
			}
			i += 5
			switch {
			case unit >= 0xDC00 && unit <= 0xDFFF:
				return true
			case unit >= 0xD800 && unit <= 0xDBFF:
				low, ok := uint16(0), false
				if i+6 < len(raw) && raw[i+1] == '\\' && raw[i+2] == 'u' {
					low, ok = hex4(raw, i+3)
				}
				if !ok || low < 0xDC00 || low > 0xDFFF {
					return true
				}
				i += 6
			}
		}
	}
	return false
}

func hex4(raw []byte, at int) (uint16, bool) {
	if at+4 > len(raw) {
		return 0, false
	}
	var unit uint16
	for _, c := range raw[at : at+4] {
		unit <<= 4
		switch {
		case c >= '0' && c <= '9':
			unit |= uint16(c - '0')
		case c >= 'a' && c <= 'f':
			unit |= uint16(c-'a') + 10
		case c >= 'A' && c <= 'F':
			unit |= uint16(c-'A') + 10
		default:
			return 0, false
		}
	}
	return unit, true
}
