package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// The golden token fixtures and their expected values are frozen
// (token_golden_test.go, testdata/tokens). This file is the only part that
// names the codec's entry points, so reorganizing the codec touches it and
// not the bytes or the expectations.

// goldenDecodeEncode decodes a token and re-encodes it.
func goldenDecodeEncode(input string) (string, error) {
	parts, err := unmarshalToken(input)
	if err != nil {
		return "", err
	}
	return marshalToken(parts.run, parts.stats)
}

// goldenEncodeTwice decodes a token once and encodes the decoded value
// twice.
func goldenEncodeTwice(input string) (string, string, error) {
	parts, err := unmarshalToken(input)
	if err != nil {
		return "", "", err
	}
	first, err := marshalToken(parts.run, parts.stats)
	if err != nil {
		return "", "", err
	}
	second, err := marshalToken(parts.run, parts.stats)
	if err != nil {
		return "", "", err
	}
	return first, second, nil
}
