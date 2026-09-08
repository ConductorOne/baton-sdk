package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// The golden token fixtures and their expected values are frozen
// (token_golden_test.go, testdata/tokens). This file is the only part that
// names the codec's entry points, so reorganizing the codec touches it and
// not the bytes or the expectations.

// goldenDecodeEncode decodes a token and re-encodes it. inlineGraph selects
// the writer option that serializes the entitlement graph into the token;
// it is a process-level setting, not a token field, so the caller supplies
// it rather than the input carrying it.
func goldenDecodeEncode(input string, inlineGraph bool) (string, error) {
	st := newState(withCheckpointEntitlementGraph(inlineGraph))
	if err := st.Unmarshal(input); err != nil {
		return "", err
	}
	return st.Marshal()
}

// goldenEncodeTwice decodes a token once and encodes the decoded value
// twice.
func goldenEncodeTwice(input string, inlineGraph bool) (string, string, error) {
	st := newState(withCheckpointEntitlementGraph(inlineGraph))
	if err := st.Unmarshal(input); err != nil {
		return "", "", err
	}
	first, err := st.Marshal()
	if err != nil {
		return "", "", err
	}
	second, err := st.Marshal()
	if err != nil {
		return "", "", err
	}
	return first, second, nil
}
