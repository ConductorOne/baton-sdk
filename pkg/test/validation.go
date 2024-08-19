package test

import "testing"

// AssertValidation - call an arbitrary validation function and assert the result
// matches the expected result.
func AssertValidation(
	t *testing.T,
	validate func() error,
	expectedSuccess bool,
) {
	err := validate()
	if err != nil {
		if expectedSuccess {
			t.Fatal("expected function to succeed, but", err.Error())
		}
	} else {
		if !expectedSuccess {
			t.Fatal("expected function to fail, but it succeeded")
		}
	}
}
