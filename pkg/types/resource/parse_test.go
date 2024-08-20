package resource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHelpers_SplitFullName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected [2]string
	}{
		{
			"Single name",
			"Prince",
			[2]string{"Prince", ""},
		},
		{
			"First and last name",
			"John Smith",
			[2]string{"John", "Smith"},
		},
		{
			"Multiple names",
			"John Jacob Jingleheimer Schmidt",
			[2]string{"John", "Jacob Jingleheimer Schmidt"},
		},
		{
			"Empty string",
			"",
			[2]string{"", ""},
		},
		{
			"Multiple spaces",
			"John  Smith",
			[2]string{"John", " Smith"},
		},
		{
			"Starts with Space",
			" John Smith",
			[2]string{"", "John Smith"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			firstName, lastName := SplitFullName(tt.input)
			require.Equal(t, tt.expected[0], firstName)
			require.Equal(t, tt.expected[1], lastName)
		})
	}
}
