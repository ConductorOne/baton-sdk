package dotc1z

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseFinalizeTimeout(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want time.Duration
	}{
		{name: "empty falls back to default", env: "", want: DefaultFinalizeTimeout},
		{name: "garbage falls back to default", env: "not-a-number", want: DefaultFinalizeTimeout},
		{name: "zero falls back to default", env: "0", want: DefaultFinalizeTimeout},
		{name: "negative falls back to default", env: "-30", want: DefaultFinalizeTimeout},
		{name: "valid seconds parses through", env: "1800", want: 30 * time.Minute},
		{name: "single-second value parses through", env: "1", want: 1 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, parseFinalizeTimeout(tc.env))
		})
	}
}
