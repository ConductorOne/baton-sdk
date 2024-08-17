package ustrings

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOxfordizeList(t *testing.T) {
	t.Run("with default configs", func(t *testing.T) {
		testCases := []struct {
			expression string
			expected   string
			message    string
		}{
			{"", "", "empty"},
			{"a", "a", "single"},
			{"a b", "a and b", "double"},
			{"a b c", "a, b, and c", "triple"},
		}
		for _, testCase := range testCases {
			t.Run(testCase.message, func(t *testing.T) {
				parts := strings.Split(testCase.expression, " ")
				actual := OxfordizeList(parts)
				require.Equal(t, testCase.expected, actual)
			})
		}
	})

	t.Run("should display empty message", func(t *testing.T) {
		actual := OxfordizeList(
			[]string{},
			WithEmptyListMessage("<empty>"),
		)
		require.Equal(t, "<empty>", actual)
	})

	t.Run("with custom configs", func(t *testing.T) {
		parts := []string{"a", "b", "c"}

		t.Run("should swap conjunction", func(t *testing.T) {
			actual := OxfordizeList(parts, WithConjunction("or"))
			require.Equal(t, "a, b, or c", actual)
		})

		t.Run("should swap separator", func(t *testing.T) {
			actual := OxfordizeList(parts, WithSeparator(";"))
			require.Equal(t, "a; b; and c", actual)
		})

		t.Run("should swap wrappers", func(t *testing.T) {
			actual := OxfordizeList(
				parts,
				WithInnerWrappers(SingleQuotes),
				WithOuterWrappers(Parentheses),
			)
			require.Equal(t, "('a', 'b', and 'c')", actual)
		})
	})
}
