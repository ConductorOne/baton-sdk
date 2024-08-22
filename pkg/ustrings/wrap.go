package ustrings

import (
	"fmt"
	"strings"
)

type WrapOption int

const (
	Empty WrapOption = iota
	SingleQuotes
	DoubleQuotes
	TackQuotes
	Parentheses
	SquareBrackets
	AngleBrackets
)

const (
	quoteOptions = "'\"`"
)

// TrimQuotes - given an input string, remove one layer of surrounding quotes if
// they exist.
func TrimQuotes(input string) string {
	if len(input) <= 1 {
		return input
	}
	for _, char := range strings.Split(quoteOptions, "") {
		if strings.HasPrefix(input, char) &&
			strings.HasSuffix(input, char) {
			return input[1 : len(input)-1]
		}
	}
	return input
}

func WrapString(input string, wrappers WrapOption) string {
	switch wrappers {
	case SingleQuotes:
		return fmt.Sprintf("'%s'", input)
	case DoubleQuotes:
		return fmt.Sprintf(`"%s"`, input)
	case TackQuotes:
		return fmt.Sprintf("`%s`", input)
	case Parentheses:
		return fmt.Sprintf("(%s)", input)
	case SquareBrackets:
		return fmt.Sprintf("[%s]", input)
	case AngleBrackets:
		return fmt.Sprintf("<%s>", input)
	default:
		return input
	}
}
