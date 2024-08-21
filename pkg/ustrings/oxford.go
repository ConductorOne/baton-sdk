package ustrings

import (
	"fmt"
	"strings"
)

const (
	ConjunctionDefault      = "and"
	SeparatorDefault        = ","
	WrappersDefault         = Empty
	EmptyListMessageDefault = ""
)

type OxfordConfigs struct {
	conjunction      string
	separator        string
	wrappersInner    WrapOption
	wrappersOuter    WrapOption
	emptyListMessage string
}

type OxfordOptions func(config *OxfordConfigs)

func WithConjunction(conjunction string) OxfordOptions {
	return func(config *OxfordConfigs) {
		config.conjunction = conjunction
	}
}

func WithSeparator(separator string) OxfordOptions {
	return func(config *OxfordConfigs) {
		config.separator = separator
	}
}

func WithInnerWrappers(wrappers WrapOption) OxfordOptions {
	return func(config *OxfordConfigs) {
		config.wrappersInner = wrappers
	}
}

func WithOuterWrappers(wrappers WrapOption) OxfordOptions {
	return func(config *OxfordConfigs) {
		config.wrappersOuter = wrappers
	}
}

func WithEmptyListMessage(message string) OxfordOptions {
	return func(config *OxfordConfigs) {
		config.emptyListMessage = message
	}
}

func OxfordizeList(elements []string, options ...OxfordOptions) string {
	configs := OxfordConfigs{
		conjunction:      ConjunctionDefault,
		separator:        SeparatorDefault,
		wrappersInner:    WrappersDefault,
		wrappersOuter:    WrappersDefault,
		emptyListMessage: EmptyListMessageDefault,
	}
	for _, option := range options {
		option(&configs)
	}

	wrapped := make([]string, 0)
	for _, element := range elements {
		wrapped = append(
			wrapped,
			WrapString(
				element,
				configs.wrappersInner,
			),
		)
	}

	last := len(elements) - 1
	var output string
	switch len(elements) {
	case 0:
		output = configs.emptyListMessage
	case 1:
		output = wrapped[0]
	case 2:
		output = fmt.Sprintf(
			"%s %s %s",
			wrapped[0],
			configs.conjunction,
			wrapped[last],
		)
	default:
		wrapped[last] = fmt.Sprintf(
			"%s %s",
			configs.conjunction,
			wrapped[last],
		)
		output = strings.Join(
			wrapped,
			fmt.Sprintf(
				"%s ",
				configs.separator,
			),
		)
	}
	return WrapString(output, configs.wrappersOuter)
}
