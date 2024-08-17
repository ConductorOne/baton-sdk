package ustrings

import (
	"fmt"
	libStr "strings"
)

const (
	ConjunctionDefault = "and"
	SeparatorDefault   = ","
	WrappersDefault    = Empty
)

type OxfordConfigs struct {
	conjunction   string
	separator     string
	wrappersInner WrapOption
	wrappersOuter WrapOption
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

func OxfordizeList(elements []string, options ...OxfordOptions) string {
	configs := OxfordConfigs{
		conjunction:   ConjunctionDefault,
		separator:     SeparatorDefault,
		wrappersInner: WrappersDefault,
		wrappersOuter: WrappersDefault,
	}
	for _, option := range options {
		option(&configs)
	}

	if len(elements) == 0 {
		return WrapString("", configs.wrappersOuter)
	}

	last := len(elements) - 1
	var rest []string
	var tail string
	for _, element := range elements[:last] {
		rest = append(
			rest,
			WrapString(
				element,
				configs.wrappersInner,
			),
		)
	}
	tail = WrapString(
		elements[last],
		configs.wrappersInner,
	)

	var output string
	switch len(rest) {
	case 0:
		output = tail
	case 1:
		output = fmt.Sprintf(
			"%s %s %s",
			rest[0],
			configs.conjunction,
			tail,
		)
	default:
		output = fmt.Sprintf(
			"%s%s %s %s",
			libStr.Join(
				rest,
				fmt.Sprintf(
					"%s ",
					configs.separator,
				),
			),
			configs.separator,
			configs.conjunction,
			tail,
		)
	}
	return WrapString(output, configs.wrappersOuter)
}
