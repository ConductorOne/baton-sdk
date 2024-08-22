package ustrings

import (
	"fmt"
	"strings"
)

const (
	flagSeparator     = " "
	flagPrefix        = "--"
	flagListSeparator = ","
	flagDefaultValue  = "true"
)

func handleEmptyFlag(currentFlag string, output map[string]string) error {
	if currentFlag != "" {
		if _, ok := output[currentFlag]; ok {
			return fmt.Errorf("duplicate flag has no value: %s", currentFlag)
		}
		output[currentFlag] = flagDefaultValue
	}
	return nil
}

// ParseFlags - convert command line flags to a map of keys to strings.
func ParseFlags(expression string) (map[string]string, error) {
	output := make(map[string]string)
	trimmed := strings.TrimSpace(expression)
	if trimmed == "" {
		return output, nil
	}
	tokens := strings.Split(trimmed, flagSeparator)
	currentFlag := ""
	for _, token := range tokens {
		if strings.HasPrefix(token, flagPrefix) {
			err := handleEmptyFlag(currentFlag, output)
			if err != nil {
				return nil, err
			}
			currentFlag = strings.TrimPrefix(token, flagPrefix)
		} else {
			if currentFlag == "" {
				return nil, fmt.Errorf("got a value without a flag: %s", token)
			}
			toAdd := TrimQuotes(token)
			if found, ok := output[currentFlag]; ok {
				toAdd = found + flagListSeparator + token
			}
			output[currentFlag] = toAdd
			currentFlag = ""
		}
	}
	// Clean up final flag if it exists.
	err := handleEmptyFlag(currentFlag, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}
