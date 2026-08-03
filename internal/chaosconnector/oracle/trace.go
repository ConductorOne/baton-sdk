package oracle

import (
	"errors"
	"fmt"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
)

// TraceExpectation is an independent control/evidence oracle over observed
// connector operations.
type TraceExpectation struct {
	Name     string
	Match    chaosconnector.Matcher
	Outcomes []chaosconnector.Outcome
	Min      int
	Max      int
}

// VerifyTrace checks call and outcome obligations. Max zero is unbounded.
func VerifyTrace(events []chaosconnector.TraceEvent, expectations ...TraceExpectation) error {
	var failures []error
	for _, expectation := range expectations {
		count := 0
		for _, event := range events {
			if !expectation.Match.Matches(event.Operation) ||
				!containsOutcome(expectation.Outcomes, event.Outcome) {
				continue
			}
			count++
		}
		if count < expectation.Min {
			failures = append(failures, fmt.Errorf(
				"chaos oracle: trace expectation %q observed %d, requires at least %d",
				expectation.Name,
				count,
				expectation.Min,
			))
		}
		if expectation.Max > 0 && count > expectation.Max {
			failures = append(failures, fmt.Errorf(
				"chaos oracle: trace expectation %q observed %d, allows at most %d",
				expectation.Name,
				count,
				expectation.Max,
			))
		}
	}
	return errors.Join(failures...)
}

func containsOutcome(allowed []chaosconnector.Outcome, actual chaosconnector.Outcome) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, outcome := range allowed {
		if outcome == actual {
			return true
		}
	}
	return false
}
