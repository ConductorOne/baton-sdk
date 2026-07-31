package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
)

func TestTraceOracleRejectsPlantedControlViolation(t *testing.T) {
	events := []chaosconnector.TraceEvent{
		{
			Operation: chaosconnector.Operation{
				Domain:  chaosconnector.DomainConnector,
				Service: "ResourcesService",
				Method:  "ListResources",
				Attempt: 1,
			},
			Outcome: chaosconnector.OutcomeErrored,
		},
	}
	expectation := TraceExpectation{
		Name: "retry happened",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("ResourcesService"),
			Method:  chaosconnector.ExactString("ListResources"),
		},
		Outcomes: []chaosconnector.Outcome{
			chaosconnector.OutcomeReturned,
			chaosconnector.OutcomeErrored,
		},
		Min: 2,
	}
	require.ErrorContains(t, VerifyTrace(events, expectation), "retry happened")

	events = append(events, chaosconnector.TraceEvent{
		Operation: chaosconnector.Operation{
			Domain:  chaosconnector.DomainConnector,
			Service: "ResourcesService",
			Method:  "ListResources",
			Attempt: 2,
		},
		Outcome: chaosconnector.OutcomeReturned,
	})
	require.NoError(t, VerifyTrace(events, expectation))
}
