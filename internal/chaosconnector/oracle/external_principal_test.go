package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareExternalPrincipalRejectsPlantedViolations(t *testing.T) {
	expected := ExternalPrincipalExpectation{
		PrincipalIDs:  []string{"u1", "u2"},
		RequireSealed: true,
	}
	require.NoError(t, CompareExternalPrincipal(expected, ExternalPrincipalObservation{
		PrincipalIDs: []string{"u2", "u1"},
		Sealed:       true,
	}))
	require.Error(t, CompareExternalPrincipal(expected, ExternalPrincipalObservation{
		PrincipalIDs: []string{"u1"},
		Sealed:       true,
	}))
	require.Error(t, CompareExternalPrincipal(expected, ExternalPrincipalObservation{
		PrincipalIDs: []string{"u1", "u2"},
		CarrierCount: 1,
		Sealed:       true,
	}))
	require.Error(t, CompareExternalPrincipal(expected, ExternalPrincipalObservation{
		PrincipalIDs: []string{"u1", "u2"},
	}))
}
