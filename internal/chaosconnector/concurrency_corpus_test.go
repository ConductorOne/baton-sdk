package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestConcurrentDuplicateCorpusCoversBothOrders(t *testing.T) {
	corpus := ConcurrentDuplicateCorpus()
	require.Len(t, corpus, 6)
	ordersByEntity := make(map[ReferentialEntity][]string)
	for _, corpusCase := range corpus {
		ordersByEntity[corpusCase.Entity] = append(
			ordersByEntity[corpusCase.Entity],
			corpusCase.BlockedToken,
		)
		require.NotEqual(t, corpusCase.BlockedToken, corpusCase.FirstToken)
		require.NotEmpty(t, corpusCase.Method())
		require.NotEmpty(t, corpusCase.Expectation(corpusCase.BlockedToken).CanonicalIdentity)
		require.NoError(t, corpusCase.Schedule.Validate())
		require.NoError(t, corpusCase.InterruptSchedule.Validate())

		scenario, err := NewConcurrentDuplicateScenario(corpusCase.Entity)
		require.NoError(t, err)
		require.NoError(t, scenario.Validate())
		if corpusCase.Entity == ReferentialResource {
			dataset := scenario.Epochs[scenario.InitialEpoch]
			for _, token := range []string{"left", "right"} {
				parentID := v2.ResourceId_builder{
					ResourceType: concurrentParentResourceTypeID,
					Resource:     concurrentResourceParentID(token),
				}.Build()
				page := dataset.Resources[resourcePageScope(FullCapabilityResourceTypeID, parentID)][""]
				require.Len(t, page.List, 1)
				require.Equal(t, FullCapabilityResourceTypeID, page.List[0].GetId().GetResourceType(),
					"parent-scoped response must match the requested child type")
			}
		}
	}
	for _, entity := range []ReferentialEntity{
		ReferentialResource,
		ReferentialEntitlement,
		ReferentialGrant,
	} {
		require.ElementsMatch(t, []string{"left", "right"}, ordersByEntity[entity])
	}
}
