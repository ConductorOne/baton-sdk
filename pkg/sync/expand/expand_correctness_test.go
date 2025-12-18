package expand

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func getTestdataPathWithSuffix(syncID, suffix string) string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "testdata", fmt.Sprintf("sync.%s.%s", syncID, suffix))
}

// grantKey creates a unique key for a grant based on entitlement and principal.
func grantKey(g *v2.Grant) string {
	return fmt.Sprintf("%s|%s|%s",
		g.GetEntitlement().GetId(),
		g.GetPrincipal().GetId().GetResourceType(),
		g.GetPrincipal().GetId().GetResource(),
	)
}

// grantInfo holds grant data for comparison.
type grantInfo struct {
	ID      string
	Sources map[string]bool // Set of source entitlement IDs
}

// loadAllGrants loads all grants from a c1z file into a map keyed by grantKey.
func loadAllGrants(ctx context.Context, c1f *dotc1z.C1File) (map[string]*grantInfo, error) {
	grants := make(map[string]*grantInfo)

	pageToken := ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return nil, err
		}

		for _, g := range resp.GetList() {
			key := grantKey(g)
			sources := make(map[string]bool)
			for sourceID := range g.GetSources().GetSources() {
				sources[sourceID] = true
			}
			grants[key] = &grantInfo{
				ID:      g.GetId(),
				Sources: sources,
			}
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	return grants, nil
}

func TestExpandCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow expansion correctness test in short mode")
	}

	testCases := []struct {
		name   string
		syncID string
	}{
		{
			name:   "Small",
			syncID: "36zfOAoWMxQdISbixanQCRBiX7E",
		},
		// {
		// 	name:   "SmallMedium",
		// 	syncID: "36zM46KKuaBq0wjSSvKh5o0350y",
		// },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			unexpandedPath := getTestdataPathWithSuffix(tc.syncID, "unexpanded")
			expectedPath := getTestdataPathWithSuffix(tc.syncID, "expanded")

			if _, err := os.Stat(unexpandedPath); os.IsNotExist(err) {
				t.Skipf("unexpanded testdata file not found: %s", unexpandedPath)
			}
			if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
				t.Skipf("expanded testdata file not found: %s", expectedPath)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			// Copy unexpanded file to temp location
			tmpFile, err := os.CreateTemp("", "test-expand-*.c1z")
			require.NoError(t, err)
			tmpPath := tmpFile.Name()
			tmpFile.Close()
			defer os.Remove(tmpPath)

			srcData, err := os.ReadFile(unexpandedPath)
			require.NoError(t, err)
			err = os.WriteFile(tmpPath, srcData, 0600)
			require.NoError(t, err)

			// Open the temp file and run expansion
			actualC1f, err := dotc1z.NewC1ZFile(ctx, tmpPath)
			require.NoError(t, err)
			defer actualC1f.Close()

			err = actualC1f.SetSyncID(ctx, tc.syncID)
			require.NoError(t, err)

			// Load graph and run expansion
			graph, err := loadEntitlementGraphFromC1Z(ctx, actualC1f)
			require.NoError(t, err)

			t.Logf("Graph loaded: %d nodes, %d edges", len(graph.Nodes), len(graph.Edges))

			expander := NewExpander(actualC1f, graph)
			err = expander.Run(ctx)
			require.NoError(t, err)

			// Open expected file
			expectedC1f, err := dotc1z.NewC1ZFile(ctx, expectedPath, dotc1z.WithReadOnly(true))
			require.NoError(t, err)
			defer expectedC1f.Close()

			err = expectedC1f.SetSyncID(ctx, tc.syncID)
			require.NoError(t, err)

			// Load grants from both
			actualGrants, err := loadAllGrants(ctx, actualC1f)
			require.NoError(t, err)

			expectedGrants, err := loadAllGrants(ctx, expectedC1f)
			require.NoError(t, err)

			t.Logf("Actual grants: %d, Expected grants: %d", len(actualGrants), len(expectedGrants))

			// Compare grant counts
			require.Equal(t, len(expectedGrants), len(actualGrants), "grant count mismatch")

			// Find missing and extra grants
			var missingGrants []string
			var extraGrants []string

			for key := range expectedGrants {
				if _, ok := actualGrants[key]; !ok {
					missingGrants = append(missingGrants, key)
				}
			}

			for key := range actualGrants {
				if _, ok := expectedGrants[key]; !ok {
					extraGrants = append(extraGrants, key)
				}
			}

			sort.Strings(missingGrants)
			sort.Strings(extraGrants)

			if len(missingGrants) > 0 {
				t.Errorf("Missing %d grants (first 10):", len(missingGrants))
				for i, key := range missingGrants {
					if i >= 10 {
						break
					}
					t.Errorf("  - %s", key)
				}
			}

			if len(extraGrants) > 0 {
				t.Errorf("Extra %d grants (first 10):", len(extraGrants))
				for i, key := range extraGrants {
					if i >= 10 {
						break
					}
					t.Errorf("  - %s", key)
				}
			}

			// Compare sources for matching grants
			var sourceMismatches []string
			for key, expected := range expectedGrants {
				actual, ok := actualGrants[key]
				if !ok {
					continue // Already reported as missing
				}

				// Compare sources
				if len(expected.Sources) != len(actual.Sources) {
					sourceMismatches = append(sourceMismatches,
						fmt.Sprintf("%s: expected %d sources, got %d", key, len(expected.Sources), len(actual.Sources)))
					continue
				}

				for sourceID := range expected.Sources {
					if !actual.Sources[sourceID] {
						sourceMismatches = append(sourceMismatches,
							fmt.Sprintf("%s: missing source %s", key, sourceID))
					}
				}

				for sourceID := range actual.Sources {
					if !expected.Sources[sourceID] {
						sourceMismatches = append(sourceMismatches,
							fmt.Sprintf("%s: extra source %s", key, sourceID))
					}
				}
			}

			sort.Strings(sourceMismatches)

			if len(sourceMismatches) > 0 {
				t.Errorf("Source mismatches (%d total, first 20):", len(sourceMismatches))
				for i, mismatch := range sourceMismatches {
					if i >= 20 {
						break
					}
					t.Errorf("  - %s", mismatch)
				}
			}

			require.Empty(t, missingGrants, "should have no missing grants")
			require.Empty(t, extraGrants, "should have no extra grants")
			require.Empty(t, sourceMismatches, "should have no source mismatches")
		})
	}
}
