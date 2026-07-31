package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExternalPrincipalCorpusIsNamedAndBuildable(t *testing.T) {
	seen := make(map[string]struct{})
	for _, corpusCase := range ExternalPrincipalCorpus() {
		require.NotEmpty(t, corpusCase.Name)
		_, duplicate := seen[corpusCase.Name]
		require.False(t, duplicate, "duplicate corpus case %q", corpusCase.Name)
		seen[corpusCase.Name] = struct{}{}

		external, internal, err := corpusCase.Build()
		require.NoError(t, err, corpusCase.Name)
		require.NoError(t, external.Validate(), corpusCase.Name)
		require.NoError(t, internal.Validate(), corpusCase.Name)
	}
}

func TestExternalPrincipalCorpusCoversMatchClasses(t *testing.T) {
	seen := make(map[ExternalMatchKind]bool)
	for _, corpusCase := range ExternalPrincipalCorpus() {
		seen[corpusCase.Match] = true
	}
	require.True(t, seen[ExternalMatchAll])
	require.True(t, seen[ExternalMatchID])
	require.True(t, seen[ExternalMatchAttribute])
}
