package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// TestIngestInvariantAnnotationCoverage is the completeness meta-test for
// the ingestion-invariant registry: every connector annotation known to
// imply a syncer side effect must map to the mechanism that guarantees it
// regardless of ingestion path (stream, replay, or whatever comes next).
// Adding a new side-effect-implying annotation without extending
// sideEffectAnnotationCoverage — and the invariant behind it — fails
// here instead of in review round eleven.
func TestIngestInvariantAnnotationCoverage(t *testing.T) {
	sideEffectAnnotations := []proto.Message{
		&v2.GrantExpandable{},
		&v2.ExternalResourceMatch{},
		&v2.ExternalResourceMatchAll{},
		&v2.ExternalResourceMatchID{},
		&v2.InsertResourceGrants{},
		&v2.ChildResourceType{},
		&v2.EntitlementExclusionGroup{},
	}
	for _, msg := range sideEffectAnnotations {
		name := string(msg.ProtoReflect().Descriptor().FullName())
		require.Contains(t, sideEffectAnnotationCoverage, name,
			"side-effect-implying annotation %s has no registered ingestion invariant; "+
				"see docs/tasks/source-cache-ingestion-invariants.md", name)
	}
	require.Len(t, sideEffectAnnotationCoverage, len(sideEffectAnnotations),
		"coverage map and the enumerated annotation set drifted; update both together")
}
