package pebble

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPageGrantIteratorMatchesCommittedSelection(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, _, _, base := pageTestV2Fixtures()
	grant := func(principal string) *v2.Grant {
		g := proto.Clone(base).(*v2.Grant)
		g.SetPrincipal(v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: principal}.Build()}.Build())
		return g
	}
	require.NoError(t, e.PutGrants(t.Context(), grant("a"), grant("c"), grant("e")))
	writer := e.Ledger().BeginPage()
	defer writer.Discard()
	replacement := grant("c")
	replacement.SetAnnotations(annotations.New(v2.GrantExpandable_builder{EntitlementIds: []string{"app:github:member"}}.Build()))
	require.NoError(t, writer.PutGrants(t.Context(), grant("b"), grant("c"), replacement, grant("d")))
	require.NoError(t, writer.DeleteGrants(t.Context(), grant("a"), grant("d")))
	var before []c1zstore.GrantAnnotation
	for row, err := range writer.ListGrantsWithAnnotations(t.Context()) {
		require.NoError(t, err)
		before = append(before, row)
	}
	require.Len(t, before, 3)
	require.Equal(t, "b", before[0].PrincipalResourceID)
	require.Equal(t, "c", before[1].PrincipalResourceID)
	require.NotNil(t, before[1].Annotation)
	require.Equal(t, "e", before[2].PrincipalResourceID)
	id := grantsPageIdentity("iterator", "")
	require.NoError(t, writer.Commit(t.Context(), id, &c1zstore.LedgerRow{}))
	var after []c1zstore.GrantAnnotation
	for row, err := range e.Grants().ListWithAnnotations(t.Context()) {
		require.NoError(t, err)
		after = append(after, row)
	}
	require.Len(t, after, len(before))
	for i := range before {
		require.True(t, proto.Equal(before[i].Grant, after[i].Grant))
		require.True(t, proto.Equal(before[i].Annotation, after[i].Annotation))
	}
}
