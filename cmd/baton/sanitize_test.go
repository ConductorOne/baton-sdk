package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// Exercises the relocated `baton sanitize` subcommand end-to-end: build
// a small source c1z, run the command through the same cobra root that
// owns the persistent --file flag, and confirm it produces a sanitized
// output whose grant ids no longer match the source verbatim.
func TestSanitizeCommand(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")

	src, err := dotc1z.NewC1ZFile(ctx, srcPath)
	require.NoError(t, err)
	_, err = src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	groupRes := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "eng"}.Build(), DisplayName: "Engineering"}.Build()
	userRes := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(), DisplayName: "Alice"}.Build()
	require.NoError(t, src.PutResources(ctx, groupRes, userRes))
	ent := v2.Entitlement_builder{Id: "group:eng:member", DisplayName: "Member", Resource: groupRes, Slug: "member", Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
	require.NoError(t, src.PutEntitlements(ctx, ent))
	const srcGrantID = "group:eng:member:user:alice"
	require.NoError(t, src.PutGrants(ctx, v2.Grant_builder{Id: srcGrantID, Entitlement: ent, Principal: userRes}.Build()))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	root := &cobra.Command{Use: "baton"}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "")
	root.AddCommand(sanitizeCmd())
	root.SetArgs([]string{"sanitize", "--file", srcPath, "--out", outPath})
	require.NoError(t, root.ExecuteContext(ctx))

	require.FileExists(t, outPath)
	require.FileExists(t, outPath+".secret", "a fresh secret should be written next to --out")

	out, err := dotc1z.NewC1ZFile(ctx, outPath, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer out.Close(ctx)

	resp, err := out.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "grant count must be preserved")
	require.NotEqual(t, srcGrantID, resp.GetList()[0].GetId(), "grant id must be sanitized, not passed through verbatim")
}
