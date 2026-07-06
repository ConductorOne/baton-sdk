package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/c1zsanitize"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
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

	src, err := dotc1z.NewStore(ctx, srcPath)
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

	out, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer out.Close(ctx)

	resp, err := out.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "grant count must be preserved")
	require.NotEqual(t, srcGrantID, resp.GetList()[0].GetId(), "grant id must be sanitized, not passed through verbatim")
}

// TestSanitizeCommandPebbleEngine exercises engine selection end-to-end: a
// Pebble source defaults the output to Pebble, and the reopened output carries
// every entity kind plus assets and the needs_expansion index. A second run
// with --out-engine sqlite forces a SQLite output from the same Pebble source.
func TestSanitizeCommandPebbleEngine(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	outPath := filepath.Join(tmp, "out.c1z")
	secretPath := filepath.Join(tmp, "secret")

	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = byte(i + 1)
	}
	require.NoError(t, os.WriteFile(secretPath, secret, 0o600))

	const assetID = "icon-1"
	srcIcon := []byte{0x11, 0x22, 0x33, 0x44}

	src, err := dotc1z.NewStore(ctx, srcPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	require.NoError(t, src.PutAsset(ctx, v2.AssetRef_builder{Id: assetID}.Build(), "image/png", srcIcon))
	userRes := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		DisplayName: "Alice",
		Annotations: annotations.New(v2.UserTrait_builder{Login: "alice@acme.com", Icon: v2.AssetRef_builder{Id: assetID}.Build()}.Build()),
	}.Build()
	require.NoError(t, src.PutResources(ctx, userRes))
	ent := v2.Entitlement_builder{Id: "user:alice:self", DisplayName: "Self", Resource: userRes, Slug: "self"}.Build()
	require.NoError(t, src.PutEntitlements(ctx, ent))
	require.NoError(t, src.PutGrants(ctx,
		v2.Grant_builder{
			Id: "g-expand", Entitlement: ent, Principal: userRes,
			Annotations: annotations.New(v2.GrantExpandable_builder{EntitlementIds: []string{"user:alice:self"}}.Build()),
		}.Build(),
	))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	root := &cobra.Command{Use: "baton"}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "")
	root.AddCommand(sanitizeCmd())
	root.SetArgs([]string{"sanitize", "--file", srcPath, "--out", outPath, "--secret-file", secretPath})
	require.NoError(t, root.ExecuteContext(ctx))
	require.FileExists(t, outPath)

	// Default output engine follows the Pebble source.
	out, err := openReadOnlyC1ZStore(ctx, outPath)
	require.NoError(t, err)
	defer out.Close(ctx)
	require.Equal(t, string(c1zstore.EnginePebble), out.Metadata().Engine, "output engine must default to the source (pebble)")

	rtResp, err := out.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 1)
	resResp, err := out.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, resResp.GetList(), 1)
	entResp, err := out.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, entResp.GetList(), 1)
	grantResp, err := out.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, grantResp.GetList(), 1)

	// needs_expansion index survived.
	pending := 0
	for _, err := range out.Grants().PendingExpansion(ctx) {
		require.NoError(t, err)
		pending++
	}
	require.Equal(t, 1, pending, "the expandable grant must remain in the needs_expansion index")

	// The asset reads back under its transformed id, stripped to a placeholder.
	dstAssetID := c1zsanitize.SanitizeID(secret, assetID)
	ct, r, err := out.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{Asset: v2.AssetRef_builder{Id: dstAssetID}.Build()}.Build())
	require.NoError(t, err, "the sanitized asset must be readable under its transformed id")
	assetBytes, err := io.ReadAll(r)
	require.NoError(t, err)
	if c, ok := r.(io.Closer); ok {
		_ = c.Close()
	}
	require.Equal(t, "image/png", ct)
	require.NotEqual(t, srcIcon, assetBytes, "asset payload must be stripped, not the source bytes")

	// --out-engine sqlite forces a SQLite output from the same Pebble source.
	outSQLite := filepath.Join(tmp, "out-sqlite.c1z")
	root2 := &cobra.Command{Use: "baton"}
	root2.PersistentFlags().StringP("file", "f", "sync.c1z", "")
	root2.AddCommand(sanitizeCmd())
	root2.SetArgs([]string{"sanitize", "--file", srcPath, "--out", outSQLite, "--secret-file", secretPath, "--out-engine", "sqlite"})
	require.NoError(t, root2.ExecuteContext(ctx))
	outS, err := openReadOnlyC1ZStore(ctx, outSQLite)
	require.NoError(t, err)
	defer outS.Close(ctx)
	require.Equal(t, string(c1zstore.EngineSQLite), outS.Metadata().Engine, "--out-engine sqlite must force a sqlite output")
}

// newSanitizeRoot builds the cobra root the same way main.go wires it:
// the persistent --file flag lives on the root, sanitize is a child.
func newSanitizeRoot(args ...string) *cobra.Command {
	root := &cobra.Command{Use: "baton", SilenceUsage: true, SilenceErrors: true}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "")
	root.AddCommand(sanitizeCmd())
	root.SetArgs(append([]string{"sanitize"}, args...))
	return root
}

// Failed invocations must not leave a stray generated secret behind:
// every input/output validation runs before the secret is created.
func TestSanitizeCommandFailedInvocationLeavesNoSecret(t *testing.T) {
	ctx := context.Background()

	t.Run("missing input file", func(t *testing.T) {
		tmp := t.TempDir()
		outPath := filepath.Join(tmp, "out.c1z")
		root := newSanitizeRoot("--file", filepath.Join(tmp, "nope.c1z"), "--out", outPath)
		require.Error(t, root.ExecuteContext(ctx))
		require.NoFileExists(t, outPath+".secret", "failed run must not leave a generated secret")
		require.NoFileExists(t, outPath)
	})

	t.Run("output already exists", func(t *testing.T) {
		tmp := t.TempDir()
		srcPath := filepath.Join(tmp, "src.c1z")
		outPath := filepath.Join(tmp, "out.c1z")
		require.NoError(t, os.WriteFile(srcPath, []byte("x"), 0o600))
		require.NoError(t, os.WriteFile(outPath, []byte("y"), 0o600))
		root := newSanitizeRoot("--file", srcPath, "--out", outPath)
		require.Error(t, root.ExecuteContext(ctx))
		require.NoFileExists(t, outPath+".secret", "failed run must not leave a generated secret")
	})

	t.Run("bad anchor", func(t *testing.T) {
		tmp := t.TempDir()
		srcPath := filepath.Join(tmp, "src.c1z")
		outPath := filepath.Join(tmp, "out.c1z")
		require.NoError(t, os.WriteFile(srcPath, []byte("x"), 0o600))
		root := newSanitizeRoot("--file", srcPath, "--out", outPath, "--anchor", "not-a-time")
		require.Error(t, root.ExecuteContext(ctx))
		require.NoFileExists(t, outPath+".secret", "failed run must not leave a generated secret")
	})

	t.Run("missing out flag", func(t *testing.T) {
		tmp := t.TempDir()
		srcPath := filepath.Join(tmp, "src.c1z")
		require.NoError(t, os.WriteFile(srcPath, []byte("x"), 0o600))
		root := newSanitizeRoot("--file", srcPath)
		require.Error(t, root.ExecuteContext(ctx))
		entries, err := os.ReadDir(tmp)
		require.NoError(t, err)
		for _, e := range entries {
			require.NotContains(t, e.Name(), ".secret", "failed run must not leave a generated secret")
		}
	})
}
