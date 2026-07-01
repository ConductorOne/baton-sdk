package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	batonEntitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// newAdapter opens a fresh engine in a temp dir and wraps it in an
// Adapter, registering cleanup. External-package twin of the
// in-package newAdapter helper.
func newAdapter(t testing.TB) *pebble.Adapter {
	t.Helper()
	dbDir := filepath.Join(t.TempDir(), "db")
	e, err := pebble.Open(context.Background(), dbDir)
	require.NoError(t, err, "Open")
	t.Cleanup(func() {
		_ = e.Close()
	})
	return pebble.NewAdapter(e)
}

// mkV2Grant builds a minimal v2.Grant for external (pebble_test)
// tests. Mirrors the in-package helper of the same name in
// adapter_test.go.
func mkV2Grant(id, entID, principalRT, principalID string) *v2.Grant {
	canonicalEntID := batonEntitlement.EncodeEntitlementID("app", "github", batonEntitlement.EntitlementKindCustom, entID)
	ent := v2.Entitlement_builder{
		Id: canonicalEntID,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "app",
				Resource:     "github",
			}.Build(),
		}.Build(),
	}.Build()
	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: principalRT,
			Resource:     principalID,
		}.Build(),
	}.Build()
	return v2.Grant_builder{
		Id:          batonGrant.NewGrantID(principal, ent),
		Entitlement: ent,
		Principal:   principal,
	}.Build()
}
