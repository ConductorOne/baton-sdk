// SPDX-License-Identifier: Apache-2.0

package c1zsanitize

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

// TestSecurity_SanitizeRewritesLicenseName guards the sanitizer's
// identity-stripping contract for LicenseProfileTrait.license_name.
//
// Finding: pkg/c1zsanitize/handlers.go:handleLicenseProfileTrait:license-name-verbatim-passthrough
//
// license_name is the only human-readable free-text field in the handler set
// that was copied verbatim instead of HMAC-transformed (every sibling —
// user login/aliases/names, role expression, entitlement display names — is
// rewritten via s.id()). The package contract (sanitize.go) promises an
// "identity-stripped copy ... where the original customer data must not
// appear", and a connector can place up to 1024 bytes of arbitrary
// tenant-identifying text in the field, shipping it verbatim into artifacts
// internal developers receive as sanitized.
//
// Pre-fix (red): the marker string appears VERBATIM in the sanitized output.
// Post-fix (green): license_name is HMAC-rewritten — never equal to the
// original, never empty (a redaction that emptied the field would also break
// the deterministic-reuse contract; the HMAC preserves determinism).
func TestSecurity_SanitizeRewritesLicenseName(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	secret := bytes32("license-name-security")
	marker := "acme-corp-internal-Business-Plus-2026-secret-plan"

	src := mustOpen(t, ctx, srcPath, false)
	_, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          "license",
		DisplayName: "License",
	}.Build()))
	licenseRes := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "license", Resource: "l1"}.Build(),
	}.Build()
	require.NoError(t, src.PutResources(ctx, licenseRes))
	member := v2.Entitlement_builder{
		Id:          "seat",
		Resource:    licenseRes,
		DisplayName: "Seat",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		Annotations: []*anypb.Any{
			mustAny(t, v2.LicenseProfileTrait_builder{
				LicenseName:    marker,
				PurchasedSeats: 10,
				EntitlementIds: []string{"seat"},
			}.Build()),
		},
	}.Build()
	require.NoError(t, src.PutEntitlements(ctx, member))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	srcRO := mustOpen(t, ctx, srcPath, true)
	defer srcRO.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)
	require.NoError(t, Sanitize(ctx, srcRO, dst, Options{Secret: secret}))
	require.NoError(t, dst.Close(ctx))

	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)

	rec := collectRecords(t, ctx, dstRO)
	require.Len(t, rec.entitlements, 1)

	found := false
	for _, a := range rec.entitlements[0].GetAnnotations() {
		trait, ok := anyAs[*v2.LicenseProfileTrait](t, a)
		if !ok {
			continue
		}
		found = true
		require.NotEqual(t, marker, trait.GetLicenseName(),
			"license_name shipped VERBATIM in the sanitized artifact — identity-stripping contract violated")
		require.NotEmpty(t, trait.GetLicenseName(),
			"license_name was dropped entirely; it must be HMAC-rewritten (deterministic), not emptied")
	}
	require.True(t, found, "LicenseProfileTrait annotation should survive sanitization (it is whitelisted)")

	// Sibling-field control: the entitlement display name must also be
	// rewritten (the same HMAC path), confirming the transform machinery ran.
	require.NotEqual(t, "Seat", rec.entitlements[0].GetDisplayName())
}

// anyAs unmarshals an annotation Any into the given trait type, reporting
// whether it matched.
func anyAs[T any](t *testing.T, a *anypb.Any) (T, bool) {
	t.Helper()
	var zero T
	// Match on the registered type URL for LicenseProfileTrait.
	turl := "type.googleapis.com/c1.connector.v2.LicenseProfileTrait"
	if a.GetTypeUrl() != turl {
		return zero, false
	}
	m, err := a.UnmarshalNew()
	if err != nil {
		t.Fatalf("unmarshal LicenseProfileTrait: %v", err)
	}
	tr, ok := m.(T)
	if !ok {
		t.Fatalf("annotation %s is not a LicenseProfileTrait", turl)
	}
	return tr, true
}
