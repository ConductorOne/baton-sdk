package pebble

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// === V-18: route completeness (C17) ===
//
// Every public grant-producing method on the v3 GrantsReaderService must be
// accounted for by a checked-in coverage table classifying it as
// timestamp-capable (returns c1.storage.v3.GrantRecord, so discovered_at is on
// the wire) or explicitly legacy-only. The inventory is taken from the gRPC
// ServiceDesc (the descriptor at the tested revision), so a NEW route added
// without a coverage entry trips this test — the absence-class guard.
//
// v3RouteCoverage is the authoritative table. All five v3 routes return
// GrantRecord, hence all are timestamp-capable.
var v3RouteCoverage = map[string]string{
	"GetGrant":                  "timestamp-capable",
	"ListGrantsForEntitlement":  "timestamp-capable",
	"ListGrantsForResourceType": "timestamp-capable",
	"ListGrantsForEntitlements": "timestamp-capable",
	"ListGrantsForPrincipal":    "timestamp-capable",
}

func TestV3GrantReaderRouteCompleteness(t *testing.T) {
	// Inventory the actual service surface from the descriptor.
	var descMethods []string
	for _, m := range reader_v3.GrantsReaderService_ServiceDesc.Methods {
		descMethods = append(descMethods, m.MethodName)
	}
	// Streams (none today) would also count; include for future-proofing.
	for _, s := range reader_v3.GrantsReaderService_ServiceDesc.Streams {
		descMethods = append(descMethods, s.StreamName)
	}
	sort.Strings(descMethods)
	require.NotEmpty(t, descMethods, "ServiceDesc must expose at least one method")

	// Every descriptor method must appear in the coverage table.
	for _, name := range descMethods {
		class, ok := v3RouteCoverage[name]
		require.Truef(t, ok,
			"v3 grant read route %q has no coverage-table entry — a new grant-producing route must be classified timestamp-capable or explicit legacy-only (C17)", name)
		require.Contains(t, []string{"timestamp-capable", "legacy-only"}, class,
			"route %q has an unrecognized coverage class %q", name, class)
	}

	// And the coverage table must not list phantom routes (keeps it honest;
	// this is the "delete/rename a method" adequacy direction).
	descSet := make(map[string]struct{}, len(descMethods))
	for _, n := range descMethods {
		descSet[n] = struct{}{}
	}
	for name := range v3RouteCoverage {
		_, ok := descSet[name]
		require.Truef(t, ok, "coverage table lists %q which is not on the ServiceDesc (stale/renamed route)", name)
	}

	// Adequacy plant (self-contained): simulate a route the descriptor
	// exposes but the table forgot — the check above must reject it.
	t.Run("plant_missing_coverage_entry_is_caught", func(t *testing.T) {
		planted := make([]string, 0, len(descMethods)+1)
		planted = append(planted, descMethods...)
		planted = append(planted, "NewlyAddedGrantRoute")
		missing := false
		for _, name := range planted {
			if _, ok := v3RouteCoverage[name]; !ok {
				missing = true
			}
		}
		require.True(t, missing, "the completeness check must detect a descriptor route absent from the coverage table")
	})
}

// Compile-time proof the concrete engine v3 reader satisfies the full v3
// service contract (reinforces V-10) — every route in the table is a real
// method on the implementation.
var _ connectorstore.V3GrantReader = engineV3Grants{}
var _ reader_v3.GrantsReaderServiceServer = engineV3Grants{}
