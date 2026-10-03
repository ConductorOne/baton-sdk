package pebble

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// A hostile v1 c1z can legally hold arbitrarily many rows with distinct
// external ids whose refs fold to one structural identity (the grants
// table's uniqueness is (external_id, sync_id)). Both duplicate-tolerant
// merge routes — the id-index migration and the bulk import — hand each
// same-identity group whole to mergeDuplicateGrantValues, and the old
// pairwise fold rebuilt the annotation union and source maps from scratch
// on every step: a group of N rows carrying one annotation each cost
// Θ(N²) map operations inside ONE uninterruptible resolve call, with no
// ctx check inside the group. Same class as the C1Z-SEC-006/#1164 fixes:
// fix the algorithm — the merged bytes are pinned identical by the
// fold-order invariant, so no input rejection is needed.
func TestSecurity_MergeDuplicateGrantValuesHostileGroupTerminates(t *testing.T) {
	ent := v3.EntitlementRef_builder{ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "member"}.Build()
	princ := v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "u1"}.Build()
	mk := func(ext string, i int) []byte {
		// One DISTINCT annotation and one DISTINCT source per row:
		// the pairwise fold re-sorted the whole growing annotation
		ann, err := anypb.New(v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{fmt.Sprintf("src-%06d", i)},
		}.Build())
		require.NoError(t, err)
		val, err := proto.Marshal(v3.GrantRecord_builder{
			ExternalId:   ext,
			Entitlement:  ent,
			Principal:    princ,
			Annotations:  []*anypb.Any{ann},
			DiscoveredAt: timestamppb.New(time.Unix(int64(i), 0).UTC()),
			Sources: map[string]*v3.GrantSourceRecord{
				fmt.Sprintf("src-%06d", i): v3.GrantSourceRecord_builder{IsDirect: true}.Build(),
			},
		}.Build())
		require.NoError(t, err)
		return val
	}

	// N sized so the pre-fix pairwise fold (measured: 3.1s at 4096,
	// 13.5s at 8192 — the 4x-per-doubling of Θ(N²)) blows the bound
	// while the fixed linear fold stays far under it.
	const n = 8192
	values := make([][]byte, n)
	for i := range values {
		values[i] = mk(fmt.Sprintf("dup-%06d", i), i)
	}

	start := time.Now()
	merged, err := mergeDuplicateGrantValues(values)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 10*time.Second,
		"a same-identity group of %d one-annotation rows must fold linear; the pairwise rebuild was Θ(N²)", n)

	// The fold result is byte-identical under any input order (the
	// documented commutative+associative invariant) — assert against
	// a reversed permutation.
	rev := slices.Clone(values)
	slices.Reverse(rev)
	mergedRev, err := mergeDuplicateGrantValues(rev)
	require.NoError(t, err)
	require.Equal(t, merged, mergedRev, "fold must stay order-independent")

	// Field-level shape: all N distinct sources and annotations
	// survive, earliest discovered_at wins the identity info.
	var rec v3.GrantRecord
	require.NoError(t, proto.Unmarshal(merged, &rec))
	require.Len(t, rec.GetSources(), n)
	require.Len(t, rec.GetAnnotations(), n)
	require.Equal(t, "dup-000000", rec.GetExternalId(), "earliest discovered_at wins the identity info")
}
