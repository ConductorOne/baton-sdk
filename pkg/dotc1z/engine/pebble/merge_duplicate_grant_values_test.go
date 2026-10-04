package pebble

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// The one-pass fold must write the same bytes as the pairwise fold it
// replaced, which artifacts already on disk were merged with. Groups are
// random and small, so single-contributor fields, duplicate annotations,
// colliding sources, empty expansion ids and discovered_at ties all occur.
func TestMergeDuplicateGrantValuesMatchesPairwiseFold(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic test data.
	anns := make([]*anypb.Any, 4)
	for i := range anns {
		a, err := anypb.New(v3.GrantExpandableRecord_builder{EntitlementIds: []string{fmt.Sprint("ann-", i)}}.Build())
		require.NoError(t, err)
		anns[i] = a
	}
	times := []*timestamppb.Timestamp{nil, timestamppb.New(time.Unix(1000, 0)), timestamppb.New(time.Unix(2000, 0))}
	pick := func(options ...string) string { return options[rng.IntN(len(options))] }
	row := func(i int) []byte {
		b := v3.GrantRecord_builder{
			ExternalId:     fmt.Sprint(pick("a", "b", "c"), i),
			Entitlement:    v3.EntitlementRef_builder{ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "member"}.Build(),
			Principal:      v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "u1"}.Build(),
			DiscoveredAt:   times[rng.IntN(len(times))],
			NeedsExpansion: rng.IntN(2) == 0,
		}
		if rng.IntN(5) < 2 {
			for range 1 + rng.IntN(3) {
				b.Annotations = append(b.Annotations, anns[rng.IntN(len(anns))])
			}
		}
		if rng.IntN(2) == 0 {
			b.Sources = map[string]*v3.GrantSourceRecord{}
			for range 1 + rng.IntN(3) {
				b.Sources[pick("s1", "s2", "s3")] = v3.GrantSourceRecord_builder{
					IsDirect:       rng.IntN(2) == 0,
					ResourceTypeId: pick("", "rt-a", "rt-b"),
					ResourceId:     pick("", "rid-a", "rid-b"),
					EntitlementId:  pick("", "e-a"),
				}.Build()
			}
		}
		if rng.IntN(2) == 0 {
			exp := v3.GrantExpandableRecord_builder{Shallow: rng.IntN(2) == 0}
			for range rng.IntN(4) {
				exp.EntitlementIds = append(exp.EntitlementIds, pick("", "e3", "e1", "e2"))
				exp.ResourceTypeIds = append(exp.ResourceTypeIds, pick("", "user", "group"))
			}
			b.Expansion = exp.Build()
		}
		val, err := marshalRecord(b.Build())
		require.NoError(t, err)
		return val
	}

	for g := range 2000 {
		group := make([][]byte, 2+rng.IntN(4))
		for i := range group {
			group[i] = row(i)
		}
		got, err := mergeDuplicateGrantValues(group)
		require.NoError(t, err)
		require.Equal(t, pairwiseMergeGrantValues(t, group), got, "group %d", g)
	}
}

// A group's size comes from the input file. Each row adds a distinct
// annotation and source, the shape that made a pairwise fold quadratic.
func TestSecurity_MergeDuplicateGrantValuesLargeGroup(t *testing.T) {
	const n = 8192
	values := make([][]byte, n)
	for i := range values {
		ann, err := anypb.New(v3.GrantExpandableRecord_builder{EntitlementIds: []string{fmt.Sprintf("src-%06d", i)}}.Build())
		require.NoError(t, err)
		values[i], err = marshalRecord(v3.GrantRecord_builder{
			ExternalId:   fmt.Sprintf("dup-%06d", i),
			Entitlement:  v3.EntitlementRef_builder{ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "member"}.Build(),
			Principal:    v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "u1"}.Build(),
			Annotations:  []*anypb.Any{ann},
			DiscoveredAt: timestamppb.New(time.Unix(int64(i), 0)),
			Sources: map[string]*v3.GrantSourceRecord{
				fmt.Sprintf("src-%06d", i): v3.GrantSourceRecord_builder{IsDirect: true}.Build(),
			},
		}.Build())
		require.NoError(t, err)
	}

	start := time.Now()
	merged, err := mergeDuplicateGrantValues(values)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 5*time.Second)

	reversed := slices.Clone(values)
	slices.Reverse(reversed)
	mergedReversed, err := mergeDuplicateGrantValues(reversed)
	require.NoError(t, err)
	require.Equal(t, merged, mergedReversed)

	var rec v3.GrantRecord
	require.NoError(t, proto.Unmarshal(merged, &rec))
	require.Len(t, rec.GetSources(), n)
	require.Len(t, rec.GetAnnotations(), n)
	require.Equal(t, "dup-000000", rec.GetExternalId())
}

// pairwiseMergeGrantValues is the fold mergeDuplicateGrantValues replaced:
// clone the first row and merge each later row into it.
func pairwiseMergeGrantValues(t *testing.T, values [][]byte) []byte {
	t.Helper()
	out := &v3.GrantRecord{}
	require.NoError(t, proto.Unmarshal(values[0], out))
	for _, value := range values[1:] {
		src := &v3.GrantRecord{}
		require.NoError(t, proto.Unmarshal(value, src))
		if recordIdentityInfoWins(src.GetDiscoveredAt(), src.GetExternalId(), out.GetDiscoveredAt(), out.GetExternalId()) {
			out.SetExternalId(src.GetExternalId())
			out.SetDiscoveredAt(src.GetDiscoveredAt())
		}
		out.SetNeedsExpansion(out.GetNeedsExpansion() || src.GetNeedsExpansion())
		out.SetAnnotations(pairwiseAnnotations(out.GetAnnotations(), src.GetAnnotations()))
		out.SetSources(pairwiseSources(out.GetSources(), src.GetSources()))
		out.SetExpansion(pairwiseExpansion(out.GetExpansion(), src.GetExpansion()))
	}
	b, err := marshalRecord(out)
	require.NoError(t, err)
	return b
}

func pairwiseAnnotations(a, b []*anypb.Any) []*anypb.Any {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	var out []*anypb.Any
	seen := map[string]struct{}{}
	for _, item := range slices.Concat(a, b) {
		key := item.GetTypeUrl() + "\x00" + string(item.GetValue())
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetTypeUrl() != out[j].GetTypeUrl() {
			return out[i].GetTypeUrl() < out[j].GetTypeUrl()
		}
		return string(out[i].GetValue()) < string(out[j].GetValue())
	})
	return out
}

func pairwiseSources(a, b map[string]*v3.GrantSourceRecord) map[string]*v3.GrantSourceRecord {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make(map[string]*v3.GrantSourceRecord, len(a)+len(b))
	for key, value := range a {
		out[key] = proto.Clone(value).(*v3.GrantSourceRecord)
	}
	for key, value := range b {
		existing := out[key]
		if existing == nil {
			out[key] = proto.Clone(value).(*v3.GrantSourceRecord)
			continue
		}
		out[key] = v3.GrantSourceRecord_builder{
			ResourceTypeId: minNonEmptyString(existing.GetResourceTypeId(), value.GetResourceTypeId()),
			ResourceId:     minNonEmptyString(existing.GetResourceId(), value.GetResourceId()),
			EntitlementId:  minNonEmptyString(existing.GetEntitlementId(), value.GetEntitlementId()),
			IsDirect:       existing.GetIsDirect() || value.GetIsDirect(),
		}.Build()
	}
	return out
}

func pairwiseExpansion(a, b *v3.GrantExpandableRecord) *v3.GrantExpandableRecord {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	union := func(x, y []string) []string {
		var out []string
		for _, v := range slices.Concat(x, y) {
			if v != "" && !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
		slices.Sort(out)
		return out
	}
	return v3.GrantExpandableRecord_builder{
		EntitlementIds:  union(a.GetEntitlementIds(), b.GetEntitlementIds()),
		ResourceTypeIds: union(a.GetResourceTypeIds(), b.GetResourceTypeIds()),
		Shallow:         a.GetShallow() && b.GetShallow(),
	}.Build()
}
