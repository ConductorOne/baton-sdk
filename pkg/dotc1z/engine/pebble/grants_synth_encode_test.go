package pebble

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// TestGrantSourcesWireSchemaPin fails when the schema the hand encoder
// (appendGrantSourcesWire) depends on changes, so the encoder cannot silently
// drop data after a proto or Go-struct edit. It pins three invariants:
//
//  1. GrantRecord.sources is field 9, a map<string, GrantSourceRecord>, and 9
//     is the HIGHEST field number in GrantRecord. The encoder appends sources
//     after the base marshal; a higher-numbered field would have to sort
//     after sources in the canonical deterministic byte order, breaking the
//     byte-equality contract.
//  2. GrantSourceRecord has exactly the four known fields. The encoder only
//     ever emits is_direct (the synthesized writers never set the resource /
//     entitlement fields); a new or renumbered field means revisiting that
//     assumption.
//  3. batonGrant.Source has exactly {EntitlementID, IsDirect}. A new field
//     there is data the encoder would silently not persist.
//
// If this test fails: update appendGrantSourcesWire (and
// TestAppendGrantSourcesWireMatchesDeterministicProto's coverage) before
// re-pinning.
func TestGrantSourcesWireSchemaPin(t *testing.T) {
	grantMD := (&v3.GrantRecord{}).ProtoReflect().Descriptor()
	fields := grantMD.Fields()
	maxNum := protoreflect.FieldNumber(0)
	for i := 0; i < fields.Len(); i++ {
		if n := fields.Get(i).Number(); n > maxNum {
			maxNum = n
		}
	}
	require.Equal(t, protoreflect.FieldNumber(9), maxNum,
		"GrantRecord gained a field numbered above sources(9): appendGrantSourcesWire appends sources last and would emit non-canonical field order")

	src := fields.ByName("sources")
	require.NotNil(t, src, "GrantRecord.sources missing")
	require.Equal(t, protoreflect.FieldNumber(9), src.Number(), "GrantRecord.sources renumbered")
	require.True(t, src.IsMap(), "GrantRecord.sources is no longer a map")
	require.Equal(t, protoreflect.StringKind, src.MapKey().Kind(), "sources map key kind changed")
	require.Equal(t, protoreflect.MessageKind, src.MapValue().Kind(), "sources map value kind changed")
	require.Equal(t, protoreflect.FullName("c1.storage.v3.GrantSourceRecord"), src.MapValue().Message().FullName())

	gsrMD := (&v3.GrantSourceRecord{}).ProtoReflect().Descriptor()
	gsrFields := []string{}
	for i := 0; i < gsrMD.Fields().Len(); i++ {
		f := gsrMD.Fields().Get(i)
		gsrFields = append(gsrFields, fmt.Sprintf("%d:%s:%s", f.Number(), f.Name(), f.Kind()))
	}
	require.Equal(t, []string{
		"1:resource_type_id:string",
		"2:resource_id:string",
		"3:entitlement_id:string",
		"4:is_direct:bool",
	}, gsrFields, "GrantSourceRecord shape changed: appendGrantSourcesWire only emits is_direct")

	srcType := reflect.TypeOf(batonGrant.Source{})
	goFields := []string{}
	for i := 0; i < srcType.NumField(); i++ {
		f := srcType.Field(i)
		goFields = append(goFields, f.Name+":"+f.Type.String())
	}
	require.Equal(t, []string{
		"EntitlementID:string",
		"IsDirect:bool",
	}, goFields, "grant.Source gained a field appendGrantSourcesWire would silently drop")
}

// TestAppendGrantSourcesWireMatchesDeterministicProto pins the hand-encoded
// GrantRecord.sources field to the exact bytes the reflective deterministic
// marshal produces. The codec equivalence harness asserts equal records
// produce equal bytes, so any divergence here is a correctness bug, not just
// a formatting nit.
func TestAppendGrantSourcesWireMatchesDeterministicProto(t *testing.T) {
	cases := []struct {
		name    string
		sources batonGrant.Sources
	}{
		{"single direct", batonGrant.Sources{{EntitlementID: "ent-a", IsDirect: true}}},
		{"single indirect", batonGrant.Sources{{EntitlementID: "ent-a", IsDirect: false}}},
		{"multi unsorted mixed", batonGrant.Sources{
			{EntitlementID: "zeta", IsDirect: true},
			{EntitlementID: "alpha", IsDirect: false},
			{EntitlementID: "mid", IsDirect: true},
		}},
		{"empty key", batonGrant.Sources{{EntitlementID: "", IsDirect: true}}},
		{"unicode key", batonGrant.Sources{
			{EntitlementID: "grüppe:entitlement/членство", IsDirect: false},
			{EntitlementID: "grüppe:entitlement/adminship", IsDirect: true},
		}},
		{"long key multi-byte varint", batonGrant.Sources{
			{EntitlementID: strings.Repeat("x", 300), IsDirect: true},
			{EntitlementID: strings.Repeat("x", 200), IsDirect: false},
		}},
		{"duplicate key last wins", batonGrant.Sources{
			{EntitlementID: "dup", IsDirect: false},
			{EntitlementID: "other", IsDirect: true},
			{EntitlementID: "dup", IsDirect: true},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := timestamppb.Now()
			ent := v3.EntitlementRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "g1",
				EntitlementId:  "group:g1:member",
			}.Build()
			principal := v3.PrincipalRef_builder{
				ResourceTypeId: "user",
				ResourceId:     "u1",
			}.Build()

			// Reference: map field marshaled by the reflective deterministic
			// path — exactly what sourceListToV3 + marshalRecord produced.
			sourceMap := make(map[string]*v3.GrantSourceRecord, len(tc.sources))
			for _, src := range tc.sources {
				sourceMap[src.EntitlementID] = v3.GrantSourceRecord_builder{IsDirect: src.IsDirect}.Build()
			}
			full := v3.GrantRecord_builder{
				ExternalId:   "grant:external:id",
				Entitlement:  ent,
				Principal:    principal,
				Annotations:  []*anypb.Any{expandedGrantImmutableAnnotationAny},
				Sources:      sourceMap,
				DiscoveredAt: now,
			}.Build()
			want, err := marshalRecord(full)
			require.NoError(t, err)

			base := v3.GrantRecord_builder{
				ExternalId:   "grant:external:id",
				Entitlement:  ent,
				Principal:    principal,
				Annotations:  []*anypb.Any{expandedGrantImmutableAnnotationAny},
				DiscoveredAt: now,
			}.Build()
			got, err := marshalRecordAppend(nil, base)
			require.NoError(t, err)
			got, _ = appendGrantSourcesWire(got, nil, tc.sources)

			require.Equal(t, want, got, "hand-encoded sources must match deterministic proto marshal byte-for-byte")

			// Round-trip: the bytes must decode back to the same record.
			decoded := &v3.GrantRecord{}
			require.NoError(t, proto.Unmarshal(got, decoded))
			require.True(t, proto.Equal(full, decoded))
		})
	}
}

// TestAppendGrantSourcesWireRandomized cross-checks the hand encoder against
// the reflective deterministic marshal on a few thousand randomized source
// lists (fixed seed). Catches behavioral drift the targeted cases miss —
// varint length boundaries, duplicate distributions, empty/oddball keys.
func TestAppendGrantSourcesWireRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test data
	alphabet := []rune("abcXYZ:/-_é中\x01\x00 ")
	randKey := func() string {
		n := rng.Intn(40)
		if rng.Intn(20) == 0 {
			n = 120 + rng.Intn(200) // force multi-byte length varints
		}
		b := make([]rune, n)
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(b)
	}

	for iter := 0; iter < 2000; iter++ {
		n := 1 + rng.Intn(6)
		sources := make(batonGrant.Sources, 0, n)
		for i := 0; i < n; i++ {
			key := randKey()
			if len(sources) > 0 && rng.Intn(4) == 0 {
				key = sources[rng.Intn(len(sources))].EntitlementID // duplicate
			}
			sources = append(sources, batonGrant.Source{EntitlementID: key, IsDirect: rng.Intn(2) == 0})
		}

		sourceMap := make(map[string]*v3.GrantSourceRecord, len(sources))
		for _, src := range sources {
			sourceMap[src.EntitlementID] = v3.GrantSourceRecord_builder{IsDirect: src.IsDirect}.Build()
		}
		full := v3.GrantRecord_builder{ExternalId: "x", Sources: sourceMap}.Build()
		want, err := marshalRecord(full)
		require.NoError(t, err)

		got, err := marshalRecordAppend(nil, v3.GrantRecord_builder{ExternalId: "x"}.Build())
		require.NoError(t, err)
		got, _ = appendGrantSourcesWire(got, nil, sources)
		require.Equalf(t, want, got, "iter %d sources %#v", iter, sources)
	}
}

// TestFillSynthGrantRecordSchemaPin fails when the shapes fillSynthGrantRecord
// depends on change, so the setter-based fill cannot silently drop data:
//
//  1. GrantRecord's full field list is pinned. A new proto field means
//     deciding whether the synthesized writers must emit it; if so it has to
//     be assigned unconditionally in fillSynthGrantRecord (reused struct —
//     see its INVARIANT comment) and covered by TestFillSynthGrantRecordReuse.
//  2. The Go-side synthesizedGrantRecord struct is pinned. A new field there
//     is data the writers carry but fillSynthGrantRecord would never persist.
//
// Re-pin only after updating fillSynthGrantRecord (or deliberately deciding
// the new field is not part of the synthesized write path).
func TestFillSynthGrantRecordSchemaPin(t *testing.T) {
	grantMD := (&v3.GrantRecord{}).ProtoReflect().Descriptor()
	protoFields := []string{}
	for i := 0; i < grantMD.Fields().Len(); i++ {
		f := grantMD.Fields().Get(i)
		protoFields = append(protoFields, fmt.Sprintf("%d:%s:%s", f.Number(), f.Name(), f.Kind()))
	}
	require.Equal(t, []string{
		"2:external_id:string",
		"3:entitlement:message",
		"4:principal:message",
		"5:discovered_at:message",
		"6:expansion:message",
		"7:needs_expansion:bool",
		"8:annotations:message",
		"9:sources:message",
	}, protoFields, "GrantRecord shape changed: decide whether fillSynthGrantRecord must set the new/changed field")

	recType := reflect.TypeOf(synthesizedGrantRecord{})
	goFields := []string{}
	for i := 0; i < recType.NumField(); i++ {
		f := recType.Field(i)
		goFields = append(goFields, f.Name+":"+f.Type.String())
	}
	require.Equal(t, []string{
		"id:pebble.grantIdentity",
		"externalID:string",
		"entitlement:*v3.EntitlementRef",
		"principal:*v3.PrincipalRef",
		"sources:grant.Sources",
	}, goFields, "synthesizedGrantRecord gained a field fillSynthGrantRecord would silently drop")
}

// TestFillSynthGrantRecordReuse pins that marshaling one REUSED GrantRecord
// filled row-by-row produces the same bytes as building a fresh record per
// row — i.e. no field from a previous row leaks through the reused struct.
// If a new field is added to what the synthesized writers emit, it must be
// assigned unconditionally in fillSynthGrantRecord or this test's fresh
// reference will diverge.
func TestFillSynthGrantRecordReuse(t *testing.T) {
	now := timestamppb.Now()
	rows := []synthesizedGrantRecord{
		{
			externalID:  "grant-1",
			entitlement: v3.EntitlementRef_builder{ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "e1"}.Build(),
			principal:   v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "u1", ParentResourceTypeId: "org", ParentResourceId: "o1"}.Build(),
			sources:     batonGrant.Sources{{EntitlementID: "src-a", IsDirect: true}},
		},
		// Second row is "smaller" than the first (no parent refs): stale
		// values from row one would show up here if anything leaked.
		{
			externalID:  "grant-2",
			entitlement: v3.EntitlementRef_builder{ResourceTypeId: "role", ResourceId: "r1", EntitlementId: "e2"}.Build(),
			principal:   v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "u2"}.Build(),
			sources:     batonGrant.Sources{{EntitlementID: "src-b", IsDirect: false}},
		},
	}

	reused := &v3.GrantRecord{}
	var srcScratch batonGrant.Sources
	for i := range rows {
		rec := &rows[i]
		fillSynthGrantRecord(reused, rec, now)
		got, err := marshalRecordAppend(nil, reused)
		require.NoError(t, err)
		got, srcScratch = appendGrantSourcesWire(got, srcScratch, rec.sources)

		fresh := v3.GrantRecord_builder{
			ExternalId:   rec.externalID,
			Entitlement:  rec.entitlement,
			Principal:    rec.principal,
			Annotations:  []*anypb.Any{expandedGrantImmutableAnnotationAny},
			DiscoveredAt: now,
		}.Build()
		want, err := marshalRecordAppend(nil, fresh)
		require.NoError(t, err)
		want, _ = appendGrantSourcesWire(want, nil, rec.sources)

		require.Equalf(t, want, got, "row %d: reused record diverged from fresh record", i)
	}
}

// TestAppendGrantSourcesWireScratchReuse verifies the caller's slice is not
// mutated and the returned scratch is reusable across calls.
func TestAppendGrantSourcesWireScratchReuse(t *testing.T) {
	in := batonGrant.Sources{
		{EntitlementID: "b", IsDirect: false},
		{EntitlementID: "a", IsDirect: true},
	}
	orig := append(batonGrant.Sources(nil), in...)

	out1, scratch := appendGrantSourcesWire(nil, nil, in)
	require.Equal(t, orig, in, "input slice must not be reordered")

	out2, _ := appendGrantSourcesWire(nil, scratch, in)
	require.Equal(t, out1, out2, "scratch reuse must not change output")
}
