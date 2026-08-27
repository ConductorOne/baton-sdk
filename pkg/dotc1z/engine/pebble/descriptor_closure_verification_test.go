package pebble

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// descriptorFieldClosure is intentionally explicit: adding a schema field must
// fail this test until its replay/overlay obligation is accepted here. There are
// currently no exempt top-level fields; every listed field receives a sentinel.
var descriptorFieldClosure = map[protoreflect.FullName][]protoreflect.Name{
	"c1.storage.v3.ResourceRecord": {
		"resource_type_id", "resource_id", "display_name", "description", "parent",
		"annotations", "discovered_at", "profile", "status", "created_at", "source_scope_key",
		"icon_asset_external_id",
	},
	"c1.storage.v3.EntitlementRecord": {
		"external_id", "resource", "display_name", "description", "purpose",
		"annotations", "discovered_at", "slug", "grantable_to_resource_type_ids", "source_scope_key",
	},
	"c1.storage.v3.GrantRecord": {
		"external_id", "entitlement", "principal", "discovered_at", "expansion",
		"needs_expansion", "annotations", "sources", "source_scope_key",
	},
	"c1.storage.v3.SourceCacheEntryRecord": {
		"row_kind", "scope_key", "cache_validator", "discovered_at", "invalidated",
		"row_count",
	},
}

// fillDescriptorFixture gives every declared top-level field a non-default
// sentinel. The recursive fill is only fixture construction; closure is asserted
// over the four top-level record descriptors named by D11.
func fillDescriptorFixture(t *testing.T, msg proto.Message, overrides map[protoreflect.Name]string) {
	t.Helper()
	m := msg.ProtoReflect()
	wantFields, ok := descriptorFieldClosure[m.Descriptor().FullName()]
	require.Truef(t, ok, "descriptor %s has no explicit D11 field registry", m.Descriptor().FullName())
	gotFields := make([]protoreflect.Name, 0, m.Descriptor().Fields().Len())
	for i := 0; i < m.Descriptor().Fields().Len(); i++ {
		gotFields = append(gotFields, m.Descriptor().Fields().Get(i).Name())
	}
	require.Equal(t, wantFields, gotFields,
		"schema fields changed: classify every new field before accepting replay/overlay closure")
	fillDescriptorMessage(m, 2)
	for name, value := range overrides {
		fd := m.Descriptor().Fields().ByName(name)
		require.NotNilf(t, fd, "%s has no field %q", m.Descriptor().FullName(), name)
		require.Equal(t, protoreflect.StringKind, fd.Kind(), "override %s.%s must be a string", m.Descriptor().FullName(), name)
		m.Set(fd, protoreflect.ValueOfString(value))
	}
	for i := 0; i < m.Descriptor().Fields().Len(); i++ {
		fd := m.Descriptor().Fields().Get(i)
		switch {
		case fd.IsMap():
			require.Positivef(t, m.Get(fd).Map().Len(), "%s.%s is not represented", m.Descriptor().FullName(), fd.Name())
		case fd.IsList():
			require.Positivef(t, m.Get(fd).List().Len(), "%s.%s is not represented", m.Descriptor().FullName(), fd.Name())
		case fd.Kind() == protoreflect.MessageKind:
			require.Truef(t, m.Has(fd), "%s.%s is not represented", m.Descriptor().FullName(), fd.Name())
		default:
			require.NotEqualf(t, fd.Default().Interface(), m.Get(fd).Interface(),
				"%s.%s is not represented", m.Descriptor().FullName(), fd.Name())
		}
	}
}

func fillDescriptorMessage(m protoreflect.Message, depth int) {
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch {
		case fd.IsMap():
			mp := m.Mutable(fd).Map()
			key := descriptorScalarValue(fd.MapKey(), "key")
			value := mp.NewValue()
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				if depth > 0 {
					fillDescriptorMessage(value.Message(), depth-1)
				}
			} else {
				value = descriptorScalarValue(fd.MapValue(), "value")
			}
			mp.Set(key.MapKey(), value)
		case fd.IsList():
			list := m.Mutable(fd).List()
			value := list.NewElement()
			if fd.Kind() == protoreflect.MessageKind {
				if depth > 0 {
					fillDescriptorMessage(value.Message(), depth-1)
				}
			} else {
				value = descriptorScalarValue(fd, string(fd.Name()))
			}
			list.Append(value)
		case fd.Kind() == protoreflect.MessageKind:
			child := m.Mutable(fd).Message()
			if depth > 0 {
				fillDescriptorMessage(child, depth-1)
			}
		default:
			m.Set(fd, descriptorScalarValue(fd, string(fd.Name())))
		}
	}
}

func descriptorScalarValue(fd protoreflect.FieldDescriptor, label string) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.EnumKind:
		values := fd.Enum().Values()
		return protoreflect.ValueOfEnum(values.Get(values.Len() - 1).Number())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(17)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(17)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(17)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(17)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(17.5)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(17.5)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("sentinel-" + label)
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte("sentinel-" + label))
	default:
		panic(fmt.Sprintf("unsupported scalar descriptor kind %s", fd.Kind()))
	}
}

func assertRecordIndexesPresent(t *testing.T, e *Engine, keys [][]byte) {
	t.Helper()
	for _, key := range keys {
		_, closer, err := e.db.Get(key)
		require.NoErrorf(t, err, "missing index key %x", key)
		require.NoError(t, closer.Close())
	}
}

func validateDirectMaterialization(want, got proto.Message) error {
	if !proto.Equal(want, got) {
		return fmt.Errorf("semantic store differs from direct materialization")
	}
	return nil
}

func TestVerificationDescriptorClosedReplayAndDirectMaterialization(t *testing.T) {
	ctx := t.Context()
	tests := []struct {
		name      string
		newRecord func() proto.Message
		put       func(*Engine, proto.Message) error
		read      func(*Engine) proto.Message
		indexKeys func(proto.Message) [][]byte
		overlay   func(proto.Message)
		project   func(*Engine, proto.Message) map[string][]string
	}{
		{
			name: "resources",
			newRecord: func() proto.Message {
				r := &v3.ResourceRecord{}
				fillDescriptorFixture(t, r, map[protoreflect.Name]string{
					"resource_type_id": "user", "resource_id": "alice", "source_scope_key": "scope-a",
				})
				return r
			},
			put: func(e *Engine, msg proto.Message) error {
				return e.PutResourceRecords(ctx, msg.(*v3.ResourceRecord))
			},
			read: func(e *Engine) proto.Message {
				var got *v3.ResourceRecord
				require.NoError(t, e.IterateResources(ctx, func(r *v3.ResourceRecord) bool { got = r; return false }))
				return got
			},
			indexKeys: func(msg proto.Message) [][]byte { return ResourceIndexKeys(msg.(*v3.ResourceRecord)) },
			overlay: func(msg proto.Message) {
				r := msg.(*v3.ResourceRecord)
				r.SetDisplayName("overlay-display")
				r.GetParent().SetResourceId("overlay-parent")
			},
			project: func(e *Engine, msg proto.Message) map[string][]string {
				r := msg.(*v3.ResourceRecord)
				out := map[string][]string{"by_parent": {}}
				require.NoError(t, e.IterateResourcesByParent(ctx, r.GetParent().GetResourceTypeId(), r.GetParent().GetResourceId(),
					func(got *v3.ResourceRecord) bool {
						out["by_parent"] = append(out["by_parent"], got.GetResourceTypeId()+"|"+got.GetResourceId())
						return true
					}))
				return out
			},
		},
		{
			name: "entitlements",
			newRecord: func() proto.Message {
				r := &v3.EntitlementRecord{}
				fillDescriptorFixture(t, r, map[protoreflect.Name]string{
					"external_id": "group:g1:member", "source_scope_key": "scope-a",
				})
				return r
			},
			put: func(e *Engine, msg proto.Message) error {
				return e.PutEntitlementRecords(ctx, msg.(*v3.EntitlementRecord))
			},
			read: func(e *Engine) proto.Message {
				var got *v3.EntitlementRecord
				require.NoError(t, e.IterateEntitlements(ctx, func(r *v3.EntitlementRecord) bool { got = r; return false }))
				return got
			},
			indexKeys: func(msg proto.Message) [][]byte { return EntitlementIndexKeys(msg.(*v3.EntitlementRecord)) },
			overlay: func(msg proto.Message) {
				msg.(*v3.EntitlementRecord).SetPurpose("overlay-purpose")
			},
			project: func(e *Engine, msg proto.Message) map[string][]string {
				r := msg.(*v3.EntitlementRecord)
				out := map[string][]string{"by_resource": {}}
				require.NoError(t, e.IterateEntitlementsByResource(ctx, r.GetResource().GetResourceTypeId(), r.GetResource().GetResourceId(),
					func(got *v3.EntitlementRecord) bool {
						out["by_resource"] = append(out["by_resource"], got.GetExternalId())
						return true
					}))
				return out
			},
		},
		{
			name: "grants",
			newRecord: func() proto.Message {
				r := &v3.GrantRecord{}
				fillDescriptorFixture(t, r, map[protoreflect.Name]string{
					"external_id": "grant-1", "source_scope_key": "scope-a",
				})
				return r
			},
			put: func(e *Engine, msg proto.Message) error {
				return e.PutGrantRecords(ctx, msg.(*v3.GrantRecord))
			},
			read: func(e *Engine) proto.Message {
				var got *v3.GrantRecord
				require.NoError(t, e.IterateGrants(ctx, func(r *v3.GrantRecord) bool { got = r; return false }))
				return got
			},
			indexKeys: func(msg proto.Message) [][]byte { return GrantIndexKeys(msg.(*v3.GrantRecord)) },
			overlay: func(msg proto.Message) {
				r := msg.(*v3.GrantRecord)
				r.SetNeedsExpansion(false)
				r.GetExpansion().SetEntitlementIds([]string{"overlay-expansion"})
			},
			project: func(e *Engine, msg proto.Message) map[string][]string {
				r := msg.(*v3.GrantRecord)
				out := map[string][]string{
					"by_entitlement":  {},
					"by_principal":    {},
					"needs_expansion": {},
				}
				require.NoError(t, e.IterateGrantsByEntitlement(ctx, r.GetEntitlement().GetEntitlementId(),
					func(got *v3.GrantRecord) bool {
						out["by_entitlement"] = append(out["by_entitlement"], got.GetExternalId())
						return true
					}))
				require.NoError(t, e.IterateGrantsByPrincipal(ctx, r.GetPrincipal().GetResourceTypeId(), r.GetPrincipal().GetResourceId(),
					func(got *v3.GrantRecord) bool {
						out["by_principal"] = append(out["by_principal"], got.GetExternalId())
						return true
					}))
				require.NoError(t, e.IterateGrantsByNeedsExpansion(ctx, func(got *v3.GrantRecord) bool {
					out["needs_expansion"] = append(out["needs_expansion"], got.GetExternalId())
					return true
				}))
				for _, ids := range out {
					sort.Strings(ids)
				}
				return out
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record := tc.newRecord()
			open := func() *Engine {
				e, _ := newTestEngine(t)
				return e
			}
			source, replayed, direct := open(), open(), open()
			// The replay preflight (CO-014) requires the source sealed by a
			// counting EndSync, which needs a real sync-run record — bind
			// the source through StartNewSync instead of a bare rebind.
			_, err := NewAdapter(source).StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			syncID := ksuid.New().String()
			for _, e := range []*Engine{replayed, direct} {
				require.NoError(t, e.SetCurrentSync(ctx, syncID))
			}
			require.NoError(t, tc.put(source, record))
			require.NoError(t, tc.put(direct, proto.Clone(record)))
			sealReplaySource(ctx, t, source, sourcecache.RowKind(tc.name), "scope-a")

			switch tc.name {
			case "resources":
				_, err := replayed.ReplaySourceCacheResources(ctx, source, "scope-a")
				require.NoError(t, err)
			case "entitlements":
				_, err := replayed.ReplaySourceCacheEntitlements(ctx, source, "scope-a")
				require.NoError(t, err)
			case "grants":
				_, err := replayed.ReplaySourceCacheGrants(ctx, source, "scope-a")
				require.NoError(t, err)
			}

			replayedRecord, directRecord := tc.read(replayed), tc.read(direct)
			require.NotNil(t, replayedRecord)
			require.NoError(t, validateDirectMaterialization(record, replayedRecord),
				"replay lost a descriptor-covered field")
			require.NoError(t, validateDirectMaterialization(directRecord, replayedRecord),
				"replay differs from direct materialization")
			assertRecordIndexesPresent(t, replayed, tc.indexKeys(replayedRecord))
			assertRecordIndexesPresent(t, direct, tc.indexKeys(directRecord))
			require.Equal(t, tc.project(direct, directRecord), tc.project(replayed, replayedRecord),
				"index-backed query projections differ after replay")
			require.NoError(t, replayed.PutSourceCacheEntry(ctx, tc.name, "scope-a", "etag"))
			require.NoError(t, direct.PutSourceCacheEntry(ctx, tc.name, "scope-a", "etag"))
			replayedManifest, err := replayed.GetSourceCacheEntry(ctx, tc.name, "scope-a")
			require.NoError(t, err)
			directManifest, err := direct.GetSourceCacheEntry(ctx, tc.name, "scope-a")
			require.NoError(t, err)
			replayedManifest.SetDiscoveredAt(nil)
			directManifest.SetDiscoveredAt(nil)
			require.True(t, proto.Equal(directManifest, replayedManifest), "destination manifest semantics differ")

			baseIndexKeys := tc.indexKeys(record)
			overlay := proto.Clone(record)
			tc.overlay(overlay)
			require.NoError(t, tc.put(replayed, overlay))
			require.NoError(t, tc.put(replayed, proto.Clone(overlay)), "second overlay must be idempotent")
			require.NoError(t, tc.put(direct, proto.Clone(overlay)))

			replayedOverlay, directOverlay := tc.read(replayed), tc.read(direct)
			require.NoError(t, validateDirectMaterialization(overlay, replayedOverlay),
				"overlay lost a descriptor-covered field")
			require.NoError(t, validateDirectMaterialization(directOverlay, replayedOverlay),
				"repeated overlay differs from one direct application")
			overlayIndexKeys := tc.indexKeys(replayedOverlay)
			assertRecordIndexesPresent(t, replayed, overlayIndexKeys)
			require.Equal(t, tc.project(direct, directOverlay), tc.project(replayed, replayedOverlay),
				"index-backed query projections differ after repeated overlay")
			require.NoError(t, replayed.PutSourceCacheEntry(ctx, tc.name, "scope-a", "etag-overlay"))
			require.NoError(t, replayed.PutSourceCacheEntry(ctx, tc.name, "scope-a", "etag-overlay"))
			require.NoError(t, direct.PutSourceCacheEntry(ctx, tc.name, "scope-a", "etag-overlay"))
			replayedManifest, err = replayed.GetSourceCacheEntry(ctx, tc.name, "scope-a")
			require.NoError(t, err)
			directManifest, err = direct.GetSourceCacheEntry(ctx, tc.name, "scope-a")
			require.NoError(t, err)
			replayedManifest.SetDiscoveredAt(nil)
			directManifest.SetDiscoveredAt(nil)
			require.True(t, proto.Equal(directManifest, replayedManifest),
				"repeated overlay manifest differs from one direct publication")
			for _, oldKey := range baseIndexKeys {
				stillOwned := false
				for _, newKey := range overlayIndexKeys {
					if bytes.Equal(oldKey, newKey) {
						stillOwned = true
						break
					}
				}
				if !stillOwned {
					_, closer, err := replayed.db.Get(oldKey)
					if closer != nil {
						_ = closer.Close()
					}
					require.ErrorIsf(t, err, cpebble.ErrNotFound, "stale overlay index key %x", oldKey)
				}
			}
		})
	}

	manifest := &v3.SourceCacheEntryRecord{}
	fillDescriptorFixture(t, manifest, map[protoreflect.Name]string{
		"row_kind": "resources", "scope_key": "scope-a", "cache_validator": "etag",
	})
	encoded, err := proto.Marshal(manifest)
	require.NoError(t, err)
	roundTrip := &v3.SourceCacheEntryRecord{}
	require.NoError(t, proto.Unmarshal(encoded, roundTrip))
	require.True(t, proto.Equal(manifest, roundTrip), "manifest descriptor fixture did not round trip")

	t.Run("direct-differential-mutation-adequacy", func(t *testing.T) {
		want := &v3.ResourceRecord{}
		fillDescriptorFixture(t, want, map[protoreflect.Name]string{
			"resource_type_id": "user", "resource_id": "alice", "source_scope_key": "scope-a",
		})
		wrongMerge := proto.Clone(want).(*v3.ResourceRecord)
		wrongMerge.SetDescription("dirty destination value won")
		require.Error(t, validateDirectMaterialization(want, wrongMerge))
	})
}
