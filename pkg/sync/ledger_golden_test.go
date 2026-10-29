package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func assertLedgerGolden(t *testing.T, store c1zstore.Store, name string) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "ledger_golden", name))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &doc))
	ctx := t.Context()
	if raw, ok := doc["resource_types"]; ok {
		got, err := store.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
		require.NoError(t, err)
		equalProtoLists(t, unmarshalGoldenList[v2.ResourceType](t, raw), got.GetList())
	}
	if raw, ok := doc["resources"]; ok {
		got, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{})
		require.NoError(t, err)
		equalProtoLists(t, unmarshalGoldenList[v2.Resource](t, raw), got.GetList())
	}
	if raw, ok := doc["entitlements"]; ok {
		got, err := store.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{})
		require.NoError(t, err)
		equalProtoLists(t, unmarshalGoldenList[v2.Entitlement](t, raw), got.GetList())
	}
	if raw, ok := doc["grants"]; ok {
		got, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
		require.NoError(t, err)
		equalProtoLists(t, unmarshalGoldenList[v2.Grant](t, raw), got.GetList())
	}
}

func unmarshalGoldenList[T any, P interface {
	*T
	proto.Message
}](t *testing.T, raw json.RawMessage) []P {
	t.Helper()
	var msgs []json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &msgs))
	out := make([]P, 0, len(msgs))
	for _, msg := range msgs {
		item := P(new(T))
		require.NoError(t, protojson.Unmarshal(msg, item))
		out = append(out, item)
	}
	return out
}
