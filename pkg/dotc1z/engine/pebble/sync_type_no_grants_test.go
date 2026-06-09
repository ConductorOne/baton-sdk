package pebble

import (
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// no_grants must survive the v2 -> v3 -> v2 round-trip, not collapse to unspecified.
func TestNoGrantsV3RoundTrip(t *testing.T) {
	enum := v2SyncTypeToV3(connectorstore.SyncTypeNoGrants)
	if enum != v3.SyncType_SYNC_TYPE_NO_GRANTS {
		t.Fatalf("v2SyncTypeToV3: got %v, want SYNC_TYPE_NO_GRANTS", enum)
	}
	if got := syncTypeV3ToConnectorstore(enum); got != connectorstore.SyncTypeNoGrants {
		t.Errorf("syncTypeV3ToConnectorstore: got %q, want no_grants", got)
	}
	if got := v3SyncTypeToString(enum); got != string(connectorstore.SyncTypeNoGrants) {
		t.Errorf("v3SyncTypeToString: got %q, want no_grants", got)
	}
}
