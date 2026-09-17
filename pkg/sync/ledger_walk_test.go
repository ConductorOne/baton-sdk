package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerWalkRestoresNextAndChildrenWithoutWrites(t *testing.T) {
	f := newLedgerFixture(t)
	runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	root := c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: "type", PageToken: "first"}
	child := c1zstore.LedgerActionIdentity{
		Op: "list-entitlements", ResourceTypeID: "child-type", ResourceID: "child",
		ParentResourceTypeID: "parent-type", ParentResourceID: "parent", PageToken: "child-token", TypeScoped: true,
	}
	_, err = runtime.runPage(t.Context(), 0, root, func(_ context.Context, page *ledgerPage) error {
		page.row.TypeScopedPlanned = true
		return page.transition("second", c1zstore.LedgerChild{Identity: child, Spawned: true})
	})
	require.NoError(t, err)
	before := ledgerRawSnapshot(t, f.engine)
	eventCount := len(f.audit.events)
	f.audit.enter(ledgerWalk)
	pending, err := runtime.walk(t.Context(), []ledgerAction{{identity: root}})
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	next := root
	next.PageToken = "second"
	require.Equal(t, []ledgerAction{{identity: next, typeScopedPlanned: true}, {identity: child, spawned: true}}, pending)
	require.Len(t, f.audit.events, eventCount)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerWalkIdentityFields(t *testing.T) {
	base := c1zstore.LedgerActionIdentity{
		Op: "list-resources", ResourceTypeID: "type", ResourceID: "resource", ParentResourceTypeID: "parent-type",
		ParentResourceID: "parent", PageToken: "page", TypeScoped: true,
	}
	changes := []struct {
		name   string
		change func(*c1zstore.LedgerActionIdentity)
	}{
		{"op", func(id *c1zstore.LedgerActionIdentity) { id.Op = "list-grants" }},
		{"type", func(id *c1zstore.LedgerActionIdentity) { id.ResourceTypeID = "different" }},
		{"resource", func(id *c1zstore.LedgerActionIdentity) { id.ResourceID = "different" }},
		{"parent-type", func(id *c1zstore.LedgerActionIdentity) { id.ParentResourceTypeID = "different" }},
		{"parent", func(id *c1zstore.LedgerActionIdentity) { id.ParentResourceID = "different" }},
		{"token", func(id *c1zstore.LedgerActionIdentity) { id.PageToken = "different" }},
		{"type-scoped", func(id *c1zstore.LedgerActionIdentity) { id.TypeScoped = false }},
	}
	f := newLedgerFixture(t)
	runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	_, err = runtime.runPage(t.Context(), 0, base, func(_ context.Context, page *ledgerPage) error { return page.transition("") })
	require.NoError(t, err)
	f.audit.enter(ledgerWalk)
	defer f.audit.enter(ledgerLifecycle)
	pending, err := runtime.walk(t.Context(), []ledgerAction{{identity: base}})
	require.NoError(t, err)
	require.Empty(t, pending)
	for _, change := range changes {
		t.Run(change.name, func(t *testing.T) {
			id := base
			change.change(&id)
			pending, err := runtime.walk(t.Context(), []ledgerAction{{identity: id}})
			require.NoError(t, err)
			require.Equal(t, []ledgerAction{{identity: id}}, pending)
		})
	}
}

type ledgerRowReadOverride struct {
	c1zstore.PageLedgerStore
	row *c1zstore.LedgerRow
	err error
}

func (s ledgerRowReadOverride) GetLedgerRow(context.Context, c1zstore.LedgerActionIdentity) (*c1zstore.LedgerRow, bool, error) {
	return s.row, s.row != nil, s.err
}

func TestLedgerWalkRefusesScrubbedPaginationWithoutWrites(t *testing.T) {
	f := newLedgerFixture(t)
	id := c1zstore.LedgerActionIdentity{Op: "list-resources", PageToken: "credential-bearing-page"}
	scrubbed := id
	scrubbed.PageToken = ""
	source := ledgerRowReadOverride{PageLedgerStore: f.ledger, row: &c1zstore.LedgerRow{Identity: scrubbed, Scrubbed: true}}
	runtime, err := newLedgerRuntime(t.Context(), source, "attempt")
	require.NoError(t, err)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	_, err = runtime.walk(t.Context(), []ledgerAction{{identity: id}})
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, errLedgerScrubbedUnfinished)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}
