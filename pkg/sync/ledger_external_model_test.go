package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type externalStreamingProbe struct {
	connectorstore.Reader
	destination c1zstore.Store
	t           *testing.T
	pages       int
}

func (p *externalStreamingProbe) ListGrantsForEntitlement(
	ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	if req.GetPageToken() != "" {
		prior, err := p.destination.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{Entitlement: req.GetEntitlement()}.Build())
		require.NoError(p.t, err)
		require.NotEmpty(p.t, prior.GetList(), "imported page must be written before fetching the next page")
	}
	p.pages++
	req = proto.CloneOf(req)
	req.SetPageSize(1)
	return p.Reader.ListGrantsForEntitlement(ctx, req)
}

func TestLedgerExternalImportsPagesDirectly(t *testing.T) {
	s, f, source := externalPageFixture(t, true)
	fresh := externalMatchPrincipal(t, "fresh", nil)
	for _, id := range []string{"one", "two", "three"} {
		require.NoError(t, source.store.PutGrants(t.Context(), gt.NewGrant(fresh, "member", v2.ResourceId_builder{ResourceType: "user", Resource: id}.Build())))
	}
	probe := &externalStreamingProbe{Reader: source.store, destination: f.store, t: t}
	s.externalResourceReader = probe
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	require.Greater(t, probe.pages, 1)
	_, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(&Action{Op: SyncExternalResourcesOp}))
	require.NoError(t, err)
	require.False(t, found)
	require.EqualValues(t, 1, s.ledger.accounting.snapshot().Counters[ledgerCompletedPrefix+SyncExternalResourcesOp.String()])
}

func ledgerGrantIdentity(grant *v2.Grant) [6]string {
	ent := grant.GetEntitlement()
	return [6]string{grant.GetId(), ent.GetId(), ent.GetResource().GetId().GetResourceType(), ent.GetResource().GetId().GetResource(),
		grant.GetPrincipal().GetId().GetResourceType(), grant.GetPrincipal().GetId().GetResource()}
}
