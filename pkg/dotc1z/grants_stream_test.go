package dotc1z

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

// streamTestFixture mirrors setupBenchmarkDB but is testing.T-friendly.
type streamTestFixture struct {
	f             *C1File
	entitlementID string
	resourceType  *v2.ResourceType
}

func newStreamFixture(t *testing.T, numGrants int) *streamTestFixture {
	t.Helper()
	ctx := t.Context()
	tempDir := t.TempDir()

	f, err := NewC1ZFile(ctx, tempDir+"/test.c1z",
		WithTmpDir(tempDir),
		WithEncoderConcurrency(0),
		WithDecoderOptions(WithDecoderConcurrency(0)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close(ctx) })

	_, _, err = f.StartOrResumeSync(ctx, "full", "")
	require.NoError(t, err)

	rt := &v2.ResourceType{Id: "test-type", DisplayName: "Test Type"}
	require.NoError(t, f.PutResourceTypes(ctx, rt))

	res := &v2.Resource{
		Id:          &v2.ResourceId{ResourceType: "test-type", Resource: "test-resource"},
		DisplayName: "Test Resource",
	}
	require.NoError(t, f.PutResources(ctx, res))

	ent := &v2.Entitlement{Id: "test-entitlement", Resource: res}
	require.NoError(t, f.PutEntitlements(ctx, ent))

	grants := make([]*v2.Grant, numGrants)
	for i := range numGrants {
		grants[i] = &v2.Grant{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: ent,
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "test-type",
					Resource:     fmt.Sprintf("principal-%d", i),
				},
			},
		}
	}
	batchSize := 1000
	for i := 0; i < len(grants); i += batchSize {
		end := min(i+batchSize, len(grants))
		require.NoError(t, f.PutGrants(ctx, grants[i:end]...))
	}

	return &streamTestFixture{f: f, entitlementID: ent.Id, resourceType: rt}
}

func (fx *streamTestFixture) entitlement(t *testing.T) *v2.Entitlement {
	t.Helper()
	ctx := t.Context()
	resp, err := fx.f.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: fx.entitlementID,
	}.Build())
	require.NoError(t, err)
	return resp.GetEntitlement()
}

// collectSlice returns every grant for the entitlement via the non-streaming RPC,
// paging until exhausted. This is the reference implementation the streaming
// variant must match.
func (fx *streamTestFixture) collectSlice(t *testing.T) []*v2.Grant {
	t.Helper()
	ctx := t.Context()
	var out []*v2.Grant
	pageToken := ""
	for {
		resp, err := fx.f.ListGrantsForEntitlement(ctx, &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
			Entitlement: fx.entitlement(t),
			PageSize:    1000,
			PageToken:   pageToken,
		})
		require.NoError(t, err)
		out = append(out, resp.GetList()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return out
		}
	}
}

// TestListGrantsForEntitlementStream_ParityWithSlice is the equivalence gate for
// OPS-1483: streaming must visit the exact same grants in the same order as the
// paginated slice path, exactly once each.
func TestListGrantsForEntitlementStream_ParityWithSlice(t *testing.T) {
	// 2500 grants > 2× grantStreamPageSize forces multi-page paging in stream.
	fx := newStreamFixture(t, 2500)

	wantGrants := fx.collectSlice(t)
	wantIDs := make([]string, len(wantGrants))
	for i, g := range wantGrants {
		wantIDs[i] = g.GetId()
	}

	var gotIDs []string
	err := fx.f.ListGrantsForEntitlementStream(
		t.Context(),
		&reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
			Entitlement: fx.entitlement(t),
		},
		func(g *v2.Grant) error {
			gotIDs = append(gotIDs, g.GetId())
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, wantIDs, gotIDs,
		"stream visited a different set/order of grants than the slice path")
}

// TestListGrantsForEntitlementStream_ScanErrorPropagates asserts a scan error
// stops iteration immediately and the error bubbles up.
func TestListGrantsForEntitlementStream_ScanErrorPropagates(t *testing.T) {
	fx := newStreamFixture(t, 2500)
	sentinel := errors.New("stop")

	count := 0
	err := fx.f.ListGrantsForEntitlementStream(
		t.Context(),
		&reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
			Entitlement: fx.entitlement(t),
		},
		func(g *v2.Grant) error {
			count++
			if count == 5 {
				return sentinel
			}
			return nil
		},
	)
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 5, count,
		"stream did not halt immediately on scan error; visited %d grants instead of 5", count)
}

// TestListGrantsForEntitlementStream_NilScanRejected guards the documented
// contract that scan must be non-nil.
func TestListGrantsForEntitlementStream_NilScanRejected(t *testing.T) {
	fx := newStreamFixture(t, 10)
	err := fx.f.ListGrantsForEntitlementStream(
		t.Context(),
		&reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
			Entitlement: fx.entitlement(t),
		},
		nil,
	)
	require.Error(t, err)
}

// TestListGrantsForEntitlementStream_NoDeadlockOnReentrantCall is the regression
// test for baton-sdk PR #619's failure mode. The stream MUST release its SQL
// cursor between batches so user code in scan can safely re-enter *C1File
// methods (e.g. GetResourceType) without deadlocking against the pinned conn.
func TestListGrantsForEntitlementStream_NoDeadlockOnReentrantCall(t *testing.T) {
	const n = 2500
	fx := newStreamFixture(t, n)

	count := 0
	err := fx.f.ListGrantsForEntitlementStream(
		t.Context(),
		&reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
			Entitlement: fx.entitlement(t),
		},
		func(g *v2.Grant) error {
			count++
			// Re-enter C1File from inside the iterator. With the single-conn
			// pin this would deadlock if the stream were holding a rows cursor.
			resp, err := fx.f.GetResourceType(t.Context(), reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
				ResourceTypeId: g.GetPrincipal().GetId().GetResourceType(),
			}.Build())
			if err != nil {
				return fmt.Errorf("re-entrant GetResourceType failed: %w", err)
			}
			require.Equal(t, "test-type", resp.GetResourceType().GetId())
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, n, count)
}
