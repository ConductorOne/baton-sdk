package expand

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

// fanoutStore is a self-contained in-memory ExpanderStore for the
// high-out-degree (read-amplification) scenario: one hot source entitlement
// with `members` grants, fanned out to `dests` destination entitlements.
//
// It deliberately holds only the source's grants (built once) and DISCARDS
// written grants (counting them) so memory stays bounded even at 10M expanded
// grants. Destinations report no existing grants, so every source grant
// expands to every destination (full fan-out, identical write volume for both
// the old and new expander — the only difference is how many times the source
// is read).
type fanoutStore struct {
	sourceEntID  string
	sourceGrants []*v2.Grant
	srcReads     atomic.Int64 // ListGrantsForEntitlement calls against the hot source
	written      atomic.Int64 // expanded grants handed to StoreExpandedGrants
}

func newFanoutStore(sourceEntID string, members int) *fanoutStore {
	grants := make([]*v2.Grant, members)
	srcEnt := v2.Entitlement_builder{Id: sourceEntID}.Build()
	for i := 0; i < members; i++ {
		principal := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u" + strconv.Itoa(i)}.Build(),
		}.Build()
		grants[i] = v2.Grant_builder{
			Id:          "g-src-" + strconv.Itoa(i),
			Entitlement: srcEnt,
			Principal:   principal,
		}.Build()
	}
	return &fanoutStore{sourceEntID: sourceEntID, sourceGrants: grants}
}

func (s *fanoutStore) GetEntitlement(
	_ context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	id := req.GetEntitlementId()
	ent := v2.Entitlement_builder{
		Id: id,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: id}.Build(),
		}.Build(),
	}.Build()
	return reader_v2.EntitlementsReaderServiceGetEntitlementResponse_builder{Entitlement: ent}.Build(), nil
}

func (s *fanoutStore) ListGrantsForEntitlement(
	_ context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	if req.GetEntitlement().GetId() != s.sourceEntID {
		// Destination entitlements have no existing grants yet.
		return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{}.Build(), nil
	}
	// The hot source: single page of all member grants. Every call here is a
	// re-read of the source — the cost the "group by source" change removes.
	s.srcReads.Add(1)
	return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List: s.sourceGrants,
	}.Build(), nil
}

func (s *fanoutStore) StoreExpandedGrants(_ context.Context, grants ...*v2.Grant) error {
	s.written.Add(int64(len(grants)))
	return nil // discard: keeps memory bounded at 10M scale
}

// GrantsForEntitlementPrincipalSorted is false: this store exercises the
// source-batched expander's read-amplification path, and its member grants are
// not emitted in principal order.
func (s *fanoutStore) GrantsForEntitlementPrincipalSorted() bool { return false }

// benchmarkFanout builds a 1-source -> N-dest graph and expands it once per op.
func benchmarkFanout(b *testing.B, members, dests int) {
	ctx := context.Background()
	const sourceEntID = "group:everyone:member"

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store := newFanoutStore(sourceEntID, members)
		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(sourceEntID)
		for d := 0; d < dests; d++ {
			dst := "role:" + strconv.Itoa(d) + ":assigned"
			graph.AddEntitlementID(dst)
			if err := graph.AddEdge(ctx, sourceEntID, dst, false, nil); err != nil {
				b.Fatalf("AddEdge: %v", err)
			}
		}
		graph.Loaded = true
		b.StartTimer()

		expander := NewExpander(store, graph)
		if err := expander.Run(ctx); err != nil {
			b.Fatalf("expand: %v", err)
		}

		b.StopTimer()
		// Report the two metrics the design doc cares about:
		//   src_reads/op  -> read amplification (should be ~dests for old, ~1 for new)
		//   written/op    -> expanded grant volume (should be identical: members*dests)
		b.ReportMetric(float64(store.srcReads.Load()), "src_reads/op")
		b.ReportMetric(float64(store.written.Load()), "written/op")
		b.StartTimer()
	}
}

// 10k members x 1000 dests = 10,000,000 expanded grants.
func BenchmarkExpandFanout10M(b *testing.B) {
	benchmarkFanout(b, 10000, 1000)
}
