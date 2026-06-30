package expand

import (
	"container/heap"
	"context"
	"fmt"
	"sort"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

// RunTopologicalMergeStreaming is the memory-bounded pull evaluator. It keeps
// only one principal group per active input stream, instead of materializing all
// source entitlement grant groups into maps for each destination.
func (e *Expander) RunTopologicalMergeStreaming(ctx context.Context) error {
	entitlements, order, err := e.prepareTopological(ctx)
	if err != nil {
		return err
	}
	if err := e.driveTopological(ctx, entitlements, order, topologicalRun{
		reduce: func(ctx context.Context, dest *v2.Entitlement, incoming []topoIncomingEdge, ents map[string]*v2.Entitlement, sink destinationSink) error {
			return e.mergeDestinationStreams(ctx, dest, incoming, ents, nil, nil, sink, nil)
		},
	}); err != nil {
		return err
	}
	e.markExpansionComplete()
	return nil
}

type principalGrantGroup struct {
	key    topoPrincipalKey
	grants []*v2.Grant
}

// principalGroupStream yields a destination/source entitlement's grants
// grouped by principal, in strictly increasing principal-key order. The k-way
// merge in mergeContributionGroupStreams relies on that ordering invariant.
type principalGroupStream interface {
	next(context.Context) (principalGrantGroup, bool, error)
}

// newPrincipalGroupStream picks the cheapest correct grouping strategy for the
// store. When the store guarantees principal-sorted rows (Pebble) we stream
// page-by-page, holding only the current principal group. Otherwise we fall
// back to buffering the full entitlement and sorting it in memory so the k-way
// merge's sorted-input invariant holds for SQLite and test doubles.
func newPrincipalGroupStream(store ExpanderStore, entitlement *v2.Entitlement) principalGroupStream {
	if store.GrantsForEntitlementPrincipalSorted() {
		return &streamingPrincipalGroupStream{store: store, entitlement: entitlement}
	}
	return &sortingPrincipalGroupStream{store: store, entitlement: entitlement}
}

// streamingPrincipalGroupStream yields an entitlement's grants grouped by
// principal by paging through ListGrantsForEntitlement and relying on the
// store's principal-sorted order. Memory is bounded to one page plus the
// current principal group rather than the whole entitlement. It asserts the
// sort invariant and fails loud if the store ever violates it.
type streamingPrincipalGroupStream struct {
	store       ExpanderStore
	entitlement *v2.Entitlement
	pageToken   string
	exhausted   bool
	buf         []*v2.Grant
	pos         int
	pending     *v2.Grant
	lastKey     topoPrincipalKey
	haveLast    bool
}

// rawNext returns the next grant, honoring any pushed-back grant and paging
// from the store as needed.
func (s *streamingPrincipalGroupStream) rawNext(ctx context.Context) (*v2.Grant, bool, error) {
	if s.pending != nil {
		g := s.pending
		s.pending = nil
		return g, true, nil
	}
	for s.pos >= len(s.buf) {
		if s.exhausted {
			return nil, false, nil
		}
		resp, err := s.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: s.entitlement,
			PageToken:   s.pageToken,
		}.Build())
		if err != nil {
			return nil, false, err
		}
		s.buf = resp.GetList()
		s.pos = 0
		s.pageToken = resp.GetNextPageToken()
		if s.pageToken == "" {
			s.exhausted = true
		}
	}
	g := s.buf[s.pos]
	s.pos++
	return g, true, nil
}

func (s *streamingPrincipalGroupStream) next(ctx context.Context) (principalGrantGroup, bool, error) {
	var group principalGrantGroup
	haveGroup := false
	for {
		g, ok, err := s.rawNext(ctx)
		if err != nil {
			return principalGrantGroup{}, false, err
		}
		if !ok {
			break
		}
		key, kok := principalKeyFromResource(g.GetPrincipal())
		if !kok {
			continue
		}
		if !haveGroup {
			if s.haveLast && !principalKeyLess(s.lastKey, key) {
				return principalGrantGroup{}, false, fmt.Errorf(
					"streamingPrincipalGroupStream: entitlement %q grants not principal-sorted (saw %v after %v)",
					s.entitlement.GetId(), key, s.lastKey)
			}
			group = principalGrantGroup{key: key, grants: []*v2.Grant{g}}
			haveGroup = true
			continue
		}
		if key == group.key {
			group.grants = append(group.grants, g)
			continue
		}
		s.pending = g
		break
	}
	if !haveGroup {
		return principalGrantGroup{}, false, nil
	}
	s.lastKey = group.key
	s.haveLast = true
	return group, true, nil
}

// sortingPrincipalGroupStream materializes an entitlement's grants via
// ListGrantsForEntitlement and yields them grouped by principal.
//
// It buffers the full entitlement and sorts by principal key before grouping
// rather than assuming the store returns principal-sorted rows. Used for the
// SQLite reader and in-memory test doubles, which do not guarantee
// principal-sorted output. Memory is O(grants on the entitlement); the Pebble
// path uses the streaming variant instead and stays page-bounded.
type sortingPrincipalGroupStream struct {
	store       ExpanderStore
	entitlement *v2.Entitlement
	grants      []*v2.Grant
	loaded      bool
	pos         int
}

func (s *sortingPrincipalGroupStream) load(ctx context.Context) error {
	pageToken := ""
	for {
		resp, err := s.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: s.entitlement,
			PageToken:   pageToken,
		}.Build())
		if err != nil {
			return err
		}
		s.grants = append(s.grants, resp.GetList()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	sort.SliceStable(s.grants, func(i, j int) bool {
		ki, okI := principalKeyFromResource(s.grants[i].GetPrincipal())
		kj, okJ := principalKeyFromResource(s.grants[j].GetPrincipal())
		if okI != okJ {
			return okI
		}
		if okI && ki != kj {
			return principalKeyLess(ki, kj)
		}
		return s.grants[i].GetId() < s.grants[j].GetId()
	})
	s.loaded = true
	return nil
}

func (s *sortingPrincipalGroupStream) next(ctx context.Context) (principalGrantGroup, bool, error) {
	if !s.loaded {
		if err := s.load(ctx); err != nil {
			return principalGrantGroup{}, false, err
		}
	}
	for s.pos < len(s.grants) {
		key, ok := principalKeyFromResource(s.grants[s.pos].GetPrincipal())
		if !ok {
			s.pos++
			continue
		}
		group := principalGrantGroup{key: key}
		for s.pos < len(s.grants) {
			g := s.grants[s.pos]
			gk, gok := principalKeyFromResource(g.GetPrincipal())
			if !gok {
				s.pos++
				continue
			}
			if gk != key {
				break
			}
			group.grants = append(group.grants, g)
			s.pos++
		}
		return group, true, nil
	}
	return principalGrantGroup{}, false, nil
}

type contributionGroup struct {
	key     topoPrincipalKey
	contrib *topoContribution
	base    []*v2.Grant
	isBase  bool
}

type contributionGroupStream interface {
	next(context.Context) (contributionGroup, bool, error)
	close() error
}

type contributionStream struct {
	sourceEntitlementID string
	edge                Edge
	stream              principalGroupStream
}

func (s *contributionStream) next(ctx context.Context) (contributionGroup, bool, error) {
	for {
		group, ok, err := s.stream.next(ctx)
		if err != nil || !ok {
			return contributionGroup{}, ok, err
		}
		contrib := &topoContribution{}
		for _, grant := range group.grants {
			if !grantContributesOverEdge(grant, s.sourceEntitlementID, s.edge) {
				continue
			}
			contrib.add(s.sourceEntitlementID, isGrantDirectOnEntitlement(grant, s.sourceEntitlementID), grant.GetPrincipal())
		}
		if len(contrib.sources) == 0 {
			continue
		}
		return contributionGroup{key: group.key, contrib: contrib}, true, nil
	}
}

func (s *contributionStream) close() error { return nil }

type baseContributionStream struct {
	stream principalGroupStream
}

func (s *baseContributionStream) next(ctx context.Context) (contributionGroup, bool, error) {
	group, ok, err := s.stream.next(ctx)
	if err != nil || !ok {
		return contributionGroup{}, ok, err
	}
	return contributionGroup{key: group.key, base: group.grants, isBase: true}, true, nil
}

func (s *baseContributionStream) close() error { return nil }

type mergeHeapItem struct {
	group    contributionGroup
	streamID int
}

type mergeHeap []mergeHeapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].group.key != h[j].group.key {
		return principalKeyLess(h[i].group.key, h[j].group.key)
	}
	return h[i].streamID < h[j].streamID
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeHeapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// mergeContributionGroupStreams k-way merges contribution streams by principal
// key. Every input stream MUST yield groups in strictly increasing principal
// key order (see principalGroupStream / projectionContributionStream); the heap
// surfaces the minimum key and collects all streams positioned at it.
func mergeContributionGroupStreams(
	ctx context.Context,
	destEntitlement *v2.Entitlement,
	streams []contributionGroupStream,
	sink destinationSink,
	metrics *EntitlementGraphMetrics,
) error {
	for _, stream := range streams {
		defer stream.close()
	}
	h := make(mergeHeap, 0, len(streams))
	for i, stream := range streams {
		group, ok, err := stream.next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		heap.Push(&h, mergeHeapItem{group: group, streamID: i})
	}

	flusher := newDirtyFlusher(sink)

	// Reuse the per-principal accumulator (struct + sources map) and the base
	// slice across iterations rather than allocating fresh ones per output
	// group — there are tens of millions of groups on a large expansion, and
	// that churn was a measurable share of GC time. Safe because
	// newExpandedGrantWithSources and mergeContributionIntoExistingGrant copy
	// out of contrib.sources, and the output grant holds its own reference to
	// the principal, so clearing contrib after each group keeps nothing alive.
	contrib := &topoContribution{}
	var base []*v2.Grant
	consume := func(group contributionGroup) {
		if group.isBase {
			base = append(base, group.base...)
			return
		}
		contrib.merge(group.contrib)
	}
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := heap.Pop(&h).(mergeHeapItem)
		key := item.group.key
		base = base[:0]
		if contrib.sources != nil {
			clear(contrib.sources)
		}
		contrib.principal = nil
		contrib.principalBytes = nil

		consume(item.group)

		if next, ok, err := streams[item.streamID].next(ctx); err != nil {
			return err
		} else if ok {
			heap.Push(&h, mergeHeapItem{group: next, streamID: item.streamID})
		}

		for h.Len() > 0 && h[0].group.key == key {
			same := heap.Pop(&h).(mergeHeapItem)
			consume(same.group)
			if next, ok, err := streams[same.streamID].next(ctx); err != nil {
				return err
			} else if ok {
				heap.Push(&h, mergeHeapItem{group: next, streamID: same.streamID})
			}
		}

		if len(contrib.sources) == 0 {
			continue
		}
		if len(base) == 0 {
			principal, err := contrib.principalResource()
			if err != nil {
				return err
			}
			if principal == nil {
				continue
			}
			grant, err := newExpandedGrantWithSources(destEntitlement, principal, contrib.sources)
			if err != nil {
				return err
			}
			// Synthesized: base stream reported no grant for this principal on
			// the destination, so the deterministic external_id is brand-new and
			// the store can skip its read-before-write Get.
			if err := flusher.add(ctx, grant, true); err != nil {
				return err
			}
			if metrics != nil {
				metrics.SynthesizedGrants++
			}
			continue
		}
		for _, baseGrant := range base {
			updated := mergeContributionIntoExistingGrant(baseGrant, destEntitlement.GetId(), contrib.sources)
			if updated != nil {
				// Base update: rewrites an existing grant's Sources, so a prior
				// record exists and the store must take the read-merge path.
				if err := flusher.add(ctx, updated, false); err != nil {
					return err
				}
				if metrics != nil {
					metrics.BaseUpdateGrants++
				}
			}
		}
	}
	return flusher.flush(ctx)
}
