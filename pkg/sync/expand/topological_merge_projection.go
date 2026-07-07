package expand

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// projectionProgressInterval bounds how often RunTopologicalMergeProjection
// emits progress logs during a large expansion.
const projectionProgressInterval = 15 * time.Second

// RunTopologicalMergeProjection runs topological pull expansion using a
// temporary source projection DB for finalized source streams. The projection
// avoids repeatedly materializing parent grants through permanent grant scans
// while keeping the canonical grant store as the sink.
func (e *Expander) RunTopologicalMergeProjection(ctx context.Context) error {
	plan, err := e.graph.ensureExpansionPlan(ctx)
	if err != nil {
		return err
	}
	projectionSources := projectionSourceSet(plan)
	metrics := &EntitlementGraphMetrics{Algorithm: "topological_projection"}
	e.graph.ExpansionMetrics = metrics

	tempDir, err := os.MkdirTemp("", "baton-expand-projection-*")
	if err != nil {
		return fmt.Errorf("topological projection: temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	projDB, err := pebble.Open(filepath.Join(tempDir, "db"), &pebble.Options{})
	if err != nil {
		return fmt.Errorf("topological projection: open temp db: %w", err)
	}
	defer projDB.Close()

	entitlements, layers, err := e.prepareTopological(ctx)
	if err != nil {
		return err
	}
	totalNodes := 0
	for _, layer := range layers {
		totalNodes += len(layer)
	}

	l := ctxzap.Extract(ctx)
	buildStart := time.Now()
	rows, err := e.buildProjectionDB(ctx, projDB, tempDir, entitlements, projectionSources)
	if err != nil {
		return err
	}
	metrics.ProjectionRowsBuilt += int64(rows)
	l.Info("topological projection: built source projection",
		zap.Int("projection_rows", rows),
		zap.Int("projection_sources", len(projectionSources)),
		zap.Int("nodes", totalNodes),
		zap.Int("layers", len(layers)),
		zap.Duration("elapsed", time.Since(buildStart)),
	)

	expandStart := time.Now()
	lastLog := expandStart
	err = e.driveTopological(ctx, entitlements, layers, topologicalRun{
		reduce: func(ctx context.Context, dest *v2.Entitlement, incoming []topoIncomingEdge, ents map[string]*v2.Entitlement, sink *destinationSink) error {
			return e.mergeDestinationStreams(ctx, dest, incoming, ents, projDB, projectionSources, sink)
		},
		onStored: func(_ context.Context, dirty []*v2.Grant) error {
			addedRows, err := addProjectionRows(projDB, dirty, projectionSources)
			if err != nil {
				return fmt.Errorf("topological projection: append projection rows: %w", err)
			}
			metrics.ProjectionRowsBuilt += int64(addedRows)
			return nil
		},
		onStoredSynth: func(_ context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
			addedRows, err := addProjectionRowsFromSynthesized(projDB, dest, principals, sources, projectionSources)
			if err != nil {
				return fmt.Errorf("topological projection: append synthesized projection rows for %q: %w", dest.GetId(), err)
			}
			metrics.ProjectionRowsBuilt += int64(addedRows)
			return nil
		},
		// The context deadline is the projection's budget: a caller (e.g. a
		// bounded profiling run) sets a timeout, and a cancelled context aborts
		// the pass promptly. There is no separate budget knob in the production
		// path.
		checkBudget: ctx.Err,
		metrics:     metrics,
		progress: func(nodeIdx, nodeTotal int) {
			now := time.Now()
			if now.Sub(lastLog) < projectionProgressInterval {
				return
			}
			lastLog = now
			l.Info("topological projection: progress",
				zap.Int("nodes_processed", nodeIdx),
				zap.Int("nodes_total", nodeTotal),
				zap.Int64("dirty_grants_written", metrics.DirtyGrantsWritten),
				zap.Int64("projection_rows", metrics.ProjectionRowsBuilt),
				zap.Duration("elapsed", now.Sub(expandStart)),
			)
		},
	})
	if err != nil {
		return err
	}

	l.Info("topological projection: expansion complete",
		zap.Int("nodes_total", totalNodes),
		zap.Int64("dirty_grants_written", metrics.DirtyGrantsWritten),
		zap.Int64("projection_rows", metrics.ProjectionRowsBuilt),
		zap.Int64("nodes_reduced", metrics.NodesReduced),
		zap.Duration("elapsed", time.Since(expandStart)),
	)

	e.markExpansionComplete()
	return nil
}

// mergeDestinationStreams reduces one destination entitlement by k-way merging
// its base grants with a contribution stream per incoming source entitlement.
// When projDB is non-nil, sources in projectionSources are read from the scratch
// projection index instead of the canonical store; passing a nil projDB yields
// the pure streaming evaluator.
func (e *Expander) mergeDestinationStreams(
	ctx context.Context,
	destEntitlement *v2.Entitlement,
	incoming []topoIncomingEdge,
	entitlements map[string]*v2.Entitlement,
	projDB *pebble.DB,
	projectionSources map[string]struct{},
	sink *destinationSink,
) error {
	streams := make([]contributionGroupStream, 0, 1+len(incoming))
	streams = append(streams, &baseContributionStream{
		stream: newPrincipalGroupStream(e.store, destEntitlement),
	})
	for _, incomingEdge := range incoming {
		sourceNode, ok := e.graph.Nodes[incomingEdge.sourceNodeID]
		if !ok {
			continue
		}
		for _, sourceID := range sortedCopy(sourceNode.EntitlementIDs) {
			sourceEntitlement := entitlements[sourceID]
			if sourceEntitlement == nil {
				// Drop-don't-fail, but loudly (see the destination-side
				// warning in driveTopologicalLayer).
				ctxzap.Extract(ctx).Warn("topological expansion: source entitlement not in store; its contributions are skipped",
					zap.String("entitlement_id", sourceID))
				continue
			}
			if projDB != nil {
				if _, ok := projectionSources[sourceID]; ok {
					streams = append(streams, newProjectionContributionStream(projDB, sourceID, incomingEdge.edge))
					continue
				}
			}
			streams = append(streams, &contributionStream{
				sourceEntitlementID: sourceID,
				edge:                incomingEdge.edge,
				stream:              newPrincipalGroupStream(e.store, sourceEntitlement),
			})
		}
	}
	return mergeContributionGroupStreams(ctx, destEntitlement, streams, sink)
}

// buildProjectionDB scans the projection-source entitlements once and ingests a
// single sorted SST into projDB. It is memory-bounded: it never holds the whole
// projection in memory.
//
// Entitlements are processed in sorted id order, and the tuple-encoded
// projection key is prefix-ordered by entitlement, so each entitlement's rows
// form a contiguous, globally-ordered block. When the store yields grants in
// principal order (Pebble's entitlement-first primary grant key — see
// ExpanderStore.GrantsForEntitlementPrincipalSorted) the rows arrive already in
// projection-key order and are streamed straight into
// the SST writer with only one page resident. Otherwise (SQLite orders by grant
// id) each entitlement's rows are buffered and sorted before being appended,
// bounding memory to a single entitlement's grants rather than the whole
// projection. The SST writer asserts strict cross-block ordering as a safety net.
func (e *Expander) buildProjectionDB(
	ctx context.Context,
	projDB *pebble.DB,
	tempDir string,
	entitlements map[string]*v2.Entitlement,
	projectionSources map[string]struct{},
) (int, error) {
	entIDs := make([]string, 0, len(projectionSources))
	for entID := range projectionSources {
		if entitlements[entID] == nil {
			continue
		}
		entIDs = append(entIDs, entID)
	}
	sort.Strings(entIDs)

	principalSorted := e.store.GrantsForEntitlementPrincipalSorted()

	sstPath := filepath.Join(tempDir, "projection.sst")
	w, err := newProjectionSSTWriter(sstPath)
	if err != nil {
		return 0, err
	}
	defer w.abort() // no-op once finish() succeeds.

	var block []projectionKV
	for _, entID := range entIDs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		ent := entitlements[entID]
		block = block[:0]
		pageToken := ""
		for {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			resp, err := e.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: ent,
				PageToken:   pageToken,
			}.Build())
			if err != nil {
				return 0, fmt.Errorf("topological projection: list grants for %q: %w", entID, err)
			}
			for _, grant := range resp.GetList() {
				key, val, ok, err := encodeProjectionKV(grant, entID)
				if err != nil {
					return 0, err
				}
				if !ok {
					continue
				}
				if principalSorted {
					// Already in projection-key order; stream directly.
					if err := w.add(key, val); err != nil {
						return 0, err
					}
					continue
				}
				block = append(block, projectionKV{key: key, val: val})
			}
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		if principalSorted {
			continue
		}
		sort.Slice(block, func(i, j int) bool {
			return bytes.Compare(block[i].key, block[j].key) < 0
		})
		for _, kv := range block {
			if err := w.add(kv.key, kv.val); err != nil {
				return 0, err
			}
		}
	}

	if err := w.finish(); err != nil {
		return 0, err
	}
	if w.count == 0 {
		return 0, nil
	}
	if err := projDB.Ingest(ctx, []string{sstPath}); err != nil {
		return 0, fmt.Errorf("topological projection: ingest projection: %w", err)
	}
	return w.count, nil
}

type projectionKV struct {
	key []byte
	val []byte
}

func encodeProjectionKV(grant *v2.Grant, entitlementID string) ([]byte, []byte, bool, error) {
	if grant == nil || grant.GetPrincipal() == nil || grant.GetPrincipal().GetId() == nil {
		return nil, nil, false, nil
	}
	pid := grant.GetPrincipal().GetId()
	key := projectionKey(entitlementID, topoPrincipalKey{
		resourceType: pid.GetResourceType(),
		resource:     pid.GetResource(),
	}, grant.GetId())
	val, err := encodeProjectionValueFromResource(grant.GetPrincipal(), isGrantDirectOnEntitlement(grant, entitlementID))
	if err != nil {
		return nil, nil, false, err
	}
	return key, val, true, nil
}

func addProjectionRows(projDB *pebble.DB, grants []*v2.Grant, projectionSources map[string]struct{}) (int, error) {
	batch := projDB.NewBatch()
	defer batch.Close()
	count := 0
	for _, grant := range grants {
		entID := grant.GetEntitlement().GetId()
		if _, ok := projectionSources[entID]; !ok {
			continue
		}
		key, val, ok, err := encodeProjectionKV(grant, entID)
		if err != nil {
			return 0, err
		}
		if !ok {
			continue
		}
		if err := batch.Set(key, val, nil); err != nil {
			return 0, err
		}
		count++
	}
	if count == 0 {
		return 0, nil
	}
	return count, batch.Commit(pebble.NoSync)
}

func addProjectionRowsFromSynthesized(projDB *pebble.DB, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources, projectionSources map[string]struct{}) (int, error) {
	if dest == nil {
		return 0, nil
	}
	entID := dest.GetId()
	if _, ok := projectionSources[entID]; !ok {
		return 0, nil
	}
	batch := projDB.NewBatch()
	defer batch.Close()
	count := 0
	for i, principal := range principals {
		if principal == nil {
			continue
		}
		if principal.GetResourceTypeId() == "" || principal.GetResourceId() == "" {
			continue
		}
		grantID := grantIDForPrincipalRef(dest, principal)
		key := projectionKey(entID, topoPrincipalKey{
			resourceType: principal.GetResourceTypeId(),
			resource:     principal.GetResourceId(),
		}, grantID)
		val := encodeProjectionValueFromPrincipalRef(principal, sources[i].DirectFor(entID))
		if err := batch.Set(key, val, nil); err != nil {
			return 0, err
		}
		count++
	}
	if count == 0 {
		return 0, nil
	}
	return count, batch.Commit(pebble.NoSync)
}

func encodeProjectionValueFromResource(principal *v2.Resource, direct bool) ([]byte, error) {
	var parent *v2.ResourceId
	if principal != nil {
		parent = principal.GetParentResourceId()
	}
	val := []byte{0}
	if direct {
		val[0] = 1
	}
	val = codec.AppendTupleStrings(val, parent.GetResourceType(), parent.GetResource())
	if principal != nil {
		principalBytes, err := proto.Marshal(principal)
		if err != nil {
			// A dropped payload would silently degrade fallback stores to a
			// refs-only principal reconstruction; surface the failure instead.
			return nil, fmt.Errorf("topological projection: marshal principal payload: %w", err)
		}
		val = codec.AppendTupleSeparator(val)
		val = append(val, principalBytes...)
	}
	return val, nil
}

func encodeProjectionValueFromPrincipalRef(principal *v3.PrincipalRef, direct bool) []byte {
	val := []byte{0}
	if direct {
		val[0] = 1
	}
	if principal == nil {
		return codec.AppendTupleStrings(val, "", "")
	}
	return codec.AppendTupleStrings(val, principal.GetParentResourceTypeId(), principal.GetParentResourceId())
}

func projectionPrincipalRefFromValue(key topoPrincipalKey, val []byte) (principalRefData, []byte, bool) {
	data := principalRefData{
		resourceTypeID: key.resourceType,
		resourceID:     key.resource,
		ok:             true,
	}
	if len(val) <= 1 {
		return data, nil, true
	}
	parentRT, off, ok := codec.DecodeTupleStringAlias(val, 1)
	if !ok {
		return principalRefData{}, nil, false
	}
	if off < len(val) && val[off] == 0 {
		off++
	}
	parentID, off, ok := codec.DecodeTupleStringAlias(val, off)
	if !ok {
		return principalRefData{}, nil, false
	}
	data.parentResourceTypeID = string(parentRT)
	data.parentResourceID = string(parentID)
	var principalBytes []byte
	if off < len(val) && val[off] == 0 {
		principalBytes = val[off+1:]
	}
	return data, principalBytes, true
}

// grantIDForPrincipalRef builds the legacy public grant id (raw concat) —
// byte-identical to what batonGrant.NewGrantID emits. External ids are an
// external-consumer contract; identity is structural and never derives
// from this string.
func grantIDForPrincipalRef(dest *v2.Entitlement, principal *v3.PrincipalRef) string {
	if principal == nil {
		return ""
	}
	return dest.GetId() + ":" + principal.GetResourceTypeId() + ":" + principal.GetResourceId()
}

func projectionSourceSet(plan *EntitlementGraphPlan) map[string]struct{} {
	out := make(map[string]struct{}, len(plan.GetProjectionSources()))
	for _, source := range plan.GetProjectionSources() {
		out[source] = struct{}{}
	}
	return out
}

// projectionKey encodes (entitlement, principal_rt, principal_id, external_id)
// with the engine's order-preserving tuple codec — the same entitlement-first
// ordering as primary grant keys. This keeps the projection key
// unambiguous when any component carries a raw 0x00/0x01 (which the codec
// escapes), and makes the projection key order byte-for-byte identical to
// Pebble's by_entitlement iteration order, so a principal-sorted store yields
// projection rows already in key order.
func projectionKey(entitlementID string, principal topoPrincipalKey, externalID string) []byte {
	buf := make([]byte, 0, len(entitlementID)+len(principal.resourceType)+len(principal.resource)+len(externalID)+8)
	return codec.AppendTupleStrings(buf, entitlementID, principal.resourceType, principal.resource, externalID)
}

// projectionPrefix is the tuple-encoded entitlement component plus a trailing
// separator, so a range scan over [prefix, upperBound) selects exactly the rows
// for one entitlement. The trailing separator prevents an entitlement id from
// matching another that shares it as a prefix.
func projectionPrefix(entitlementID string) []byte {
	buf := make([]byte, 0, len(entitlementID)+2)
	buf = codec.AppendTupleString(buf, entitlementID)
	return codec.AppendTupleSeparator(buf)
}

// projectionSSTWriter incrementally writes a sorted projection SST. Callers feed
// keys via add in non-decreasing order across the whole file (across the
// per-entitlement blocks buildProjectionDB produces); add dedups exactly-equal
// keys and rejects any out-of-order key, so a violated ordering assumption fails
// loud instead of producing a corrupt SST.
type projectionSSTWriter struct {
	path     string
	w        *sstable.Writer
	last     []byte
	count    int
	finished bool
}

func newProjectionSSTWriter(path string) (*projectionSSTWriter, error) {
	file, err := vfs.Default.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{
		BlockSize: 32 << 10,
	})
	return &projectionSSTWriter{path: path, w: w}, nil
}

func (p *projectionSSTWriter) add(key, val []byte) error {
	if len(p.last) > 0 && bytes.Compare(key, p.last) <= 0 {
		if bytes.Equal(key, p.last) {
			return nil
		}
		return fmt.Errorf("topological projection: projection SST keys out of order")
	}
	if err := p.w.Set(key, val); err != nil {
		return err
	}
	p.last = append(p.last[:0], key...)
	p.count++
	return nil
}

func (p *projectionSSTWriter) finish() error {
	if err := p.w.Close(); err != nil {
		return err
	}
	p.finished = true
	return nil
}

// abort closes the writer and removes the partial file unless finish already
// succeeded. Safe to call via defer in all paths.
func (p *projectionSSTWriter) abort() {
	if p.finished {
		return
	}
	_ = p.w.Close()
	_ = os.Remove(p.path)
}

type projectionContributionStream struct {
	db       *pebble.DB
	sourceID string
	edge     Edge
	prefix   []byte
	iter     *pebble.Iterator
	started  bool
	valid    bool
	// contrib is reused across groups: the k-way merge fully consumes a
	// stream's current group before pulling the next one from that stream, so
	// at most one group per stream is live at a time. The PrincipalRef handed
	// to it is still allocated per group because downstream sinks retain it.
	contrib topoContribution
}

func newProjectionContributionStream(db *pebble.DB, sourceID string, edge Edge) *projectionContributionStream {
	return &projectionContributionStream{
		db:       db,
		sourceID: sourceID,
		edge:     edge,
		prefix:   projectionPrefix(sourceID),
	}
}

func (s *projectionContributionStream) next(_ context.Context) (contributionGroup, bool, error) {
	if !s.started {
		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: s.prefix,
			UpperBound: codec.KeyUpperBound(s.prefix),
		})
		if err != nil {
			return contributionGroup{}, false, err
		}
		s.iter = iter
		s.valid = iter.First()
		s.started = true
	}
	// Do NOT advance here: the inner loop below leaves the iterator parked on
	// the next unconsumed principal key after each emitted group, so advancing
	// at the top would skip every other principal.

	for s.valid {
		rt, resource, ok := decodeProjectionPrincipal(s.iter.Key(), s.prefix)
		if !ok {
			s.valid = s.iter.Next()
			continue
		}
		// rt/resource alias the iterator key; string(b)==s comparisons do not
		// allocate. All rows in a principal group share the same rt, so the
		// filter only needs to run on the group's first row.
		if len(s.edge.ResourceTypeIDs) > 0 && !resourceTypeAllowed(s.edge.ResourceTypeIDs, rt) {
			s.valid = s.iter.Next()
			continue
		}
		val := s.iter.Value()
		isDirect := len(val) > 0 && val[0] == 1
		if s.edge.IsShallow && !isDirect {
			s.valid = s.iter.Next()
			continue
		}
		key := topoPrincipalKey{resourceType: string(rt), resource: string(resource)}
		principalData, principalBytes, ok := projectionPrincipalRefFromValue(key, val)
		if !ok {
			return contributionGroup{}, false, fmt.Errorf("topological projection: decode principal ref for source %q", s.sourceID)
		}
		s.contrib.resetForReuse()
		s.contrib.principal.setRef(principalData, principalBytes)
		s.contrib.addSource(s.sourceID, isDirect)
		for s.valid = s.iter.Next(); s.valid; s.valid = s.iter.Next() {
			nextRT, nextResource, ok := decodeProjectionPrincipal(s.iter.Key(), s.prefix)
			if !ok {
				continue
			}
			if string(nextRT) != key.resourceType || string(nextResource) != key.resource {
				break
			}
			nextVal := s.iter.Value()
			nextDirect := len(nextVal) > 0 && nextVal[0] == 1
			if s.edge.IsShallow && !nextDirect {
				continue
			}
			s.contrib.addSource(s.sourceID, nextDirect)
		}
		return contributionGroup{key: key, contrib: &s.contrib}, true, nil
	}
	if s.iter == nil {
		return contributionGroup{}, false, nil
	}
	if err := s.iter.Error(); err != nil {
		return contributionGroup{}, false, err
	}
	return contributionGroup{}, false, nil
}

func (s *projectionContributionStream) close() error {
	if s.iter == nil {
		return nil
	}
	return s.iter.Close()
}

// decodeProjectionPrincipal extracts the principal (resource_type, resource)
// from a tuple-encoded projection key, validating the encoder's
// rt | resource | external_id layout: two separators and a non-empty external
// id. It delegates separator/escape handling to the canonical codec via
// DecodeTupleStringAlias, which aliases key with no allocation when a component
// has no escape byte (the overwhelmingly common case for ids). Aliased slices
// are only valid until the iterator that produced key advances.
func decodeProjectionPrincipal(key, prefix []byte) ([]byte, []byte, bool) {
	if len(key) <= len(prefix) {
		return nil, nil, false
	}
	tail := key[len(prefix):]
	rt, n1, ok := codec.DecodeTupleStringAlias(tail, 0)
	if !ok || n1 >= len(tail) {
		return nil, nil, false
	}
	id, n2, ok := codec.DecodeTupleStringAlias(tail, n1+1)
	if !ok || n2 >= len(tail) || len(tail[n2+1:]) == 0 {
		return nil, nil, false
	}
	return rt, id, true
}

// resourceTypeAllowed reports whether rt is in allowed. string(rt)==entry is a
// comparison the compiler performs without allocating a string for rt.
func resourceTypeAllowed(allowed []string, rt []byte) bool {
	for _, entry := range allowed {
		if string(rt) == entry {
			return true
		}
	}
	return false
}
