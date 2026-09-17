package pebble

import (
	"context"
	"fmt"
	"maps"
	"math"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type pageWriter struct {
	e      *Engine
	syncID string
	unit   *pageUnit
}

// The sync id is captured at begin so the page translates against the sync
// it was started in.
func (l *Ledger) BeginPage() c1zstore.PageWriter {
	return &pageWriter{e: l.e, syncID: l.e.CurrentSyncID(), unit: l.newPageUnit()}
}

func (w *pageWriter) requireSync() error {
	if w.syncID == "" {
		return ErrNoCurrentSync
	}
	return nil
}

func (w *pageWriter) PutResourceTypes(ctx context.Context, rts ...*v2.ResourceType) error {
	if err := w.requireSync(); err != nil {
		return err
	}
	return w.unit.StageResourceTypes(translateResourceTypesForPut(w.syncID, rts)...)
}

func (w *pageWriter) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	if err := w.requireSync(); err != nil {
		return err
	}
	return w.unit.StageResources(translateResourcesForPut(ctx, w.syncID, resources)...)
}

func (w *pageWriter) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	if err := w.requireSync(); err != nil {
		return err
	}
	return w.unit.StageEntitlements(translateEntitlementsForPut(ctx, w.syncID, entitlements)...)
}

func (w *pageWriter) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	if err := w.requireSync(); err != nil {
		return err
	}
	return w.unit.StageGrants(translateGrantsForPut(ctx, w.syncID, grants)...)
}

func (w *pageWriter) GetResource(ctx context.Context, resourceTypeID, resourceID string) (*v2.Resource, error) {
	rec, err := w.unit.resourceRecord(ctx, resourceTypeID, resourceID)
	if err = c1zstore.AdaptNotFound(err, pebble.ErrNotFound); err != nil {
		return nil, err
	}
	return V3ResourceToV2(rec), nil
}

func (w *pageWriter) GetEntitlement(ctx context.Context, entitlementID string) (*v2.Entitlement, error) {
	rec, err := w.unit.entitlementRecord(ctx, entitlementID)
	if err = c1zstore.AdaptNotFound(err, pebble.ErrNotFound); err != nil {
		return nil, err
	}
	return V3EntitlementToV2(rec), nil
}

func (w *pageWriter) DeleteGrants(ctx context.Context, grants ...*v2.Grant) error {
	if err := w.requireSync(); err != nil {
		return err
	}
	recs := make([]*v3.GrantRecord, 0, len(grants))
	for _, g := range grants {
		recs = append(recs, V2GrantToV3(w.syncID, g))
	}
	return w.unit.StageGrantDeletes(recs...)
}

func (w *pageWriter) DropStagedSourceCacheRows(kind sourcecache.RowKind, scopeKey string, canonicalIDs, principalIDs []string) (int, error) {
	return w.unit.DropStagedRows(string(kind), scopeKey, canonicalIDs, principalIDs)
}

func (w *pageWriter) SetFact(name string) error { return w.unit.StageFact(name) }

func (w *pageWriter) SetFactValue(name, value string) error {
	return w.unit.StageFactValue(name, value)
}

func (w *pageWriter) SetCounterBucket(runID string, worker uint32, counters c1zstore.LedgerCounters) error {
	return w.unit.StageCounterBucket(runID, worker, ledgerCountersToProto(counters))
}

func (w *pageWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	if err := w.requireSync(); err != nil {
		return err
	}
	if err := w.unit.Commit(ctx, id, ledgerRowToProto(row)); err != nil {
		return fmt.Errorf("page commit: %w", err)
	}
	return nil
}

func (w *pageWriter) Discard() { w.unit.Discard() }

func syncStatsOverlay(stats c1zstore.SyncStats) *v3.SyncStatsRecord {
	overlay := v3.SyncStatsRecord_builder{
		StepDurationsMs:    cloneInt64Map(stats.Run.StepDurationsMs),
		ConnectorCallStats: callStatsToProto(stats.Run.ConnectorCallStats),
		SessionStoreStats:  callStatsToProto(stats.Run.SessionStoreStats),
	}.Build()
	if q := stats.IngestQuality; q != nil {
		overlay.SetIngestQuality(v3.IngestQualityStats_builder{
			SourceCacheReplayBlocked:      q.SourceCacheReplayBlocked,
			EntitlementsDropped:           q.EntitlementsDropped,
			GrantsDropped:                 q.GrantsDropped,
			GrantResourcesDropped:         q.GrantResourcesDropped,
			ExpansionResourceTypesDropped: q.ExpansionResourceTypesDropped,
			ExpansionsDropped:             q.ExpansionsDropped,
			InvalidResourceTypesObserved:  q.InvalidResourceTypesObserved,
			InvalidResourcesObserved:      q.InvalidResourcesObserved,
			InvalidEntitlementsObserved:   q.InvalidEntitlementsObserved,
			ReasonFlags:                   q.ReasonFlags,
		}.Build())
	}
	return overlay
}

func cloneInt64Map(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func callStatsToProto(in map[string]c1zstore.CallStat) map[string]*v3.CallStat {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*v3.CallStat, len(in))
	for k, v := range in {
		out[k] = v3.CallStat_builder{Count: v.Count, TotalMs: v.TotalMs, MaxMs: v.MaxMs, Errors: v.Errors, Timeouts: v.Timeouts}.Build()
	}
	return out
}

func callStatsFromProto(in map[string]*v3.CallStat) map[string]c1zstore.CallStat {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]c1zstore.CallStat, len(in))
	for k, v := range in {
		out[k] = c1zstore.CallStat{Count: v.GetCount(), TotalMs: v.GetTotalMs(), MaxMs: v.GetMaxMs(), Errors: v.GetErrors(), Timeouts: v.GetTimeouts()}
	}
	return out
}

func msToDuration(ms uint64) time.Duration {
	const maxMs = uint64(math.MaxInt64 / int64(time.Millisecond))
	if ms > maxMs {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ms) * time.Millisecond
}

// Copies every map: the bucket is held until Commit while the caller keeps
// mutating its cache.
func ledgerCountersToProto(c c1zstore.LedgerCounters) *v3.LedgerCounterBucket {
	return v3.LedgerCounterBucket_builder{
		Counters:        maps.Clone(c.Counters),
		Flags:           c.Flags,
		ConnectorCalls:  callStatsToProto(c.ConnectorCalls),
		StepDurationsMs: cloneInt64Map(c.StepDurationsMs),
		SessionCalls:    callStatsToProto(c.SessionCalls),
	}.Build()
}

func ledgerCountersFromProto(b *v3.LedgerCounterBucket) c1zstore.LedgerCounters {
	out := c1zstore.LedgerCounters{
		Counters:        map[string]uint64{},
		Flags:           b.GetFlags(),
		ConnectorCalls:  callStatsFromProto(b.GetConnectorCalls()),
		StepDurationsMs: cloneInt64Map(b.GetStepDurationsMs()),
		SessionCalls:    callStatsFromProto(b.GetSessionCalls()),
	}
	for k, v := range b.GetCounters() {
		out.Counters[k] = v
	}
	return out
}

func ledgerRowToProto(row *c1zstore.LedgerRow) *v3.LedgerRow {
	if row == nil {
		return nil
	}
	children := make([]*v3.LedgerActionIdentity, 0, len(row.Children))
	for _, c := range row.Children {
		children = append(children, ledgerIdentityToProto(c))
	}
	b := v3.LedgerRow_builder{
		NextPageToken:     row.NextPageToken,
		Children:          children,
		Attempt:           row.Attempt,
		Replayed:          row.Replayed,
		TypeScopedPlanned: row.TypeScopedPlanned,
		PageMs:            uint64(max(row.PageDuration.Milliseconds(), 0)),
		ConnectorMs:       uint64(max(row.ConnectorDuration.Milliseconds(), 0)),
		WaitMs:            uint64(max(row.WaitDuration.Milliseconds(), 0)),
	}
	if !row.CommittedAt.IsZero() {
		b.CommittedAt = timestamppb.New(row.CommittedAt)
	}
	return b.Build()
}

func ledgerRowFromProto(p *v3.LedgerRow) *c1zstore.LedgerRow {
	children := make([]c1zstore.LedgerActionIdentity, 0, len(p.GetChildren()))
	for _, c := range p.GetChildren() {
		children = append(children, ledgerIdentityFromProto(c))
	}
	row := &c1zstore.LedgerRow{
		Identity:             ledgerIdentityFromProto(p.GetIdentity()),
		NextPageToken:        p.GetNextPageToken(),
		Children:             children,
		Attempt:              p.GetAttempt(),
		ResourceTypesWritten: p.GetResourceTypesWritten(),
		ResourcesWritten:     p.GetResourcesWritten(),
		EntitlementsWritten:  p.GetEntitlementsWritten(),
		GrantsWritten:        p.GetGrantsWritten(),
		Replayed:             p.GetReplayed(),
		TypeScopedPlanned:    p.GetTypeScopedPlanned(),
		Scrubbed:             p.GetScrubbed(),
		PageDuration:         msToDuration(p.GetPageMs()),
		ConnectorDuration:    msToDuration(p.GetConnectorMs()),
		WaitDuration:         msToDuration(p.GetWaitMs()),
	}
	if ts := p.GetCommittedAt(); ts != nil {
		row.CommittedAt = ts.AsTime()
	}
	return row
}
