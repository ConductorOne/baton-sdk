package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

func (s *ledgerGuardedStore) ResumeSync(ctx context.Context, kind connectorstore.SyncType, id string) (string, error) {
	if err := s.audit.record(ctx, "ResumeSync"); err != nil {
		return "", err
	}
	return s.Store.ResumeSync(ctx, kind, id)
}

func (s *ledgerGuardedStore) StartOrResumeSync(ctx context.Context, kind connectorstore.SyncType, id string) (string, bool, error) {
	if err := s.audit.record(ctx, "StartOrResumeSync"); err != nil {
		return "", false, err
	}
	return s.Store.StartOrResumeSync(ctx, kind, id)
}

func (s *ledgerGuardedStore) StartNewSync(ctx context.Context, kind connectorstore.SyncType, parent string) (string, error) {
	if err := s.audit.record(ctx, "StartNewSync"); err != nil {
		return "", err
	}
	return s.Store.StartNewSync(ctx, kind, parent)
}

func (s *ledgerGuardedStore) SetCurrentSync(ctx context.Context, id string) error {
	if err := s.audit.record(ctx, "SetCurrentSync"); err != nil {
		return err
	}
	return s.Store.SetCurrentSync(ctx, id)
}

func (s *ledgerGuardedStore) Cleanup(ctx context.Context) error {
	if err := s.audit.record(ctx, "Cleanup"); err != nil {
		return err
	}
	return s.Store.Cleanup(ctx)
}

func (s *ledgerGuardedStore) Close(ctx context.Context) error {
	if err := s.audit.record(ctx, "Close"); err != nil {
		return err
	}
	return s.Store.Close(ctx)
}

func (s *ledgerGuardedStore) DropLedger(ctx context.Context) error {
	if err := s.audit.record(ctx, "DropLedger"); err != nil {
		return err
	}
	return s.PageLedgerStore.DropLedger(ctx)
}

func (s *ledgerGuardedStore) EndSyncWithStats(ctx context.Context, stats c1zstore.SyncStats) error {
	if err := s.audit.record(ctx, "EndSyncWithStats"); err != nil {
		return err
	}
	return s.PageLedgerStore.EndSyncWithStats(ctx, stats)
}

func (s *ledgerGuardedStore) TakeoverToken(ctx context.Context, run string, facts []string, counters c1zstore.LedgerCounters) (string, error) {
	if err := s.audit.record(ctx, "TakeoverToken"); err != nil {
		return "", err
	}
	return s.PageLedgerStore.TakeoverToken(ctx, run, facts, counters)
}

func (s *ledgerGuardedStore) PutCounterBucket(ctx context.Context, run string, worker uint32, counters c1zstore.LedgerCounters) error {
	if err := s.audit.record(ctx, "PutCounterBucket"); err != nil {
		return err
	}
	return s.PageLedgerStore.PutCounterBucket(ctx, run, worker, counters)
}

type ledgerGuardedGrants struct {
	caps storeCaps
	c1zstore.GrantStore
	audit *ledgerWriteAudit
}

type ledgerGuardedMeta struct {
	caps storeCaps
	c1zstore.SyncMeta
	audit *ledgerWriteAudit
}

type ledgerGuardedSessions struct {
	sessions.SessionStore
	audit *ledgerWriteAudit
}

type ledgerGuardedFiles struct {
	c1zstore.FileOps
	audit *ledgerWriteAudit
}

func (s ledgerGuardedGrants) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	if err := s.audit.record(ctx, "StoreExpandedGrants"); err != nil {
		return err
	}
	return s.GrantStore.StoreExpandedGrants(ctx, grants...)
}

func (s ledgerGuardedMeta) MarkSyncSupportsDiff(ctx context.Context, id string) error {
	if err := s.audit.record(ctx, "MarkSyncSupportsDiff"); err != nil {
		return err
	}
	return s.SyncMeta.MarkSyncSupportsDiff(ctx, id)
}

func (s ledgerGuardedMeta) RecalculateStats(ctx context.Context, id string) error {
	if err := s.audit.record(ctx, "RecalculateStats"); err != nil {
		return err
	}
	return s.SyncMeta.RecalculateStats(ctx, id)
}

func (s ledgerGuardedSessions) Set(ctx context.Context, key string, value []byte, opts ...sessions.SessionStoreOption) error {
	if err := s.audit.record(ctx, "Set"); err != nil {
		return err
	}
	return s.SessionStore.Set(ctx, key, value, opts...)
}

func (s ledgerGuardedSessions) SetMany(ctx context.Context, values map[string][]byte, opts ...sessions.SessionStoreOption) error {
	if err := s.audit.record(ctx, "SetMany"); err != nil {
		return err
	}
	return s.SessionStore.SetMany(ctx, values, opts...)
}

func (s ledgerGuardedSessions) Delete(ctx context.Context, key string, opts ...sessions.SessionStoreOption) error {
	if err := s.audit.record(ctx, "Delete"); err != nil {
		return err
	}
	return s.SessionStore.Delete(ctx, key, opts...)
}

func (s ledgerGuardedSessions) Clear(ctx context.Context, opts ...sessions.SessionStoreOption) error {
	if err := s.audit.record(ctx, "Clear"); err != nil {
		return err
	}
	return s.SessionStore.Clear(ctx, opts...)
}

func (s ledgerGuardedFiles) CloneSync(ctx context.Context, path, id string, opts ...c1zstore.CloneSyncOption) error {
	if err := s.audit.record(ctx, "CloneSync"); err != nil {
		return err
	}
	return s.FileOps.CloneSync(ctx, path, id, opts...)
}

func (s ledgerGuardedFiles) CopyIsolateSync(ctx context.Context, path, id string, opts ...c1zstore.CloneSyncOption) error {
	if err := s.audit.record(ctx, "CopyIsolateSync"); err != nil {
		return err
	}
	return s.FileOps.CopyIsolateSync(ctx, path, id, opts...)
}

func (s *ledgerGuardedStore) Grants() c1zstore.GrantStore {
	return ledgerGuardedGrants{caps: s.caps, GrantStore: s.Store.Grants(), audit: s.audit}
}
func (s *ledgerGuardedStore) SyncMeta() c1zstore.SyncMeta {
	return ledgerGuardedMeta{caps: s.caps, SyncMeta: s.Store.SyncMeta(), audit: s.audit}
}
func (s *ledgerGuardedStore) SessionStore() sessions.SessionStore {
	return ledgerGuardedSessions{SessionStore: s.Store.SessionStore(), audit: s.audit}
}
func (s *ledgerGuardedStore) FileOps() c1zstore.FileOps {
	return ledgerGuardedFiles{FileOps: s.Store.FileOps(), audit: s.audit}
}

func (s *ledgerGuardedStore) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	if err := s.audit.record(ctx, "DeleteResourceRecord"); err != nil {
		return err
	}
	return s.caps.resourceDeleter.DeleteResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *ledgerGuardedStore) DeleteEntitlementByRefs(ctx context.Context, entitlement *v2.Entitlement) error {
	if err := s.audit.record(ctx, "DeleteEntitlementByRefs"); err != nil {
		return err
	}
	return s.caps.entitlementDeleter.DeleteEntitlementByRefs(ctx, entitlement)
}

func (s *ledgerGuardedStore) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	if err := s.audit.record(ctx, "DeleteGrantByRefs"); err != nil {
		return err
	}
	return s.caps.grantRefsDeleter.DeleteGrantByRefs(ctx, grant)
}

func (s *ledgerGuardedStore) DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error {
	if err := s.audit.record(ctx, "DeleteGrantsByRefs"); err != nil {
		return err
	}
	return s.caps.grantBatchDeleter.DeleteGrantsByRefs(ctx, grants...)
}

func (s *ledgerGuardedStore) PutEntitlementGraphBlob(ctx context.Context, data []byte) error {
	if err := s.audit.record(ctx, "PutEntitlementGraphBlob"); err != nil {
		return err
	}
	return s.caps.entitlementGraph.PutEntitlementGraphBlob(ctx, data)
}

func (s *ledgerGuardedStore) DeleteEntitlementGraphBlob(ctx context.Context) error {
	if err := s.audit.record(ctx, "DeleteEntitlementGraphBlob"); err != nil {
		return err
	}
	return s.caps.entitlementGraph.DeleteEntitlementGraphBlob(ctx)
}

func (s ledgerGuardedMeta) MarkIngestInvariantsVerified(ctx context.Context, id string, verification c1zstore.IngestInvariantVerification) error {
	if err := s.audit.record(ctx, "MarkIngestInvariantsVerified"); err != nil {
		return err
	}
	return s.caps.ingestVerification.MarkIngestInvariantsVerified(ctx, id, verification)
}

func (s ledgerGuardedMeta) ClearIngestInvariantVerification(ctx context.Context, id string) error {
	if err := s.audit.record(ctx, "ClearIngestInvariantVerification"); err != nil {
		return err
	}
	return s.caps.ingestVerification.ClearIngestInvariantVerification(ctx, id)
}

func (s ledgerGuardedGrants) StoreNewExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	if err := s.audit.record(ctx, "StoreNewExpandedGrants"); err != nil {
		return err
	}
	return s.caps.newExpandedGrants.StoreNewExpandedGrants(ctx, grants...)
}

func (s ledgerGuardedGrants) StoreNewExpandedGrantContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	if err := s.audit.record(ctx, "StoreNewExpandedGrantContributions"); err != nil {
		return err
	}
	return s.caps.newExpandedContributions.StoreNewExpandedGrantContributions(ctx, dest, principals, sources)
}

func (s ledgerGuardedGrants) BeginExpandedGrantLayer(ctx context.Context) (bool, error) {
	if err := s.audit.record(ctx, "BeginExpandedGrantLayer"); err != nil {
		return false, err
	}
	return s.caps.expandedGrantLayer.BeginExpandedGrantLayer(ctx)
}

func (s ledgerGuardedGrants) AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	if err := s.audit.record(ctx, "AddExpandedGrantLayerContributions"); err != nil {
		return err
	}
	return s.caps.expandedGrantLayer.AddExpandedGrantLayerContributions(ctx, dest, principals, sources)
}

func (s ledgerGuardedGrants) FinishExpandedGrantLayer(ctx context.Context) error {
	if err := s.audit.record(ctx, "FinishExpandedGrantLayer"); err != nil {
		return err
	}
	return s.caps.expandedGrantLayer.FinishExpandedGrantLayer(ctx)
}

func (s ledgerGuardedGrants) AbortExpandedGrantLayer(ctx context.Context) error {
	if err := s.audit.record(ctx, "AbortExpandedGrantLayer"); err != nil {
		return err
	}
	return s.caps.expandedGrantLayer.AbortExpandedGrantLayer(ctx)
}

type ledgerTrackedWriter struct {
	c1zstore.PageWriter
	audit    *ledgerWriteAudit
	released bool
}

func (s *ledgerGuardedStore) BeginPage() c1zstore.PageWriter {
	s.audit.mu.Lock()
	s.audit.writers++
	s.audit.mu.Unlock()
	return &ledgerTrackedWriter{PageWriter: s.PageLedgerStore.BeginPage(), audit: s.audit}
}

func (w *ledgerTrackedWriter) release() {
	if !w.released {
		w.audit.mu.Lock()
		w.audit.writers--
		w.audit.mu.Unlock()
		w.released = true
	}
}

func (w *ledgerTrackedWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	w.audit.mu.Lock()
	phase := w.audit.phase
	w.audit.events = append(w.audit.events, ledgerWriteEvent{method: "Page.Commit", phase: phase, open: c1zstore.PageOpen(ctx)})
	w.audit.mu.Unlock()
	if phase == ledgerWalk {
		return errLedgerFixtureWrite
	}
	if err := w.PageWriter.Commit(ctx, id, row); err != nil {
		return err
	}
	w.release()
	return nil
}

func (w *ledgerTrackedWriter) Discard() {
	w.PageWriter.Discard()
	w.release()
}

func (s *ledgerGuardedStore) ClearLedgerRows(ctx context.Context, facts []string) error {
	if err := s.audit.record(ctx, "ClearLedgerRows"); err != nil {
		return err
	}
	return s.PageLedgerStore.ClearLedgerRows(ctx, facts)
}

func (s *ledgerGuardedStore) ArchiveLedgerReport(ctx context.Context) ([]byte, error) {
	if err := s.audit.record(ctx, "ArchiveLedgerReport"); err != nil {
		return nil, err
	}
	return s.PageLedgerStore.ArchiveLedgerReport(ctx)
}
func (s *ledgerGuardedStore) RestoreLedgerArchive(ctx context.Context) error {
	if err := s.audit.record(ctx, "RestoreLedgerArchive"); err != nil {
		return err
	}
	return s.PageLedgerStore.RestoreLedgerArchive(ctx)
}

func (s *ledgerGuardedStore) InitializePendingWork(ctx context.Context, work []c1zstore.LedgerWork, facts ...string) error {
	if err := s.audit.record(ctx, "InitializePendingWork"); err != nil {
		return err
	}
	return s.PageLedgerStore.InitializePendingWork(ctx, work, facts...)
}

func (s *ledgerGuardedStore) TakeoverPendingWork(
	ctx context.Context, runID, token string, facts []string, counters c1zstore.LedgerCounters, work []c1zstore.LedgerWork,
) (string, error) {
	if err := s.audit.record(ctx, "TakeoverPendingWork"); err != nil {
		return "", err
	}
	return s.PageLedgerStore.TakeoverPendingWork(ctx, runID, token, facts, counters, work)
}

func (s *ledgerGuardedStore) CompletePendingWork(ctx context.Context, work c1zstore.LedgerWork, runID string, counters c1zstore.LedgerCounters) error {
	if err := s.audit.record(ctx, "CompletePendingWork"); err != nil {
		return err
	}
	return s.PageLedgerStore.CompletePendingWork(ctx, work, runID, counters)
}
