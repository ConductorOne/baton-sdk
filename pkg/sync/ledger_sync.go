package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func WithRetainLedgerTokens(retain bool) SyncOpt {
	return func(s *syncer) { s.cfg.retainLedgerTokens = retain }
}

func (r *ledgerRuntime) preparePage(ctx context.Context) error {
	r.prepareMu.Lock()
	defer r.prepareMu.Unlock()
	if r.prepared || r.beforePage == nil {
		return nil
	}
	if err := r.beforePage(ctx); err != nil {
		return err
	}
	r.prepared = true
	return nil
}

func (s *syncer) syncLedger(ctx, runCtx context.Context, span trace.Span, newSync bool, targetedResources []*v2.Resource) error {
	l := ctxzap.Extract(ctx)
	syncID := s.syncID
	s.caps.pageLedger.SetRetainLedgerTokens(s.cfg.retainLedgerTokens)
	if _, err := s.prepareLedgerState(ctx, rand.Text(), newSync); err != nil {
		return s.returnSyncError(l, span, err)
	}
	if s.run.hasFact(c1zstore.LedgerFactRetainTokens) && !s.ledgerDebug {
		s.ledgerDebug = true
		l.Warn("resuming with durably retained ledger history and tokens; tokens may contain credentials")
	}
	if writer := s.caps.ingestVerification; writer != nil {
		s.ledger.beforePage = func(pageCtx context.Context) error {
			pageCtx = c1zstore.WithPageWriteBypass(pageCtx, "invalidate prior verification before changing records; absence cannot attest an uncommitted page")
			if err := writer.ClearIngestInvariantVerification(pageCtx, syncID); err != nil {
				return fmt.Errorf("clear prior ingest invariant verification: %w", err)
			}
			return nil
		}
	}
	warnings, err := s.parallelSync(ctx, runCtx, targetedResources)
	if err != nil {
		return s.returnSyncError(l, span, err)
	}

	s.logIngestFilterSummary(ctx)

	if err := s.runIngestionInvariants(ctx); err != nil {
		return s.returnSyncError(l, span, err)
	}
	if s.testHooks.ingestHaltHook != nil {
		if err := s.testHooks.ingestHaltHook(haltStageInvariantsComplete); err != nil {
			return s.returnSyncError(l, span, err)
		}
	}

	var graphToPersist *expand.EntitlementGraph
	if s.cfg.preserveEntitlementGraph {
		if s.graph.peek() == nil && s.run.getActionCount(SyncGrantExpansionOp).CompletedCount > 0 && s.run.hasFact(factNeedsExpansion) && !s.cfg.dontExpandGrants {
			graph, _, graphErr := s.rebuildLedgerPreservedGraph(ctx)
			if graphErr != nil {
				return s.returnSyncError(l, span, graphErr)
			}
			graph.MarkExpansionComplete()
			s.graph.restore(graph)
		}
		s.graph.clearTransientState()
		if s.caps.entitlementGraph != nil && s.caps.grantDigest != nil {
			graphToPersist = s.graph.peek()
			s.graph.clear()
		}
	} else {
		s.graph.clear()
	}
	err = s.store.Cleanup(runCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrSyncNotComplete
		}
		return s.returnSyncError(l, span, err)
	}

	counters := s.terminalLedgerCounters()
	counters.Flags |= s.ingestFilterStats.reasonFlags.Load()
	var terminalFacts []string
	if !s.ledgerDebug {
		terminalFacts = append(terminalFacts, c1zstore.LedgerFactDiscardOnSeal)
	}
	if s.ingestFilterStats.known.Load() {
		terminalFacts = append(terminalFacts, ledgerFactIngestKnown)
	}
	if s.ingestFilterStats.replayBlocked.Load() {
		terminalFacts = append(terminalFacts, ledgerFactIngestBlocked)
	}
	if err := s.prepareLedgerSeal(ctx, counters, terminalFacts...); err != nil {
		return s.returnSyncError(l, span, err)
	}
	err = s.ledger.seal(ctx)
	if err != nil {
		return s.returnSyncError(l, span, err)
	}
	s.persistEntitlementGraphToStore(ctx, syncID, graphToPersist)

	if err := s.persistIngestInvariantVerification(ctx); err != nil {
		l.Warn("failed to persist ingest invariant verification; the sealed sync remains unverified", zap.Error(err))
	}

	s.finishLedgerReport(ctx)
	if s.recordStats {
		l.Info("Sync complete.", s.syncSummaryFields(span)...)
	} else {
		l.Info("Sync complete.")
	}

	cleanupResp, err := s.connector.Cleanup(ctx, v2.ConnectorServiceCleanupRequest_builder{
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	if err != nil {
		l.Error("error clearing connector caches", zap.Error(err))
	}
	if s.recordStats {
		usage := &v2.SessionStoreUsage{}
		cleanupAnnos := annotations.Annotations(cleanupResp.GetAnnotations())
		if ok, pickErr := cleanupAnnos.Pick(usage); pickErr == nil && ok {
			l.Info("connector session store cleanup stats", zap.Any("session_store_usage", usage))
		}
	}

	if len(warnings) > 0 {
		l.Warn("sync completed with warnings", zap.Int("warning_count", len(warnings)), zap.Any("warnings", warnings))
	}
	return nil
}

func (s *syncer) skipLedgerSync(ctx context.Context) error {
	syncID, err := s.store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		return err
	}
	s.syncID = syncID
	s.caps.pageLedger.SetRetainLedgerTokens(s.cfg.retainLedgerTokens)
	seed := c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
	if err := s.caps.pageLedger.InitializePendingWork(ctx, []c1zstore.LedgerWork{seed}); err != nil {
		return err
	}
	work, _, err := s.caps.pageLedger.PendingWork(ctx, 0, 1)
	if err != nil {
		return err
	}
	if len(work) != 1 {
		return errors.New("skip sync has no initial pending work")
	}
	s.ledger, err = newLedgerRuntime(ctx, s.caps.pageLedger, rand.Text())
	if err != nil {
		return err
	}
	_, err = s.ledger.runPage(ctx, 0, c1zstore.LedgerActionIdentity{Op: InitOp.String()}, func(_ context.Context, page *ledgerPage) error {
		if err := page.writer.SetPendingWork(work[0]); err != nil {
			return err
		}
		if err := s.stageLedgerReportOptions(&ledgerInvocation{page: page}); err != nil {
			return err
		}
		return page.transition("")
	})
	if err != nil {
		return err
	}
	var terminalFacts []string
	if !s.ledgerDebug {
		terminalFacts = append(terminalFacts, c1zstore.LedgerFactDiscardOnSeal)
	}
	if err := s.prepareLedgerSeal(ctx, c1zstore.LedgerCounters{}, terminalFacts...); err != nil {
		return err
	}
	if err := s.ledger.seal(ctx); err != nil {
		return err
	}
	if err := s.store.Cleanup(ctx); err != nil {
		return err
	}
	s.finishLedgerReport(ctx)
	return nil
}
