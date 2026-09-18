package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"time"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

func (s *syncer) validateSelectedResourceTypes(ctx context.Context, staged []*v2.ResourceType) error {
	if len(s.cfg.syncResourceTypes) == 0 {
		return nil
	}
	var annos annotations.Annotations
	if syncID := s.getActiveSyncID(); syncID != "" {
		annos.Update(c1zpb.SyncDetails_builder{Id: syncID}.Build())
	}
	present := make(map[string]bool, len(staged))
	for _, rt := range staged {
		present[rt.GetId()] = true
	}
	for _, id := range s.cfg.syncResourceTypes {
		if present[id] {
			continue
		}
		_, err := s.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{ResourceTypeId: id, Annotations: annos}.Build())
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("invalid resource type '%s' in filter", id)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *syncer) syncLedgerResourceTypes(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncResourceTypes")
	uotel.SetSyncIdentityAttrs(ctx, span)
	err := s.collectLedgerResourceTypes(ctx, action)
	uotel.EndSpanWithError(span, err)
	return err
}

func (s *syncer) collectLedgerResourceTypes(ctx context.Context, action *Action) error {
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("resource-type ledger handler requires an open page")
	}
	page := invocation.page
	pageStart := time.Now()
	defer func() { page.row.PageDuration = time.Since(pageStart) }()
	if action.PageToken == "" {
		ctxzap.Extract(ctx).Info("Syncing resource types...")
		s.handleInitialActionForStep(ctx, *action)
	}
	start := time.Now()
	resp, err := s.connector.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
		PageToken: action.PageToken, ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	elapsed := time.Since(start)
	s.recordLedgerConnectorResponse(ctx, invocation, "list-resource-types", elapsed, resp.GetAnnotations())
	if err != nil {
		return err
	}
	selection := make(map[string]bool, len(s.cfg.syncResourceTypes))
	for _, id := range s.cfg.syncResourceTypes {
		selection[id] = true
	}
	var selected []*v2.ResourceType
	var invalid uint64
	for _, rt := range resp.GetList() {
		reason, err := validateConnectorResourceType(rt)
		if err != nil {
			return err
		}
		if reason != "" {
			invalid++
			continue
		}
		if len(selection) == 0 || selection[rt.GetId()] {
			selected = append(selected, rt)
		}
	}
	if err := page.writer.PutResourceTypes(ctx, selected...); err != nil {
		return err
	}
	if resp.GetNextPageToken() == "" {
		if err := s.validateSelectedResourceTypes(ctx, selected); err != nil {
			return err
		}
	}
	page.observations.Counters = map[string]uint64{"ingest.invalid_resource_types_observed": invalid}

	progressAction := *action
	invocation.afterCommit = append(invocation.afterCommit, func() {
		s.ingestFilterStats.invalidResourceTypesObserved.Add(invalid)
		s.counts.AddResourceTypes(len(selected))
		s.handleProgress(ctx, &progressAction, len(selected))
		if resp.GetNextPageToken() == "" {
			s.counts.LogResourceTypesProgress(ctx)
		}
	})
	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken())
}

func (s *syncer) recordLedgerConnectorResponse(ctx context.Context, invocation *ledgerInvocation, method string, elapsed time.Duration, annos []*anypb.Any) {
	s.recordLedgerConnectorResponseForAction(ctx, invocation, invocation.action, method, elapsed, annos)
}

func (s *syncer) recordLedgerConnectorResponseForAction(
	ctx context.Context, invocation *ledgerInvocation, action *Action, method string, elapsed time.Duration, annos []*anypb.Any,
) {
	page := invocation.page
	if s.recordStats {
		page.row.ConnectorDuration += elapsed
		stat := c1zstore.CallStat{Count: 1, TotalMs: elapsed.Milliseconds(), MaxMs: elapsed.Milliseconds()}
		if page.observations.ConnectorCalls == nil {
			page.observations.ConnectorCalls = make(map[string]c1zstore.CallStat)
		}
		previous := page.observations.ConnectorCalls[method]
		previous.Add(stat)
		page.observations.ConnectorCalls[method] = previous
		if action.ResourceTypeID != "" {
			key := method + ":" + action.ResourceTypeID
			previous := page.observations.ConnectorCalls[key]
			previous.Add(stat)
			page.observations.ConnectorCalls[key] = previous
		}
		report := &v2.RateLimitWaitReport{}
		responseAnnotations := annotations.Annotations(annos)
		found, err := responseAnnotations.Pick(report)
		if err == nil && found && report.GetWaitMs() > 0 {
			waitMs := min(report.GetWaitMs(), int64(24*time.Hour/time.Millisecond))
			page.row.WaitDuration += time.Duration(waitMs) * time.Millisecond
			if page.observations.StepDurationsMs == nil {
				page.observations.StepDurationsMs = make(map[string]int64)
			}
			page.observations.StepDurationsMs["rate_limit_wait"] += waitMs
			if action.ResourceTypeID != "" {
				page.observations.StepDurationsMs["rate_limit_wait:"+action.ResourceTypeID] += waitMs
			}
		}
	}
	progressAction := *action
	invocation.afterCommit = append(invocation.afterCommit, func() {
		if s.recordStats {
			s.stats.recordConnectorCall(method, elapsed)
			if progressAction.ResourceTypeID != "" {
				s.stats.recordConnectorCall(method+":"+progressAction.ResourceTypeID, elapsed)
			}
			if elapsed > time.Minute {
				ctxzap.Extract(ctx).Warn("slow connector call", zap.String("method", method),
					zap.String("resource_type_id", progressAction.ResourceTypeID), zap.String("resource_id", progressAction.ResourceID), zap.Duration("elapsed", elapsed))
			}
			s.recordConnectorWaitReport(annos, progressAction.ResourceTypeID)
		}
	})
}

func (s *syncer) recordLedgerSessionUsage(invocation *ledgerInvocation, annos []*anypb.Any) {
	if !s.recordStats {
		return
	}
	usage := &v2.SessionStoreUsage{}
	responseAnnotations := annotations.Annotations(annos)
	found, err := responseAnnotations.Pick(usage)
	if err != nil || !found {
		return
	}
	if invocation.page.observations.SessionCalls == nil {
		invocation.page.observations.SessionCalls = make(map[string]c1zstore.CallStat)
	}
	for _, op := range usage.GetOps() {
		if op.GetOp() == "" {
			continue
		}
		key := "connector." + op.GetOp()
		stat := invocation.page.observations.SessionCalls[key]
		stat.Add(c1zstore.CallStat{Count: op.GetCount(), Errors: op.GetErrors(), Timeouts: op.GetTimeouts(), TotalMs: op.GetTotalMs(), MaxMs: op.GetMaxMs()})
		invocation.page.observations.SessionCalls[key] = stat
	}
	invocation.afterCommit = append(invocation.afterCommit, func() { s.recordSessionUsage(annos) })
}
