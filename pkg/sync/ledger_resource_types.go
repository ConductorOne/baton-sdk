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

func (s *syncer) syncLedgerResourceTypes(ctx context.Context, page *ledgerPage, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncResourceTypes")
	uotel.SetSyncIdentityAttrs(ctx, span)
	err := s.collectLedgerResourceTypes(ctx, page, action)
	uotel.EndSpanWithError(span, err)
	return err
}

func (s *syncer) collectLedgerResourceTypes(ctx context.Context, page *ledgerPage, action *Action) error {
	if page == nil {
		return errors.New("resource-type ledger handler requires an open page")
	}
	pageStart := time.Now()
	defer page.recordDuration(pageStart)
	if action.PageToken == "" {
		ctxzap.Extract(ctx).Info("Syncing resource types...")
		s.handleInitialActionForStep(ctx, *action)
	}
	start := time.Now()
	resp, err := s.connector.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
		PageToken: action.PageToken, ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	elapsed := time.Since(start)
	s.recordLedgerConnectorResponse(ctx, page, "list-resource-types", elapsed, resp.GetAnnotations())
	recordLedgerConnectorError(page, err)
	if err != nil {
		return markActionListCall(err)
	}
	collection := ledgerCollection(page)
	recordLedgerList(collection, &collection.ResourceTypesReceived, resp.GetList(), resp.GetNextPageToken())
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
		} else {
			collection.ResourceTypesExcludedBySelection++
		}
	}
	if err := page.PutResourceTypes(ctx, selected...); err != nil {
		return err
	}
	if resp.GetNextPageToken() == "" {
		if err := s.validateSelectedResourceTypes(ctx, selected); err != nil {
			return err
		}
	}
	collection.ResourceTypesExcludedInvalid += invalid
	page.observations.Counters = map[string]uint64{"ingest.invalid_resource_types_observed": invalid}

	progressAction := *action
	page.afterCommit = append(page.afterCommit, func() {
		s.ingestFilterStats.invalidResourceTypesObserved.Add(invalid)
		s.counts.AddResourceTypes(len(selected))
		s.handleProgress(ctx, &progressAction, len(selected))
		if resp.GetNextPageToken() == "" {
			s.counts.LogResourceTypesProgress(ctx)
		}
	})
	return page.stage(action, resp.GetNextPageToken(), nil)
}

func (s *syncer) recordLedgerConnectorResponse(ctx context.Context, page *ledgerPage, method string, elapsed time.Duration, annos []*anypb.Any) {
	s.recordLedgerConnectorResponseForAction(ctx, page, page.action, method, elapsed, annos)
}

func (s *syncer) recordLedgerConnectorResponseForAction(
	ctx context.Context, page *ledgerPage, action *Action, method string, elapsed time.Duration, annos []*anypb.Any,
) {
	var reportedWait time.Duration
	if page.attempts != nil {
		measured := elapsed
		if !s.recordStats {
			measured = 0
		}
		page.attempts.recordCall(measured)
	}
	if s.recordStats {
		page.row.ConnectorDuration += elapsed
		stat := c1zstore.CallStat{Count: 1, TotalMs: elapsed.Milliseconds(), MaxMs: elapsed.Milliseconds()}
		if page.connectorObservations.ConnectorCalls == nil {
			page.connectorObservations.ConnectorCalls = make(map[string]c1zstore.CallStat)
		}
		previous := page.connectorObservations.ConnectorCalls[method]
		previous.Add(stat)
		page.connectorObservations.ConnectorCalls[method] = previous
		if action.ResourceTypeID != "" {
			key := method + ":" + action.ResourceTypeID
			previous := page.connectorObservations.ConnectorCalls[key]
			previous.Add(stat)
			page.connectorObservations.ConnectorCalls[key] = previous
		}
		reportedWait = connectorReportedWait(annos)
		if reportedWait > 0 {
			waitMs := reportedWait.Milliseconds()
			page.row.WaitDuration += reportedWait
			if page.attempts != nil {
				page.attempts.recordReportedWait(reportedWait)
			}
			if page.connectorObservations.StepDurationsMs == nil {
				page.connectorObservations.StepDurationsMs = make(map[string]int64)
			}
			page.connectorObservations.StepDurationsMs["rate_limit_wait"] += waitMs
			if action.ResourceTypeID != "" {
				page.connectorObservations.StepDurationsMs["rate_limit_wait:"+action.ResourceTypeID] += waitMs
			}
		}
	}
	if s.recordStats {
		if elapsed > time.Minute {
			ctxzap.Extract(ctx).Warn("slow connector call", zap.String("method", method),
				zap.String("resource_type_id", action.ResourceTypeID), zap.String("resource_id", action.ResourceID), zap.Duration("elapsed", elapsed))
		}
		if reportedWait > 0 {
			s.recordRateLimitWallInterval(reportedWait)
		}
	}
}

func (s *syncer) recordLedgerSessionUsage(page *ledgerPage, annos []*anypb.Any) {
	if !s.recordStats {
		return
	}
	usage := &v2.SessionStoreUsage{}
	responseAnnotations := annotations.Annotations(annos)
	found, err := responseAnnotations.Pick(usage)
	if err != nil || !found {
		return
	}
	if page.connectorObservations.SessionCalls == nil {
		page.connectorObservations.SessionCalls = make(map[string]c1zstore.CallStat)
	}
	for _, op := range usage.GetOps() {
		if op.GetOp() == "" {
			continue
		}
		key := "connector." + op.GetOp()
		stat := page.connectorObservations.SessionCalls[key]
		stat.Add(c1zstore.CallStat{Count: op.GetCount(), Errors: op.GetErrors(), Timeouts: op.GetTimeouts(), TotalMs: op.GetTotalMs(), MaxMs: op.GetMaxMs()})
		page.connectorObservations.SessionCalls[key] = stat
	}
}
