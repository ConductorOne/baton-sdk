package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"iter"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func (s *syncer) syncLedgerStaticEntitlements(ctx context.Context, action *Action) error {
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("static entitlement ledger handler requires an open page")
	}
	start := time.Now()
	defer func() { invocation.page.row.PageDuration = time.Since(start) }()

	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncStaticEntitlements")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID != "" {
		return s.collectLedgerStaticEntitlements(ctx, action)
	}

	ctxzap.Extract(ctx).Info("Syncing static entitlements...")
	s.handleInitialActionForStep(ctx, *action)

	actions := make([]Action, 0)
	for rts, err := range s.listLedgerStaticResourceTypes(ctx, invocation) {
		if err != nil {
			return err
		}
		for _, rt := range rts {
			actions = append(actions, Action{Op: SyncStaticEntitlementsOp, ResourceTypeID: rt.GetId()})
		}
	}

	return s.nextPageOrFinishAction(ctx, action, "", actions...)
}

func (s *syncer) collectLedgerStaticEntitlements(ctx context.Context, action *Action) error {
	invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	ctx, span := tracer.Start(ctx, "syncer.syncStaticEntitlementsForResource")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := time.Now()
	resp, err := s.connector.ListStaticEntitlements(ctx, v2.EntitlementsServiceListStaticEntitlementsRequest_builder{
		ResourceTypeId: action.ResourceTypeID,
		PageToken:      action.PageToken,
		ActiveSyncId:   s.getActiveSyncID(),
	}.Build())
	s.recordLedgerConnectorResponse(ctx, invocation, "list-static-entitlements", time.Since(start), resp.GetAnnotations())
	recordLedgerConnectorError(invocation, err)
	s.recordLedgerSessionUsage(invocation, resp.GetAnnotations())
	if err != nil {
		if strings.Contains(err.Error(), `unable to resolve \"type.googleapis.com/c1.connector.v2.EntitlementsServiceListStaticEntitlementsRequest\": \"not found\"","errorType":"prefixError"`) {
			l := ctxzap.Extract(ctx)
			l.Info("ignoring prefixError when calling ListStaticEntitlements", zap.Error(err))
			return s.nextPageOrFinishAction(ctx, action, "")
		}

		return err
	}

	collection := ledgerCollection(invocation)
	recordLedgerList(collection, &collection.EntitlementsReceived, resp.GetList(), resp.GetNextPageToken())
	identity, err := json.Marshal(ledgerIdentity(action))
	if err != nil {
		return err
	}
	origin := sha256.Sum256(identity)
	children := make([]Action, 0, len(resp.GetList()))
	for ordinal := len(resp.GetList()) - 1; ordinal >= 0; ordinal-- {
		template, err := proto.MarshalOptions{Deterministic: true}.Marshal(resp.GetList()[ordinal])
		if err != nil {
			return err
		}
		cursor := staticMaterializationCursor{Version: 1, Origin: origin[:], Ordinal: ordinal, Template: template}
		token, err := cursor.encode()
		if err != nil {
			return err
		}
		children = append(children, Action{Op: MaterializeStaticEntitlementsOp, ResourceTypeID: action.ResourceTypeID, PageToken: token})
	}

	progressAction := *action
	invocation.afterCommit = append(invocation.afterCommit, func() { s.handleProgress(ctx, &progressAction, len(resp.GetList())) })

	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), children...)
}

func (s *syncer) listLedgerStaticResourceTypes(ctx context.Context, invocation *ledgerInvocation) iter.Seq2[[]*v2.ResourceType, error] {
	return func(yield func([]*v2.ResourceType, error) bool) {
		token := ""
		for {
			start := time.Now()
			resp, err := s.connector.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageToken: token, ActiveSyncId: s.getActiveSyncID()}.Build())
			s.recordLedgerConnectorResponse(ctx, invocation, "list-resource-types", time.Since(start), resp.GetAnnotations())
			recordLedgerConnectorError(invocation, err)
			if err != nil {
				yield(nil, err)
				return
			}
			collection := ledgerCollection(invocation)
			recordLedgerList(collection, &collection.ResourceTypesReceived, resp.GetList(), resp.GetNextPageToken())
			var types []*v2.ResourceType
			var invalid uint64
			for _, rt := range resp.GetList() {
				reason, err := validateConnectorResourceType(rt)
				if err != nil {
					yield(nil, err)
					return
				}
				if reason != "" {
					invalid++
					continue
				}
				types = append(types, rt)
			}
			if invocation.page.observations.Counters == nil {
				invocation.page.observations.Counters = make(map[string]uint64)
			}
			collection.ResourceTypesExcludedInvalid += invalid
			invocation.page.observations.Counters["ingest.invalid_resource_types_observed"] += invalid
			invocation.afterCommit = append(invocation.afterCommit, func() { s.ingestFilterStats.invalidResourceTypesObserved.Add(invalid) })
			if len(types) > 0 && !yield(types, nil) {
				return
			}
			token = resp.GetNextPageToken()
			if token == "" {
				return
			}
		}
	}
}
