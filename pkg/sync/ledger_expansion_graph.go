package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *syncer) rebuildLedgerPreservedGraph(ctx context.Context) (*expand.EntitlementGraph, *expand.DroppedEdgeStats, error) {
	drops := &expand.DroppedEdgeStats{}
	graph := expand.NewEntitlementGraph(ctx)
	l := ctxzap.Extract(ctx)
	cursor := ""
	for {
		page, next, err := s.store.Grants().PendingExpansionPage(ctx, cursor)
		if err != nil {
			return nil, nil, err
		}
		for _, def := range page {
			dstEntitlementID := def.TargetEntitlementID

			for _, srcEntitlementID := range def.Annotation.GetEntitlementIds() {
				srcEntitlement, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
					EntitlementId: srcEntitlementID,
				}.Build())
				if err != nil {
					if status.Code(err) == codes.NotFound {
						drops.RecordSourceMissing(srcEntitlementID)
						l.Debug("source entitlement not found, skipping edge",
							zap.String("src_entitlement_id", srcEntitlementID),
							zap.String("dst_entitlement_id", dstEntitlementID),
						)
						continue
					}
					l.Error("error fetching source entitlement",
						zap.String("src_entitlement_id", srcEntitlementID),
						zap.String("dst_entitlement_id", dstEntitlementID),
						zap.Error(err),
					)
					return nil, nil, err
				}

				sourceEntitlementResourceID := srcEntitlement.GetEntitlement().GetResource().GetId()
				if sourceEntitlementResourceID == nil {
					return nil, nil, fmt.Errorf("source entitlement resource id was nil")
				}
				if def.PrincipalResourceTypeID != sourceEntitlementResourceID.GetResourceType() ||
					def.PrincipalResourceID != sourceEntitlementResourceID.GetResource() {
					l.Error(
						"source entitlement resource id did not match grant principal id",
						zap.String("grant_principal_resource_type_id", def.PrincipalResourceTypeID),
						zap.String("grant_principal_resource_id", def.PrincipalResourceID),
						zap.String("source_entitlement_resource_id", sourceEntitlementResourceID.String()))

					return nil, nil, fmt.Errorf("source entitlement resource id did not match grant principal id")
				}

				graph.AddEntitlementID(dstEntitlementID)
				graph.AddEntitlementID(srcEntitlementID)
				err = graph.AddEdge(ctx, srcEntitlementID, dstEntitlementID, def.Annotation.GetShallow(), def.Annotation.GetResourceTypeIds())
				if err != nil {
					return nil, nil, fmt.Errorf("error adding edge to graph: %w", err)
				}
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	graph.Loaded = true
	if err := s.fixEntitlementGraphCycles(ctx, graph); err != nil {
		return nil, nil, err
	}
	return graph, drops, nil
}
