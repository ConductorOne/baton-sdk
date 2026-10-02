package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

func (s *syncer) collectLedgerAssets(ctx context.Context, action *Action) error {
	invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	ctx, span := tracer.Start(ctx, "syncer.syncAssetsForResource")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: action.ResourceTypeID,
			Resource:     action.ResourceID,
		}.Build(),
	}.Build())
	if err != nil {
		return err
	}

	var assetRefs []*v2.AssetRef

	rAnnos := annotations.Annotations(resourceResponse.GetResource().GetAnnotations())

	assetRefs = append(assetRefs, resource.GetIcon(resourceResponse.GetResource()))

	appTrait := &v2.AppTrait{}
	ok, err := rAnnos.Pick(appTrait)
	if err != nil {
		return err
	}
	if ok {
		assetRefs = append(assetRefs, appTrait.GetLogo())
	}

	for _, assetRef := range assetRefs {
		if assetRef == nil {
			continue
		}

		err := func() error {
			l.Debug("fetching asset", zap.String("asset_ref_id", assetRef.GetId()))
			start := time.Now()
			defer func() { s.recordLedgerConnectorResponse(ctx, invocation, "get-asset", time.Since(start), nil) }()

			resp, err := s.connector.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{Asset: assetRef}.Build())
			recordLedgerConnectorError(invocation, err)
			if err != nil {
				return err
			}

			if resp == nil {
				return nil
			}

			var metadata *v2.AssetServiceGetAssetResponse_Metadata
			assetBytes := &bytes.Buffer{}

			var recvErr error
			var msg *v2.AssetServiceGetAssetResponse
			for !errors.Is(recvErr, io.EOF) {
				msg, recvErr = resp.Recv()
				if recvErr != nil {
					if errors.Is(recvErr, io.EOF) {
						continue
					}
					recordLedgerConnectorError(invocation, recvErr)
					l.Error("error fetching asset", zap.Error(recvErr))
					return recvErr
				}

				l.Debug("received asset message")

				switch msg.WhichMsg() {
				case v2.AssetServiceGetAssetResponse_Metadata_case:
					metadata = msg.GetMetadata()
				case v2.AssetServiceGetAssetResponse_Data_case:
					l.Debug("Received data for asset")
					_, err := io.Copy(assetBytes, bytes.NewReader(msg.GetData().GetData()))
					if err != nil {
						_ = resp.CloseSend()
						return err
					}
				case v2.AssetServiceGetAssetResponse_Msg_not_set_case:
					l.Debug("Received unset asset message")
					continue
				}
			}

			if metadata == nil {
				return fmt.Errorf("no metadata received, unable to store asset")
			}

			return invocation.page.writer.PutAsset(ctx, assetRef, metadata.GetContentType(), assetBytes.Bytes())
		}()
		if err != nil {
			return err
		}
	}

	return s.nextPageOrFinishAction(ctx, action, "")
}

func (s *syncer) syncLedgerAssets(ctx context.Context, action *Action) error {
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("asset ledger handler requires an open page")
	}
	start := time.Now()
	defer func() { invocation.page.row.PageDuration = time.Since(start) }()

	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncAssets")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" && action.ResourceID == "" {
		if action.PageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing assets...")
			s.handleInitialActionForStep(ctx, *action)
		}

		resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken:    action.PageToken,
			ActiveSyncId: s.getActiveSyncID(),
		}.Build())
		if err != nil {
			return err
		}

		actions := make([]Action, 0)
		for _, r := range resp.GetList() {
			actions = append(actions, Action{Op: SyncAssetsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
		}

		return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), actions...)
	}

	err = s.collectLedgerAssets(ctx, action)
	if err != nil {
		ctxzap.Extract(ctx).Error("error syncing assets", zap.Error(err))
		return err
	}

	return nil
}
