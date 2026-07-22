package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

const (
	maxEnqueuePageTokensPerResponse = 1024
	maxEnqueuedPageTokenBytes       = 1 << 20
	maxEnqueuedPageTokenTotalBytes  = 16 << 20
)

// resourceTypeCarries reports (cached per sync in cache) whether the
// resource type's stored annotations contain marker.
func (s *syncer) resourceTypeCarries(ctx context.Context, resourceTypeID string, marker proto.Message, cache *syncMap[string, bool]) (bool, error) {
	if v, ok := cache.Load(resourceTypeID); ok {
		return v, nil
	}
	rt, err := s.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	if err != nil {
		return false, err
	}
	rtAnnos := annotations.Annotations(rt.GetResourceType().GetAnnotations())
	carries := rtAnnos.Contains(marker)
	cache.Store(resourceTypeID, carries)
	return carries, nil
}

// resourceTypesCarrying lists every synced resource type whose stored
// annotations contain marker, warming cache for every type seen.
func (s *syncer) resourceTypesCarrying(ctx context.Context, marker proto.Message, cache *syncMap[string, bool]) ([]string, error) {
	var out []string
	pageToken := ""
	for {
		resp, err := s.store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return nil, err
		}
		for _, rt := range resp.GetList() {
			rtAnnos := annotations.Annotations(rt.GetAnnotations())
			carries := rtAnnos.Contains(marker)
			cache.Store(rt.GetId(), carries)
			if carries {
				out = append(out, rt.GetId())
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return out, nil
		}
	}
}

// typeScopedRequestStub builds the wire shape of a type-scoped list request.
func typeScopedRequestStub(resourceTypeID string, marker proto.Message) (*v2.Resource, annotations.Annotations) {
	stub := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     resourceTypeID,
		}.Build(),
	}.Build()
	var reqAnnos annotations.Annotations
	reqAnnos.Update(marker)
	return stub, reqAnnos
}

// collectEnqueuedPageTokens turns connector-supplied sibling cursors into
// ordinary checkpointed actions. Empty and oversized tokens fail loudly.
func (s *syncer) collectEnqueuedPageTokens(ctx context.Context, phase string, op ActionOp, action *Action, respAnnos annotations.Annotations) ([]Action, error) {
	spawn := &v2.EnqueuePageTokens{}
	hasSpawn, err := respAnnos.Pick(spawn)
	if err != nil {
		return nil, fmt.Errorf("%s: error parsing enqueue-page-tokens annotation: %w", phase, err)
	}
	if !hasSpawn {
		return nil, nil
	}
	if len(spawn.GetPageTokens()) > maxEnqueuePageTokensPerResponse {
		return nil, fmt.Errorf(
			"%s: EnqueuePageTokens carried %d page tokens (max %d per response); chain additional spawns across pages instead",
			phase, len(spawn.GetPageTokens()), maxEnqueuePageTokensPerResponse)
	}

	spawned := make([]Action, 0, len(spawn.GetPageTokens()))
	totalTokenBytes := 0
	for _, tok := range spawn.GetPageTokens() {
		if tok == "" {
			return nil, fmt.Errorf("%s: EnqueuePageTokens carried an empty page token", phase)
		}
		if len(tok) > maxEnqueuedPageTokenBytes {
			return nil, fmt.Errorf(
				"%s: EnqueuePageTokens page token is %d bytes (max %d)",
				phase, len(tok), maxEnqueuedPageTokenBytes)
		}
		totalTokenBytes += len(tok)
		if totalTokenBytes > maxEnqueuedPageTokenTotalBytes {
			return nil, fmt.Errorf(
				"%s: EnqueuePageTokens carries more than %d total page-token bytes; shrink the tokens or chain additional spawns across pages",
				phase, maxEnqueuedPageTokenTotalBytes)
		}
		spawned = append(spawned, Action{
			Op:             op,
			ResourceTypeID: action.ResourceTypeID,
			ResourceID:     action.ResourceID,
			PageToken:      tok,
			Spawned:        true,
		})
	}

	ctxzap.Extract(ctx).Debug(phase+": enqueued sibling page-token cursors",
		zap.String("resource_type_id", action.ResourceTypeID),
		zap.String("resource_id", action.ResourceID),
		zap.Int("cursors", len(spawned)),
		zap.Int64("estimated_total", spawn.GetEstimatedTotal()))
	return spawned, nil
}
