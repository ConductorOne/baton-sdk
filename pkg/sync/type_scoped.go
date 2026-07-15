package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Shared plumbing for TYPE-SCOPED grants and entitlements (see
// proto/c1/connector/v2/annotation_type_scoped_grants.proto and its
// entitlements sibling). Both phases have identical mechanics — a
// resource-type annotation opts a type out of the per-resource fan-out,
// the syncer issues one planner action whose request carries the marker
// annotation over a {type, type} stub resource, and the connector may
// fan out sibling cursors via EnqueuePageTokens — so the mechanics live
// here once, parameterized by marker/cache/op, and the phase functions
// in syncer.go stay thin.

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

// typeScopedRequestStub builds the wire shape of a type-scoped list
// request: wire validation requires a non-empty resource id, so the stub
// resource is self-referential ({type, type}) and the marker annotation
// on the REQUEST is the routing signal the builder dispatches on.
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

// collectEnqueuedPageTokens parses a response's EnqueuePageTokens
// annotation and converts its page tokens into sibling actions — each an
// ORDINARY page that happens to be enqueued eagerly: scheduled by the
// worker pool, rate-limited, and checkpointed like any other pagination.
//
//   - Type-scoped calls: one cursor per connector-defined shard (e.g.
//     50-id delta chunks); actions carry only the resource type.
//   - Per-resource calls: parallel page fan-out for page-numbered APIs;
//     actions carry the resource identity.
//
// Every spawned action is persisted in the checkpointed state token, so
// fan-out is capped per response and oversized/empty tokens fail loudly
// (matching the proto contract) rather than bloating every checkpoint or
// silently dropping a shard.
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
	for _, tok := range spawn.GetPageTokens() {
		if tok == "" {
			// An empty token would spawn a PLANNER call, not a page —
			// a connector bug that would loop the fan-out. Loud, like
			// the caps.
			return nil, fmt.Errorf("%s: EnqueuePageTokens carried an empty page token", phase)
		}
		if len(tok) > maxEnqueuedPageTokenBytes {
			return nil, fmt.Errorf(
				"%s: EnqueuePageTokens page token is %d bytes (max %d)",
				phase, len(tok), maxEnqueuedPageTokenBytes)
		}
		spawned = append(spawned, Action{
			Op:             op,
			ResourceTypeID: action.ResourceTypeID,
			ResourceID:     action.ResourceID, // empty on type-scoped actions
			PageToken:      tok,
			Spawned:        true,
		})
	}
	if len(spawned) > 0 {
		s.state.AddSourceCacheStats(SourceCacheStats{EnqueuedPageTokens: int64(len(spawned))})
	}
	ctxzap.Extract(ctx).Debug(phase+": enqueued sibling page-token cursors",
		zap.String("resource_type_id", action.ResourceTypeID),
		zap.String("resource_id", action.ResourceID),
		zap.Int("cursors", len(spawned)),
		// estimated_total is advisory (logging/observability only);
		// nothing gates on it.
		zap.Int64("estimated_total", spawn.GetEstimatedTotal()))
	return spawned, nil
}
