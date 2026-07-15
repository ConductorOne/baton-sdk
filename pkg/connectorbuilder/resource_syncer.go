package connectorbuilder

import (
	"context"
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResourceSyncer is the primary interface for connector developers to implement.
//
// It defines the core functionality for synchronizing resources, entitlements, and grants
// from external systems into Baton. Every connector must implement at least this interface
// for each resource type it supports.
//
// Extensions to this interface include:
// - ResourceProvisioner/ResourceProvisionerV2: For adding/removing access
// - ResourceManager: For creating and managing resources
// - ResourceDeleter: For deleting resources
// - AccountManager: For account provisioning operations
// - CredentialManager: For credential rotation operations.
// - ResourceTargetedSyncer: For directly getting a resource supporting targeted sync.

type ResourceType interface {
	ResourceType(ctx context.Context) *v2.ResourceType
}

type ResourceSyncer interface {
	ResourceType
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)
}

type ResourceSyncerLimited interface {
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)
}

type StaticEntitlementSyncer interface {
	StaticEntitlements(ctx context.Context, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
}

type ResourceSyncerV2 interface {
	ResourceType
	ResourceSyncerV2Limited
}

type ResourceSyncerV2Limited interface {
	List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error)
	Entitlements(ctx context.Context, resource *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error)
	Grants(ctx context.Context, resource *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error)
}

type StaticEntitlementSyncerV2 interface {
	StaticEntitlements(ctx context.Context, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error)
}

// TypeScopedGrantsSyncer lets a connector enumerate grants for a WHOLE
// resource type instead of being called once per resource. Implement it on
// a resource syncer whose ResourceType carries the v2.TypeScopedGrants
// annotation; the syncer then issues ListGrants requests carrying the
// v2.TypeScopedGrants REQUEST annotation as the routing marker (the
// request's resource is a self-referential {type, type} stub — wire
// validation requires a non-empty resource id), which the builder routes
// here.
//
// The first call of a sync arrives with an empty page token (the planning
// call); the connector may answer with rows directly and/or spawn
// additional independent cursors by attaching a v2.EnqueuePageTokens annotation
// whose page tokens are delivered back through opts.PageToken, one action
// each. Source-cache scope/replay/tombstone annotations work exactly as on
// per-resource grants pages.
//
// The per-resource Grants method is still required by ResourceSyncerV2
// but is NEVER called for an annotated type during a full sync; the
// convention is to return empty results (registration fails loudly if
// the annotation is present without this interface — see
// validateTypeScopedRegistration).
type TypeScopedGrantsSyncer interface {
	GrantsForResourceType(ctx context.Context, resourceTypeID string, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error)
}

// TypeScopedEntitlementsSyncer is the entitlements-phase analogue of
// TypeScopedGrantsSyncer: the connector enumerates entitlements for a WHOLE
// resource type instead of being called once per resource. Implement it on
// a resource syncer whose ResourceType carries the v2.TypeScopedEntitlements
// annotation; the syncer then issues ListEntitlements requests carrying
// that annotation as the routing marker (same {type, type} stub resource
// as the grants variant), which the builder routes here.
//
// Planning / EnqueuePageTokens / source-cache behavior matches TypeScopedGrantsSyncer.
type TypeScopedEntitlementsSyncer interface {
	EntitlementsForResourceType(ctx context.Context, resourceTypeID string, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error)
}

// syncOpAttrs assembles the per-call SyncOpAttrs handed to V2 resource
// syncers. SourceCache is never nil: when the runner supplied no lookup
// (source cache disabled/degraded) connectors see NoopLookup and every
// lookup misses. Session is likewise never a nil-backed wrapper: without a
// configured store, connectors see the erroring no-op store instead of a
// panic on first use.
func (b *builder) syncOpAttrs(activeSyncID string, token pagination.Token) resource.SyncOpAttrs {
	return b.syncOpAttrsWithLookup(activeSyncID, token, b.sourceCacheLookup())
}

// syncOpAttrsWithLookup is syncOpAttrs over a caller-held slot snapshot:
// SetSourceCache can install/clear the slot concurrently with an
// in-flight RPC, so a caller that also BRANCHES on the slot value
// (continuationOpAttrs) must read it exactly once and thread the same
// snapshot into both decisions — two reads could hand a connector
// NoopLookup while treating the topology as direct.
func (b *builder) syncOpAttrsWithLookup(activeSyncID string, token pagination.Token, sc sourcecache.Lookup) resource.SyncOpAttrs {
	if sc == nil {
		sc = sourcecache.NoopLookup{}
	}
	ss := b.sessionStore
	if ss == nil {
		ss = &session.NoOpSessionStore{}
	}
	return resource.SyncOpAttrs{
		SyncID:      activeSyncID,
		PageToken:   token,
		Session:     WithSyncId(ss, activeSyncID),
		SourceCache: sc,
	}
}

// continuationOpAttrs builds SyncOpAttrs for a list RPC, selecting the
// source-cache lookup by topology:
//
//   - a runner-supplied lookup (in-process interface, subprocess loopback
//     gRPC) always wins: those topologies answer lookups directly and
//     never bounce;
//   - otherwise, when the request carries SourceCacheLookupOffer (and/or
//     SourceCacheLookupAnswers from a previous bounce), a per-request
//     ContinuationLookup is installed — it serves answered scopes and
//     defers the rest via ErrLookupDeferred;
//   - otherwise NoopLookup (today's behavior).
//
// The returned ContinuationLookup is nil when the continuation is not in
// play for this request.
func (b *builder) continuationOpAttrs(activeSyncID string, token pagination.Token, reqAnnos annotations.Annotations) (resource.SyncOpAttrs, *sourcecache.ContinuationLookup, error) {
	// One slot read for both the attrs and the topology branch below —
	// see syncOpAttrsWithLookup for why two reads would be a race.
	slot := b.sourceCacheLookup()
	attrs := b.syncOpAttrsWithLookup(activeSyncID, token, slot)
	if l := slot; l != nil {
		// A NoopLookup slot is a CLEARED slot, not a direct lookup:
		// SetSourceCache(nil) stores NoopLookup so late in-flight RPCs
		// read a deterministic miss (see SetSourceCache), and treating
		// it as "direct lookup wins" here would silently disable the
		// ask/answer continuation for every later request — permanent
		// cold syncs with no error. A degraded sync can't sneak into
		// continuation through this branch either: without a warm
		// lookup the syncer never attaches SourceCacheLookupOffer, so
		// the offer check below already returns the noop path.
		if _, isNoop := l.(sourcecache.NoopLookup); !isNoop {
			return attrs, nil, nil
		}
	}
	answersMsg := &v2.SourceCacheLookupAnswers{}
	hasAnswers, err := reqAnnos.Pick(answersMsg)
	if err != nil {
		return attrs, nil, fmt.Errorf("error parsing source-cache lookup answers annotation: %w", err)
	}
	if !hasAnswers && !reqAnnos.Contains(&v2.SourceCacheLookupOffer{}) {
		return attrs, nil, nil
	}
	var answers []sourcecache.Answer
	if hasAnswers {
		answers, err = sourcecache.AnswersFromProto(answersMsg)
		if err != nil {
			return attrs, nil, err
		}
	}
	cl := sourcecache.NewContinuationLookup(answers)
	attrs.SourceCache = cl
	return attrs, cl, nil
}

// continuationOutcome inspects a list handler's result when a
// ContinuationLookup was installed.
//
// Returns (askAnnos, deferred, misuseErr):
//   - deferred=true: the handler deferred on recorded scopes. Respond with
//     askAnnos (the SourceCacheLookupAsk), NO rows, NO next page token,
//     and no failure metrics — this is a protocol turn, not a failure.
//   - misuseErr != nil: the handler swallowed a deferred lookup (scopes
//     were recorded but no ErrLookupDeferred propagated). Loud failure:
//     silently continuing past a deferral re-asks forever and dies at the
//     bounce cap in production, so it must fail here, visibly.
//   - all-zero: the handler resolved normally; use its result as-is.
func continuationOutcome(cl *sourcecache.ContinuationLookup, handlerErr error) (annotations.Annotations, bool, error) {
	if cl == nil {
		return nil, false, nil
	}
	asked := cl.Asked()
	if errors.Is(handlerErr, sourcecache.ErrLookupDeferred) && len(asked) > 0 {
		annos := annotations.Annotations{}
		annos.Update(sourcecache.AskProto(asked))
		return annos, true, nil
	}
	if handlerErr == nil && len(asked) > 0 {
		return nil, false, status.Errorf(codes.Internal,
			"connector swallowed a deferred source-cache lookup (%d scope(s) asked, no error propagated): ErrLookupDeferred must be returned — wrap with %%w, never swallow", len(asked))
	}
	return nil, false, nil
}

// ResourceTargetedSyncer extends ResourceSyncer to add capabilities for directly syncing an individual resource
//
// Implementing this interface indicates the connector supports calling "get" on a resource
// of the associated resource type.
type ResourceTargetedSyncer interface {
	ResourceSyncer
	ResourceTargetedSyncerLimited
}

type ResourceTargetedSyncerLimited interface {
	Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error)
}

// ListResourceTypes lists all available resource types.
func (b *builder) ListResourceTypes(
	ctx context.Context,
	request *v2.ResourceTypesServiceListResourceTypesRequest,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListResourceTypes")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.ListResourceTypesType
	var out []*v2.ResourceType

	if len(b.resourceSyncers) == 0 {
		err = status.Error(codes.FailedPrecondition, "no resource builders found")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	for _, rb := range b.resourceSyncers {
		out = append(out, rb.ResourceType(ctx))
	}

	if len(out) == 0 {
		err = status.Error(codes.FailedPrecondition, "no resource types found")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: out}.Build(), nil
}

// ListResources returns all available resources for a given resource type ID.
func (b *builder) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListResources")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.ListResourcesType
	rb, ok := b.resourceSyncers[request.GetResourceTypeId()]
	if !ok {
		err = status.Errorf(codes.NotFound, "error: list resources with unknown resource type %s", request.GetResourceTypeId())
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	ctx, sessionUsage := session.WithUsageCollector(ctx)
	token := pagination.Token{
		Size:  int(request.GetPageSize()),
		Token: request.GetPageToken(),
	}
	opts, contLookup, err := b.continuationOpAttrs(request.GetActiveSyncId(), token, annotations.Annotations(request.GetAnnotations()))
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}
	out, retOptions, err := rb.List(ctx, request.GetParentResourceId(), opts)
	if retOptions == nil {
		retOptions = &resource.SyncOpResults{}
	}

	askAnnos, deferred, misuseErr := continuationOutcome(contLookup, err)
	if misuseErr != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), misuseErr)
		return nil, misuseErr
	}
	if deferred {
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		// A deferral is a protocol turn, not a failure: clear the
		// handler's ErrLookupDeferred so the span defer above doesn't
		// record every normal ask/answer bounce as a trace error.
		err = nil
		// Session usage rides the ask too: the phase-1 handler may have
		// touched the session before deferring, and the protocol turn is
		// the only response that work will ever produce.
		return v2.ResourcesServiceListResourcesResponse_builder{Annotations: appendSessionUsage(askAnnos, sessionUsage)}.Build(), nil
	}

	resp := v2.ResourcesServiceListResourcesResponse_builder{
		List:          out,
		NextPageToken: retOptions.NextPageToken,
		Annotations:   appendSessionUsage(retOptions.Annotations, sessionUsage),
	}.Build()
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return resp, fmt.Errorf("error: listing resources failed: %w", err)
	}
	if request.GetPageToken() != "" && request.GetPageToken() == retOptions.NextPageToken {
		err = status.Errorf(codes.Internal,
			"listing resources failed: next page token unchanged (token=%s, type=%s, parent=%s) - likely a connector bug",
			request.GetPageToken(), request.GetResourceTypeId(), request.GetParentResourceId())
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return resp, err
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

func (b *builder) GetResource(ctx context.Context, request *v2.ResourceGetterServiceGetResourceRequest) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetResource")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.GetResourceType
	resourceType := request.GetResourceId().GetResourceType()
	rb, ok := b.resourceTargetedSyncers[resourceType]
	if !ok {
		err = status.Errorf(codes.Unimplemented, "error: get resource with unknown resource type %s", resourceType)
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}
	resource, annos, err := rb.Get(ctx, request.GetResourceId(), request.GetParentResourceId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: get resource failed: %w", err)
	}
	if resource == nil {
		err = status.Error(codes.NotFound, "error: get resource returned nil")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.ResourceGetterServiceGetResourceResponse_builder{
		Resource:    resource,
		Annotations: annos,
	}.Build(), nil
}

// ListStaticEntitlements returns all the static entitlements for a given resource type.
// Static entitlements are used to create entitlements for all resources of a given resource type.
func (b *builder) ListStaticEntitlements(ctx context.Context, request *v2.EntitlementsServiceListStaticEntitlementsRequest) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListStaticEntitlements")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.ListStaticEntitlementsType
	rb, ok := b.resourceSyncers[request.GetResourceTypeId()]
	if !ok {
		err = status.Errorf(codes.NotFound, "error: list static entitlements with unknown resource type %s", request.GetResourceTypeId())
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}
	rbse, ok := rb.(StaticEntitlementSyncerV2)
	if !ok {
		// Resource syncer doesn't support static entitlements. Return empty response.
		return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{
			List:          []*v2.Entitlement{},
			NextPageToken: "",
			Annotations:   nil,
		}.Build(), nil
	}

	ctx, sessionUsage := session.WithUsageCollector(ctx)
	token := pagination.Token{
		Size:  int(request.GetPageSize()),
		Token: request.GetPageToken(),
	}
	opts := b.syncOpAttrs(request.GetActiveSyncId(), token)
	out, retOptions, err := rbse.StaticEntitlements(ctx, opts)
	if retOptions == nil {
		retOptions = &resource.SyncOpResults{}
	}

	resp := v2.EntitlementsServiceListStaticEntitlementsResponse_builder{
		List:          out,
		NextPageToken: retOptions.NextPageToken,
		Annotations:   appendSessionUsage(retOptions.Annotations, sessionUsage),
	}.Build()
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: listing static entitlements failed: %w", err)
	}
	if request.GetPageToken() != "" && request.GetPageToken() == retOptions.NextPageToken {
		err = status.Error(codes.Internal, "listing static entitlements failed: next page token unchanged - likely a connector bug")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return resp, err
	}

	if err = validateExclusionGroupAnnotations(out); err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

// ListEntitlements returns all the entitlements for a given resource.
func (b *builder) ListEntitlements(ctx context.Context, request *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListEntitlements")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.ListEntitlementsType

	if request.GetResource() == nil {
		err = status.Error(codes.InvalidArgument, "error: list entitlements requires a resource")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	rid := request.GetResource().GetId()
	rb, ok := b.resourceSyncers[rid.GetResourceType()]
	if !ok {
		err = status.Errorf(codes.NotFound, "error: list entitlements with unknown resource type %s", rid.GetResourceType())
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}
	ctx, sessionUsage := session.WithUsageCollector(ctx)
	token := pagination.Token{
		Size:  int(request.GetPageSize()),
		Token: request.GetPageToken(),
	}
	reqAnnos := annotations.Annotations(request.GetAnnotations())
	opts, contLookup, err := b.continuationOpAttrs(request.GetActiveSyncId(), token, reqAnnos)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	typeScoped := reqAnnos.Contains(&v2.TypeScopedEntitlements{})

	var out []*v2.Entitlement
	var retOptions *resource.SyncOpResults
	if typeScoped {
		// Type-scoped entitlements call (see v2.TypeScopedEntitlements): the
		// request annotation is the routing marker (the resource is a
		// self-referential {type, type} stub to satisfy wire validation).
		// Only legal against a syncer that opted in.
		tses, tsOk := rb.(TypeScopedEntitlementsSyncer)
		if !tsOk {
			err = status.Errorf(codes.InvalidArgument,
				"error: type-scoped list entitlements for resource type %s, but its syncer does not implement TypeScopedEntitlementsSyncer", rid.GetResourceType())
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
			return nil, err
		}
		out, retOptions, err = tses.EntitlementsForResourceType(ctx, rid.GetResourceType(), opts)
	} else {
		out, retOptions, err = rb.Entitlements(ctx, request.GetResource(), opts)
	}
	if retOptions == nil {
		retOptions = &resource.SyncOpResults{}
	}

	askAnnos, deferred, misuseErr := continuationOutcome(contLookup, err)
	if misuseErr != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), misuseErr)
		return nil, misuseErr
	}
	if deferred {
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		// Protocol turn, not a failure — clear ErrLookupDeferred for the
		// span defer; see the ListResources deferred path.
		err = nil
		// Session usage rides the ask; see the ListResources deferred path.
		return v2.EntitlementsServiceListEntitlementsResponse_builder{Annotations: appendSessionUsage(askAnnos, sessionUsage)}.Build(), nil
	}

	resp := v2.EntitlementsServiceListEntitlementsResponse_builder{
		List:          out,
		NextPageToken: retOptions.NextPageToken,
		Annotations:   appendSessionUsage(retOptions.Annotations, sessionUsage),
	}.Build()
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return resp, fmt.Errorf("error: listing entitlements failed: %w", err)
	}
	if request.GetPageToken() != "" && request.GetPageToken() == retOptions.NextPageToken {
		err = status.Error(codes.Internal, "listing entitlements failed: next page token unchanged - likely a connector bug")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return resp, err
	}

	if err = validateExclusionGroupAnnotations(out); err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

// ListGrants lists all the grants for a given resource.
func (b *builder) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListGrants")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.ListGrantsType

	if request.GetResource() == nil {
		err = status.Error(codes.InvalidArgument, "error: list grants requires a resource")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	rid := request.GetResource().GetId()
	rb, ok := b.resourceSyncers[rid.GetResourceType()]
	if !ok {
		err = status.Errorf(codes.NotFound, "error: list grants with unknown resource type %s", rid.GetResourceType())
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	ctx, sessionUsage := session.WithUsageCollector(ctx)
	token := pagination.Token{
		Size:  int(request.GetPageSize()),
		Token: request.GetPageToken(),
	}
	reqAnnos := annotations.Annotations(request.GetAnnotations())
	opts, contLookup, err := b.continuationOpAttrs(request.GetActiveSyncId(), token, reqAnnos)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	typeScoped := reqAnnos.Contains(&v2.TypeScopedGrants{})

	var out []*v2.Grant
	var retOptions *resource.SyncOpResults
	if typeScoped {
		// Type-scoped grants call (see v2.TypeScopedGrants): the request
		// annotation is the routing marker (the resource is a
		// self-referential {type, type} stub to satisfy wire validation).
		// Only legal against a syncer that opted in.
		tsgs, tsOk := rb.(TypeScopedGrantsSyncer)
		if !tsOk {
			err = status.Errorf(codes.InvalidArgument,
				"error: type-scoped list grants for resource type %s, but its syncer does not implement TypeScopedGrantsSyncer", rid.GetResourceType())
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
			return nil, err
		}
		out, retOptions, err = tsgs.GrantsForResourceType(ctx, rid.GetResourceType(), opts)
	} else {
		out, retOptions, err = rb.Grants(ctx, request.GetResource(), opts)
	}
	if retOptions == nil {
		retOptions = &resource.SyncOpResults{}
	}

	askAnnos, deferred, misuseErr := continuationOutcome(contLookup, err)
	if misuseErr != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), misuseErr)
		return nil, misuseErr
	}
	if deferred {
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		// Protocol turn, not a failure — clear ErrLookupDeferred for the
		// span defer; see the ListResources deferred path.
		err = nil
		// Session usage rides the ask; see the ListResources deferred path.
		return v2.GrantsServiceListGrantsResponse_builder{Annotations: appendSessionUsage(askAnnos, sessionUsage)}.Build(), nil
	}

	resp := v2.GrantsServiceListGrantsResponse_builder{
		List:          out,
		Annotations:   appendSessionUsage(retOptions.Annotations, sessionUsage),
		NextPageToken: retOptions.NextPageToken,
	}.Build()

	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return resp, fmt.Errorf("error: listing grants for resource %s/%s failed: %w", rid.GetResourceType(), rid.GetResource(), err)
	}
	if request.GetPageToken() != "" && request.GetPageToken() == retOptions.NextPageToken {
		err = status.Errorf(codes.Internal,
			"listing grants for resource %s/%s failed: next page token unchanged - likely a connector bug",
			rid.GetResourceType(), rid.GetResource())
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return resp, err
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

func newResourceSyncerV1toV2(rb ResourceSyncer) ResourceSyncerV2 {
	return &resourceSyncerV1toV2{rb: rb}
}

type resourceSyncerV1toV2 struct {
	rb ResourceSyncer
}

var _ ResourceSyncerV2 = &resourceSyncerV1toV2{}
var _ StaticEntitlementSyncerV2 = &resourceSyncerV1toV2{}

func (rw *resourceSyncerV1toV2) ResourceType(ctx context.Context) *v2.ResourceType {
	return rw.rb.ResourceType(ctx)
}

func (rw *resourceSyncerV1toV2) List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	resources, pageToken, annos, err := rw.rb.List(ctx, parentResourceID, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return resources, ret, err
}

func (rw *resourceSyncerV1toV2) Entitlements(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	ents, pageToken, annos, err := rw.rb.Entitlements(ctx, r, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return ents, ret, err
}

func (rw *resourceSyncerV1toV2) StaticEntitlements(ctx context.Context, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	rb, ok := rw.rb.(StaticEntitlementSyncer)
	if !ok {
		return nil, &resource.SyncOpResults{NextPageToken: "", Annotations: annotations.Annotations{}}, nil
	}

	ents, pageToken, annos, err := rb.StaticEntitlements(ctx, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return ents, ret, err
}

func (rw *resourceSyncerV1toV2) Grants(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	grants, pageToken, annos, err := rw.rb.Grants(ctx, r, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return grants, ret, err
}

// validateTypeScopedRegistration fails connector construction when a
// resource type's annotations promise a type-scoped phase the syncer
// cannot serve: a TypeScopedGrants / TypeScopedEntitlements annotation
// without the matching TypeScoped*Syncer interface would otherwise die
// mid-sync with InvalidArgument on the first type-scoped call — a
// runtime failure for what is a wiring bug, caught here at build.
//
// The reverse (interface implemented, annotation absent) is deliberately
// allowed: shared embedded bases may implement the interface across many
// syncers while only some types opt in via the annotation, and the
// unreferenced method is harmless.
func validateTypeScopedRegistration(rType *v2.ResourceType, in any) error {
	rtAnnos := annotations.Annotations(rType.GetAnnotations())
	if rtAnnos.Contains(&v2.TypeScopedGrants{}) {
		if _, ok := in.(TypeScopedGrantsSyncer); !ok {
			return fmt.Errorf(
				"resource type %s carries the TypeScopedGrants annotation but its syncer does not implement TypeScopedGrantsSyncer",
				rType.GetId())
		}
	}
	if rtAnnos.Contains(&v2.TypeScopedEntitlements{}) {
		if _, ok := in.(TypeScopedEntitlementsSyncer); !ok {
			return fmt.Errorf(
				"resource type %s carries the TypeScopedEntitlements annotation but its syncer does not implement TypeScopedEntitlementsSyncer",
				rType.GetId())
		}
	}
	return nil
}

func (b *builder) addTargetedSyncer(_ context.Context, typeId string, in any) error {
	if targetedSyncer, ok := in.(ResourceTargetedSyncerLimited); ok {
		if _, ok := b.resourceTargetedSyncers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource targeted syncer %s", typeId)
		}
		b.resourceTargetedSyncers[typeId] = targetedSyncer
	}
	return nil
}

func (b *builder) addResourceSyncers(ctx context.Context, typeId string, in any) error {
	// no duplicates
	if _, ok := b.resourceSyncers[typeId]; ok {
		return fmt.Errorf("error: duplicate resource type found for resource builder %s", typeId)
	}

	if rb, ok := in.(ResourceSyncer); ok {
		b.resourceSyncers[typeId] = newResourceSyncerV1toV2(rb)
	}

	if rb, ok := in.(ResourceSyncerV2); ok {
		b.resourceSyncers[typeId] = rb
	}

	// A resource syncer is required
	if _, ok := b.resourceSyncers[typeId]; !ok {
		return fmt.Errorf("error: the resource syncer interface must be implemented for all types (%s)", typeId)
	}

	// Check for resource actions
	if actionProvider, ok := in.(ResourceActionProvider); ok {
		registry, err := b.actionManager.GetTypeRegistry(ctx, typeId)
		if err != nil {
			return fmt.Errorf("error getting resource type action registry for %s: %w", typeId, err)
		}
		err = actionProvider.ResourceActions(ctx, registry)
		if err != nil {
			return fmt.Errorf("error getting resource actions for %s: %w", typeId, err)
		}
	}

	return nil
}

// validateExclusionGroupAnnotations checks that each entitlement has at most one
// EntitlementExclusionGroup annotation. An entitlement may belong to at most one
// exclusion group.
func validateExclusionGroupAnnotations(ents []*v2.Entitlement) error {
	for _, ent := range ents {
		count := 0
		for _, a := range ent.GetAnnotations() {
			if a.MessageIs(&v2.EntitlementExclusionGroup{}) {
				count++
				if count > 1 {
					return status.Errorf(codes.InvalidArgument,
						"entitlement %s has multiple ExclusionGroup annotations; "+
							"an entitlement may belong to at most one exclusion group",
						ent.GetId())
				}
			}
		}
	}
	return nil
}
