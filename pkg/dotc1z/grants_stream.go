package dotc1z

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// grantStreamPageSize is the internal page size used by the streaming grant readers.
// Bounds peak heap to ~pageSize × sizeof(*v2.Grant) per call; the constant trades
// heap-bounding strength against query overhead. modernc.org/sqlite (pure-Go) has
// a meaningful per-query cost, so very small pages (<500) make the streaming
// variant dramatically slower than the slice variant on large entitlements. 1000
// gives a 10× peak-heap reduction vs the 10000 maxPageSize default with manageable
// latency cost; see BenchmarkListGrantsForEntitlementStream in grants_bench_test.go.
const grantStreamPageSize = 1000

// ListGrantsForEntitlementStream invokes scan for each grant matching the request,
// transparently paging through the underlying grants table in fixed-size batches.
// Peak heap is bounded to ~grantStreamPageSize decoded grants per call regardless
// of total grant count on the entitlement.
//
// Unlike ListGrantsForEntitlement, this method does NOT hold a SQL cursor while
// scan runs — each batch's rows are closed before scan is invoked. scan MAY
// safely call back into other *C1File methods (e.g. GetResourceType) without
// deadlocking against the pinned writer connection.
//
// If scan returns a non-nil error, iteration stops and that error is returned.
// Context cancellation is checked between batches.
//
// The caller-provided PageSize and PageToken on request are ignored; internal
// paging is fixed and starts from the beginning. All other request fields
// (Entitlement, Annotations, …) are honored.
func (c *C1File) ListGrantsForEntitlementStream(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
	scan func(*v2.Grant) error,
) (retErr error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForEntitlementStream")
	defer func() { uotel.EndSpanWithError(span, retErr) }()

	if scan == nil {
		return fmt.Errorf("ListGrantsForEntitlementStream: scan callback is nil")
	}

	inner := proto.Clone(request).(*reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest)
	inner.PageSize = grantStreamPageSize
	inner.PageToken = ""

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), inner, func() *v2.Grant { return &v2.Grant{} })
		if err != nil {
			return fmt.Errorf("error streaming grants for entitlement '%s': %w", request.GetEntitlement().GetId(), err)
		}

		for _, g := range batch {
			if err := scan(g); err != nil {
				return err
			}
		}

		if nextPageToken == "" {
			return nil
		}
		inner.PageToken = nextPageToken
	}
}

// ListGrantsForPrincipalStream is the streaming counterpart to ListGrantsForPrincipal.
// Semantics mirror ListGrantsForEntitlementStream: peak-heap-bounded paging, no SQL
// cursor held across scan, callback may safely re-enter *C1File.
func (c *C1File) ListGrantsForPrincipalStream(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
	scan func(*v2.Grant) error,
) (retErr error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForPrincipalStream")
	defer func() { uotel.EndSpanWithError(span, retErr) }()

	if scan == nil {
		return fmt.Errorf("ListGrantsForPrincipalStream: scan callback is nil")
	}

	inner := proto.Clone(request).(*reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest)
	inner.PageSize = grantStreamPageSize
	inner.PageToken = ""

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), inner, func() *v2.Grant { return &v2.Grant{} })
		if err != nil {
			return fmt.Errorf("error streaming grants for principal '%s': %w", request.GetPrincipalId(), err)
		}

		for _, g := range batch {
			if err := scan(g); err != nil {
				return err
			}
		}

		if nextPageToken == "" {
			return nil
		}
		inner.PageToken = nextPageToken
	}
}

// ListGrantsForResourceTypeStream is the streaming counterpart to ListGrantsForResourceType.
// Semantics mirror ListGrantsForEntitlementStream.
func (c *C1File) ListGrantsForResourceTypeStream(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
	scan func(*v2.Grant) error,
) (retErr error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForResourceTypeStream")
	defer func() { uotel.EndSpanWithError(span, retErr) }()

	if scan == nil {
		return fmt.Errorf("ListGrantsForResourceTypeStream: scan callback is nil")
	}

	inner := proto.Clone(request).(*reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest)
	inner.PageSize = grantStreamPageSize
	inner.PageToken = ""

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), inner, func() *v2.Grant { return &v2.Grant{} })
		if err != nil {
			return fmt.Errorf("error streaming grants for resource type '%s': %w", request.GetResourceTypeId(), err)
		}

		for _, g := range batch {
			if err := scan(g); err != nil {
				return err
			}
		}

		if nextPageToken == "" {
			return nil
		}
		inner.PageToken = nextPageToken
	}
}
