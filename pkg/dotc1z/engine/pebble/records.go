package pebble

import (
	"sync"
	"sync/atomic"

	v2pb "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

type Records struct {
	e *Engine

	// Embedded Unimplemented stubs for the gRPC service surfaces Records
	// implements partially (adapter.go / adapter_reader.go). Each method
	// implemented on Records overrides the stub. GrantsReaderServiceServer
	// is deliberately NOT stubbed: Records implements it in full, and
	// adapter_reader.go asserts the complete contract — re-adding the
	// stub would make that assertion vacuous.
	v2pb.UnimplementedResourceTypesServiceServer
	reader_v2.UnimplementedResourceTypesReaderServiceServer
	v2pb.UnimplementedResourcesServiceServer
	reader_v2.UnimplementedResourcesReaderServiceServer
	v2pb.UnimplementedEntitlementsServiceServer
	reader_v2.UnimplementedEntitlementsReaderServiceServer
	v2pb.UnimplementedGrantsServiceServer

	// Lazy bare-id entitlement lookup (see lookup.go). entIDLookupGen is
	// bumped by every entitlement-keyspace mutation; the map rebuilds on the
	// next lookup when its built generation is stale.
	entIDLookupGen      atomic.Uint64
	entIDLookupMu       sync.Mutex
	entIDLookup         map[string][]entitlementIdentity
	entIDLookupBuiltGen uint64
}

func (e *Engine) Records() *Records { return &e.records }
