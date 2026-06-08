package uotel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// SyncIdentity carries the platform identifiers for a connector sync. It is
// propagated through the context so spans deep in the sync and dotc1z call
// trees can be filtered to a single connector in APM. The attribute keys
// match the pprof.Do labels set by the platform sync activity, so spans and
// CPU profiles line up on the same dimensions.
type SyncIdentity struct {
	TenantID    string
	AppID       string
	ConnectorID string
	CatalogID   string
	CatalogName string
}

// IsZero reports whether id has no fields set.
func (id SyncIdentity) IsZero() bool {
	return id == SyncIdentity{}
}

// Attrs returns the non-empty identity fields as span attributes.
func (id SyncIdentity) Attrs() []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5)
	if id.TenantID != "" {
		attrs = append(attrs, attribute.String("tenant_id", id.TenantID))
	}
	if id.AppID != "" {
		attrs = append(attrs, attribute.String("app_id", id.AppID))
	}
	if id.ConnectorID != "" {
		attrs = append(attrs, attribute.String("connector_id", id.ConnectorID))
	}
	if id.CatalogID != "" {
		attrs = append(attrs, attribute.String("catalog_id", id.CatalogID))
	}
	if id.CatalogName != "" {
		attrs = append(attrs, attribute.String("catalog_name", id.CatalogName))
	}
	return attrs
}

type syncIdentityKey struct{}

// WithSyncIdentity returns a child of ctx carrying id. It only stores the
// identity; spans are stamped where SetSyncIdentityAttrs(ctx, span) is called,
// not here.
func WithSyncIdentity(ctx context.Context, id SyncIdentity) context.Context {
	return context.WithValue(ctx, syncIdentityKey{}, id)
}

// SyncIdentityFromContext returns the identity set by WithSyncIdentity.
func SyncIdentityFromContext(ctx context.Context) (SyncIdentity, bool) {
	id, ok := ctx.Value(syncIdentityKey{}).(SyncIdentity)
	return id, ok
}

// SetSyncIdentityAttrs stamps the connector identity carried in ctx onto span.
// No-op when ctx carries no identity, so it is safe to call on every span.
func SetSyncIdentityAttrs(ctx context.Context, span trace.Span) {
	if id, ok := SyncIdentityFromContext(ctx); ok {
		span.SetAttributes(id.Attrs()...)
	}
}
