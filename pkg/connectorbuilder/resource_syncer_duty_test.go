package connectorbuilder

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/duty"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/anypb"
)

func dutyAny(t *testing.T, mappings ...duty.Mapping) *anypb.Any {
	t.Helper()
	list := make([]*v2.DutyMapping, 0, len(mappings))
	for _, m := range mappings {
		list = append(list, m.Proto())
	}
	a, err := anypb.New(v2.DutyMappingsAnnotation_builder{CatalogId: duty.CatalogID, Mappings: list}.Build())
	require.NoError(t, err)
	return a
}

func TestWarnInvalidDutyAnnotations(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	ctx := ctxzap.ToContext(t.Context(), zap.New(core))

	ents := []*v2.Entitlement{
		v2.Entitlement_builder{Id: "valid", Annotations: []*anypb.Any{dutyAny(t, duty.Exact(duty.PaymentRelease))}}.Build(),
		v2.Entitlement_builder{Id: "bad-id", Annotations: []*anypb.Any{dutyAny(t, duty.Exact("Payment Release"))}}.Build(),
		v2.Entitlement_builder{Id: "twice", Annotations: []*anypb.Any{
			dutyAny(t, duty.Exact(duty.PaymentRelease)),
			dutyAny(t, duty.Inclusive(duty.PaymentPrepare)),
		}}.Build(),
		v2.Entitlement_builder{Id: "none"}.Build(),
	}
	warnInvalidDutyAnnotations(ctx, ents)

	var ids []string
	for _, entry := range observed.All() {
		ids = append(ids, entry.ContextMap()["entitlement_id"].(string))
	}
	require.Equal(t, []string{"bad-id", "twice"}, ids)
	require.Len(t, ents[1].GetAnnotations(), 1, "invalid annotations pass through unchanged")
}
