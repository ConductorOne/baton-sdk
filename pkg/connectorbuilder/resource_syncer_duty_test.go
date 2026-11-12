package connectorbuilder

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/duty"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/anypb"
)

func dutyAny(t *testing.T, mappings ...duty.Mapping) *anypb.Any {
	t.Helper()
	list := make([]*v2.DutyMapping, 0, len(mappings))
	for _, m := range mappings {
		list = append(list, m.Proto())
	}
	a, err := anypb.New(v2.DutyMappingsAnnotation_builder{CatalogVersion: duty.CatalogVersion, Mappings: list}.Build())
	require.NoError(t, err)
	return a
}

func TestWarnInvalidDutyAnnotations(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	ctx := ctxzap.ToContext(t.Context(), zap.New(core))

	ents := []*v2.Entitlement{
		v2.Entitlement_builder{Id: "valid", Annotations: []*anypb.Any{dutyAny(t, duty.Exact(duty.PaymentRelease))}}.Build(),
		v2.Entitlement_builder{Id: "unknown", Annotations: []*anypb.Any{dutyAny(t, duty.Exact("custom.one"), duty.Exact("custom-two"))}}.Build(),
		v2.Entitlement_builder{Id: "bad-id", Annotations: []*anypb.Any{dutyAny(t, duty.Exact("Payment Release"))}}.Build(),
		v2.Entitlement_builder{Id: "twice", Annotations: []*anypb.Any{
			dutyAny(t, duty.Exact(duty.PaymentRelease)),
			dutyAny(t, duty.Inclusive(duty.PaymentPrepare), duty.Exact("custom.one")),
		}}.Build(),
		v2.Entitlement_builder{Id: "none"}.Build(),
	}
	warnInvalidDutyAnnotations(ctx, ents)

	warns := observed.FilterLevelExact(zapcore.WarnLevel).All()
	require.Len(t, warns, 1, "one warning per page")
	fields := warns[0].ContextMap()
	require.Equal(t, int64(2), fields["invalid_entitlements"])
	require.Equal(t, "bad-id", fields["sample_entitlement_id"])
	require.Contains(t, fields["error"], "DutyId")

	debugs := observed.FilterLevelExact(zapcore.DebugLevel).All()
	require.Len(t, debugs, 1)
	require.Equal(t, int64(3), debugs[0].ContextMap()["unknown_duty_ids"])
	require.Equal(t, []any{"custom.one", "custom-two", "Payment Release"}, debugs[0].ContextMap()["sample_duty_ids"])

	require.Len(t, ents[2].GetAnnotations(), 1, "invalid annotations pass through unchanged")
	require.Len(t, ents[3].GetAnnotations(), 2)
}

func TestWarnInvalidDutyAnnotationsSamples(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	ctx := ctxzap.ToContext(t.Context(), zap.New(core))

	var mappings []duty.Mapping
	for _, id := range []string{"a.a", "b.b", "c.c", "d.d", "e.e", "f.f", "g.g"} {
		mappings = append(mappings, duty.Exact(id))
	}
	warnInvalidDutyAnnotations(ctx, []*v2.Entitlement{v2.Entitlement_builder{Id: "e", Annotations: []*anypb.Any{dutyAny(t, mappings...)}}.Build()})

	require.Empty(t, observed.FilterLevelExact(zapcore.WarnLevel).All())
	debugs := observed.FilterLevelExact(zapcore.DebugLevel).All()
	require.Len(t, debugs, 1)
	require.Equal(t, int64(7), debugs[0].ContextMap()["unknown_duty_ids"])
	require.Len(t, debugs[0].ContextMap()["sample_duty_ids"], maxUnknownDutySamples)
}
