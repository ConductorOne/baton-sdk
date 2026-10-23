package server

import (
	"context"

	sdkmetrics "github.com/conductorone/baton-sdk/pkg/metrics"
)

type serverMetrics struct {
	h sdkmetrics.Handler

	helloTimeoutCtr    sdkmetrics.Int64Counter
	helloPortsOverCtr  sdkmetrics.Int64Counter
	helloRejectedCtr   sdkmetrics.Int64Counter
	regRegisterCtr     sdkmetrics.Int64Counter
	regUnregisterCtr   sdkmetrics.Int64Counter
	reverseDialOKCtr   sdkmetrics.Int64Counter
	reverseDialMissCtr sdkmetrics.Int64Counter
}

func newServerMetrics(h sdkmetrics.Handler) *serverMetrics {
	return &serverMetrics{
		h:                  h,
		helloTimeoutCtr:    h.Int64Counter("rtun.server.hello_timeout_total", "HELLO timeouts", sdkmetrics.Dimensionless),
		helloPortsOverCtr:  h.Int64Counter("rtun.server.hello_ports_over_limit_total", "HELLO ports over limit", sdkmetrics.Dimensionless),
		helloRejectedCtr:   h.Int64Counter("rtun.server.hello_rejected_total", "HELLO rejected for reason", sdkmetrics.Dimensionless),
		regRegisterCtr:     h.Int64Counter("rtun.server.registry_register_total", "registry register", sdkmetrics.Dimensionless),
		regUnregisterCtr:   h.Int64Counter("rtun.server.registry_unregister_total", "registry unregister", sdkmetrics.Dimensionless),
		reverseDialOKCtr:   h.Int64Counter("rtun.server.reverse_dial_success_total", "reverse dial success", sdkmetrics.Dimensionless),
		reverseDialMissCtr: h.Int64Counter("rtun.server.reverse_dial_not_found_total", "reverse dial not found", sdkmetrics.Dimensionless),
	}
}

func (m *serverMetrics) helloTimeout(ctx context.Context) { m.helloTimeoutCtr.Add(ctx, 1, nil) }
func (m *serverMetrics) helloPortsOverLimit(ctx context.Context) {
	m.helloPortsOverCtr.Add(ctx, 1, nil)
}
func (m *serverMetrics) helloRejected(ctx context.Context, reason string) {
	m.helloRejectedCtr.Add(ctx, 1, map[string]string{"reason": reason})
}
func (m *serverMetrics) registryRegister(ctx context.Context)   { m.regRegisterCtr.Add(ctx, 1, nil) }
func (m *serverMetrics) registryUnregister(ctx context.Context) { m.regUnregisterCtr.Add(ctx, 1, nil) }
func (m *serverMetrics) reverseDialSuccess(ctx context.Context) { m.reverseDialOKCtr.Add(ctx, 1, nil) }
func (m *serverMetrics) reverseDialNotFound(ctx context.Context) {
	m.reverseDialMissCtr.Add(ctx, 1, nil)
}
