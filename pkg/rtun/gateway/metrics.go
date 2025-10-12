package gateway

import (
	"context"

	sdkmetrics "github.com/conductorone/baton-sdk/pkg/metrics"
)

type gwMetrics struct {
	h sdkmetrics.Handler

	openReq    sdkmetrics.Int64Counter
	openOK     sdkmetrics.Int64Counter
	openMiss   sdkmetrics.Int64Counter
	framesRx   sdkmetrics.Int64Counter
	framesTx   sdkmetrics.Int64Counter
	writerDrop sdkmetrics.Int64Counter
	writeErr   sdkmetrics.Int64Counter
}

func newGwMetrics(h sdkmetrics.Handler) *gwMetrics {
	return &gwMetrics{
		h:          h,
		openReq:    h.Int64Counter("rtun.gateway.open_requests_total", "gateway open requests", sdkmetrics.Dimensionless),
		openOK:     h.Int64Counter("rtun.gateway.open_success_total", "gateway opens succeeded", sdkmetrics.Dimensionless),
		openMiss:   h.Int64Counter("rtun.gateway.open_not_found_total", "gateway opens not found", sdkmetrics.Dimensionless),
		framesRx:   h.Int64Counter("rtun.gateway.frame_rx_total", "gateway frames received", sdkmetrics.Dimensionless),
		framesTx:   h.Int64Counter("rtun.gateway.frame_tx_total", "gateway frames sent", sdkmetrics.Dimensionless),
		writerDrop: h.Int64Counter("rtun.gateway.writer_queue_drops_total", "gateway writer queue drops", sdkmetrics.Dimensionless),
		writeErr:   h.Int64Counter("rtun.gateway.writer_write_errors_total", "gateway write errors", sdkmetrics.Dimensionless),
	}
}

func (m *gwMetrics) addOpenReq(ctx context.Context)  { m.openReq.Add(ctx, 1, nil) }
func (m *gwMetrics) addOpenOK(ctx context.Context)   { m.openOK.Add(ctx, 1, nil) }
func (m *gwMetrics) addOpenMiss(ctx context.Context) { m.openMiss.Add(ctx, 1, nil) }
func (m *gwMetrics) addFrameRx(ctx context.Context, k string) {
	m.framesRx.Add(ctx, 1, map[string]string{"kind": k})
}
func (m *gwMetrics) addFrameTx(ctx context.Context, k string) {
	m.framesTx.Add(ctx, 1, map[string]string{"kind": k})
}
func (m *gwMetrics) addWriterDrop(ctx context.Context) { m.writerDrop.Add(ctx, 1, nil) }
func (m *gwMetrics) addWriteErr(ctx context.Context)   { m.writeErr.Add(ctx, 1, nil) }
