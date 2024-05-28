package metrics

import "context"

type noopRecorder struct{}

func (*noopRecorder) Record(_ context.Context, _ int64) {}

func (*noopRecorder) Add(_ context.Context, _ int64) {}

func (*noopRecorder) Observe(_ context.Context, _ int64) {}

var _ Int64Counter = (*noopRecorder)(nil)
var _ Int64Histogram = (*noopRecorder)(nil)
var _ Int64Gauge = (*noopRecorder)(nil)

type noopHandler struct{}

func (*noopHandler) Int64Counter(_ string, _ string, _ Unit) Int64Counter {
	return &noopRecorder{}
}

func (*noopHandler) Int64Gauge(_ string, _ string, _ Unit) Int64Gauge {
	return &noopRecorder{}
}

func (*noopHandler) Int64Histogram(_ string, _ string, _ Unit) Int64Histogram {
	return &noopRecorder{}
}

var _ Handler = (*noopHandler)(nil)

func NewNoOpHandler(_ context.Context) Handler {
	return &noopHandler{}
}
