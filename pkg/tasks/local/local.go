package local

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("baton-sdk/pkg.tasks.local")
