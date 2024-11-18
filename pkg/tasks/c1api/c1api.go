package c1api

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("baton-sdk/pkg.tasks.c1api")
