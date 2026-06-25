package dotc1z

import (
	"context"
	"errors"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/conductorone/baton-sdk/pkg/uotel"
)

var (
	tracer = otel.Tracer("baton-sdk/pkg.dotc1z")
	meter  = otel.Meter("baton-sdk/pkg.dotc1z")

	// One increment per grant written. The slim attribute lets you
	// confirm a slim-flipped connector is actually producing slim rows.
	// Fires per grant — don't add high-cardinality attributes.
	grantWriteCounter, _ = meter.Int64Counter(
		"c1z_grant_writes_total",
		metric.WithDescription("Grant writes through the c1z writer. Attribute: slim (bool)."),
	)

	// Increments when unsafeForSlim keeps a grant full-blob on a
	// slim-enabled connector. Use to measure how often the hatch fires.
	grantUnsafeForSlimCounter, _ = meter.Int64Counter(
		"c1z_grant_unsafe_for_slim_total",
		metric.WithDescription("Grants kept full-blob by the unsafeForSlim escape hatch on slim-enabled writers."),
	)

	// Pre-built so we don't allocate a fresh attribute.Set on every
	// grant write. WithAttributes sorts and dedups on each call;
	// WithAttributeSet on a shared Set is allocation-free.
	slimWriteAttrTrue  = metric.WithAttributeSet(attribute.NewSet(attribute.Bool("slim", true)))
	slimWriteAttrFalse = metric.WithAttributeSet(attribute.NewSet(attribute.Bool("slim", false)))

	// On-disk c1z size, recorded after vacuum and finalize. Tagged by
	// catalog_name only — connector_id is too high-cardinality for a metric
	// and is available per-connector on the spans instead. The phase attribute
	// separates the post-VACUUM uncompressed db ("vacuum") from the compressed
	// output file ("finalize").
	c1zSizeGauge, _ = meter.Int64Gauge(
		"c1z_size_bytes",
		metric.WithDescription("On-disk c1z size in bytes. Attributes: phase (vacuum|finalize), catalog_name."),
		metric.WithUnit("By"),
	)
)

// recordC1ZSize emits the c1z size gauge for phase, tagged by catalog_name when known.
func recordC1ZSize(ctx context.Context, phase string, sizeBytes int64) {
	if sizeBytes <= 0 {
		return
	}
	attrs := []attribute.KeyValue{attribute.String("phase", phase)}
	if id, ok := uotel.SyncIdentityFromContext(ctx); ok && id.CatalogName != "" {
		attrs = append(attrs, attribute.String("catalog_name", id.CatalogName))
	}
	c1zSizeGauge.Record(ctx, sizeBytes, metric.WithAttributes(attrs...))
}

// NewC1ZFileDecoder wraps a given .c1z io.Reader that validates the .c1z and decompresses/decodes the underlying file.
// Defaults: 128MiB max memory and 3GiB max decoded size
// You must close the resulting io.ReadCloser when you are done, do not forget to close the given io.Reader if necessary.
func NewC1ZFileDecoder(f io.Reader, opts ...DecoderOption) (io.ReadCloser, error) {
	return NewDecoder(f, opts...)
}

// C1ZFileCheckHeader reads len(C1ZFileHeader) bytes from the given io.ReadSeeker and compares them to C1ZFileHeader.
// Returns true if the header is valid. Returns any errors from Read() or Seek().
// If a nil error is returned, the given io.ReadSeeker will be pointing to the first byte of the stream, and is suitable
// to be passed to NewC1ZFileDecoder.
func C1ZFileCheckHeader(f io.ReadSeeker) (bool, error) {
	// Read header
	err := ReadHeader(f)

	// Seek back to start
	_, seekErr := f.Seek(0, 0)
	if seekErr != nil {
		return false, err
	}

	if err != nil {
		if errors.Is(err, ErrInvalidFile) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}
