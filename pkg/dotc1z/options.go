package dotc1z

import (
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type c1zOptions struct {
	tmpDir         string
	decoderOptions []DecoderOption
	readOnly       bool
	syncLimit      int
	skipCleanup    bool

	// disableGrantDigestIndex turns off the Pebble engine's seal-time
	// build of the by_entitlement_principal_hash index + grant digests.
	// Inverted so the zero value keeps the build on (current behavior).
	// See WithGrantDigestIndex.
	disableGrantDigestIndex bool

	// engine is the storage engine to use for newly created files.
	// Reads dispatch on magic byte regardless. NewStore defaults an
	// unset engine to EnginePebble.
	engine c1zstore.Engine

	// payloadEncoding controls the v3 envelope payload framing. Allowed
	// values: PayloadEncodingTarZstd (default), PayloadEncodingTar.
	// Zero value means PayloadEncodingTarZstd.
	payloadEncoding c1zstore.PayloadEncoding

	// decoderPool optionally scopes v3 payload-decoder reuse to the
	// caller's operation. See WithDecoderPool.
	decoderPool *EnvelopeDecoderPool
}

type C1ZOption func(*c1zOptions)

// WithTmpDir sets the temporary directory to extract the c1z file to.
// If not provided, os.TempDir() will be used.
func WithTmpDir(tmpDir string) C1ZOption {
	return func(o *c1zOptions) {
		o.tmpDir = tmpDir
	}
}

func WithDecoderOptions(opts ...DecoderOption) C1ZOption {
	return func(o *c1zOptions) {
		o.decoderOptions = opts
	}
}

// WithReadOnly opens the c1z file in read only mode. Modifying the c1z will result in an error on close.
func WithReadOnly(readOnly bool) C1ZOption {
	return func(o *c1zOptions) {
		o.readOnly = readOnly
	}
}

// WithSkipCleanup skips cleanup of old syncs when set to true.
func WithSkipCleanup(skip bool) C1ZOption {
	return func(o *c1zOptions) {
		o.skipCleanup = skip
	}
}

// WithSyncLimit sets the number of syncs to keep during cleanup.
// If not set, defaults to 2 (or BATON_KEEP_SYNC_COUNT env var if set).
func WithSyncLimit(limit int) C1ZOption {
	return func(o *c1zOptions) {
		o.syncLimit = limit
	}
}

// WithEngine sets the storage engine for newly created .c1z files.
// EnginePebble is the default and the only registered engine; any other
// value makes NewStore return ErrEngineNotAvailable.
func WithEngine(engine c1zstore.Engine) C1ZOption {
	return func(o *c1zOptions) {
		o.engine = engine
	}
}

// WithGrantDigestIndex toggles the Pebble engine's seal-time build of
// the by_entitlement_principal_hash index and per-entitlement grant
// digests (the index cross-file grant diffing reads). Default true.
//
// Pass false for files that will never be grant-diffed (local CLI syncs,
// connector development) to skip the seal-time derivation pass. Safe to
// toggle: a file sealed with this off stores no digest roots, which
// readers treat as "missing — recalculate", never as "no grants".
func WithGrantDigestIndex(enabled bool) C1ZOption {
	return func(o *c1zOptions) {
		o.disableGrantDigestIndex = !enabled
	}
}

// WithPayloadEncoding selects the c1z v3 envelope payload encoding
// for newly created files. Default is PayloadEncodingTarZstd.
// PayloadEncodingTar skips the outer zstd compression — useful when
// Pebble's L5/L6 SSTs are already zstd-compressed at the engine layer,
// or when the storage target (S3 with Content-Encoding negotiation,
// etc.) compresses in transit.
func WithPayloadEncoding(enc c1zstore.PayloadEncoding) C1ZOption {
	return func(o *c1zOptions) {
		o.payloadEncoding = enc
	}
}
