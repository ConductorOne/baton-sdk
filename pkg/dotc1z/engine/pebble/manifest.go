package pebble

import (
	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// BuildManifest assembles the v3 envelope manifest for a Pebble-engine c1z
// using the given payload encoding. Used by CloneSync and by pkg/dotc1z's
// Pebble store when saving the envelope at Close.
func BuildManifest(encoding c1zstore.PayloadEncoding) (*c1zv3.C1ZManifestV3, error) {
	descriptors, err := formatv3.BuildDescriptorClosure()
	if err != nil {
		return nil, err
	}
	return c1zv3.C1ZManifestV3_builder{
		Engine:              string(c1zstore.EnginePebble),
		EngineSchemaVersion: uint32(SDKPebbleFormat),
		PayloadEncoding:     payloadEncodingToProto(encoding),
		Descriptors:         descriptors,
	}.Build(), nil
}

// payloadEncodingToProto maps the public c1zstore.PayloadEncoding to
// the c1zv3 enum. PayloadEncodingUnspecified means "engine default"
// for our purposes: TAR_ZSTD.
func payloadEncodingToProto(enc c1zstore.PayloadEncoding) c1zv3.PayloadEncoding {
	switch enc {
	case c1zstore.PayloadEncodingTar:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR
	case c1zstore.PayloadEncodingTarZstd, c1zstore.PayloadEncodingUnspecified:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD
	default:
		// Any non-enumerated value falls back to the default.
		// WriteEnvelope will reject any non-TAR/non-TAR_ZSTD value
		// before writing bytes, so a caller setting a bogus
		// encoding gets an error at save time.
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD
	}
}
