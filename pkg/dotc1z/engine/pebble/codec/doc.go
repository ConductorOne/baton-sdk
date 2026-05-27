// Package codec implements the v3 storage engine's record codec layer.
//
// The codec layer encodes and decodes c1.storage.v3 record types onto
// Pebble's key-value primitives. It uses a hybrid pattern:
//
//   - Generated, typed codecs for the SDK's six built-in record types,
//     registered via init() during package import. The hot write path
//     dispatches to these.
//
//   - Cached descriptor-reflection codecs (*ReflectCodec) for any
//     other proto message — debug paths, manifest descriptor walking,
//     potential future extension protos. Lazily constructed on first
//     Lookup miss and cached process-wide.
package codec
