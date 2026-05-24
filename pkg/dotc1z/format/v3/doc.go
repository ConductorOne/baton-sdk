// Package v3 implements the C1Z3 envelope format: 5-byte magic, a
// length-prefixed proto manifest, and a payload (typically a zstd-tar
// of a Pebble directory).
//
// The envelope is independent of the engine it carries. v3 readers
// inspect the manifest's `engine` field to dispatch; new engines plug
// in without an envelope change. The descriptor sidecar makes every
// v3 file self-describing — any reader can resolve every stored
// record's fields without compiled-in protos for that record type.
//
// Build tag: gated by `//go:build batonsdkv2`. Default connector
// builds never link the v3 envelope code (or its Pebble transitive
// deps).
package v3
