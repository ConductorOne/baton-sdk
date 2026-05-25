// Package microtests holds the 5 risk-validation tests that were
// written to prove the material design choices in RFC 0004 (storage
// engine v4) actually hold under Pebble's real behavior. They were
// originally at /tmp/baton-rfc-microtests/ during
// the RFC pressure-test phase; lifting them into the repo keeps them
// as regression tests against the production code.
//
// The 5 tests (one per file):
//
//   - tuple_test.go — prefix-free property of the tuple key
//     encoding. Critical: range scans give wrong results if encoded
//     keys aren't prefix-free.
//   - compress_test.go — DBCompressionGood (zstd at L5/L6 + Snappy at
//     upper levels) gives the expected size + decompress speed.
//   - checkpoint_test.go — db.Checkpoint produces a self-contained
//     directory that reopens cleanly.
//   - ingest_excise_test.go — IngestAndExcise atomically replaces a
//     key range with an externally-built SST.
//   - codec_perf_test.go — codegen-emitted codecs are ~5× faster than
//     reflection-based codecs (justifies the hybrid registry).
package microtests
