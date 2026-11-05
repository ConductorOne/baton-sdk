// Package predelegation holds a test that must run in its own test binary:
// it checks that metric handlers created before uotel.InitOtel export once
// InitOtel installs the global MeterProvider. The otel global provider
// delegates only once per process, so no other test may set it first.
package predelegation
