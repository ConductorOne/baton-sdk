# Testing and confidence suites

The repository has several classes of verification with intentionally
different cost and infrastructure requirements. Use the named Make targets
instead of invoking hidden `BATON_*` switches directly.

Run `make help` for the current target list.

## CI-equivalent tests

`make test` runs the ordinary Go test suite with the same build tag used by
CI. Pull-request CI also runs lint, protobuf checks, and the full build.

Tests in this tier should be deterministic, self-contained, and reasonably
fast. A test that only skips on Windows with `testing.Short()` is still a CI
test on the other platforms.

Long-running tests use two repository-wide opt-ins:

- `BATON_TEST_EXTRA=1` enables deterministic exhaustive and boundary-scale
  tests.
- `BATON_TEST_NIGHTLY=1` enables randomized, repeated, and full-corpus tests;
  it also satisfies extra-tier guards.

The named Make targets set these variables. Direct `go test` invocations omit
both tiers unless the caller explicitly sets the corresponding variable.

## Bounded checks omitted from CI

`make test-extra` is the memorable pre-merge/pre-release command. It composes:

- `make race-check` — ordinary and extra-tier tests under the race detector.
- `make compat-check` — exchanges real checkpoint artifacts between HEAD and
  a pinned older SDK release. Override the old release with
  `BATON_COMPAT_OLD_REF=<tag>`.
- `make interrupt-check` — runs both the exhaustive in-process checkpoint-cut
  sweep and the real-process crash/resume harness.
- `make fuzz-smoke` — runs each native Go fuzzer for a bounded duration.
  Override it with `FUZZ_TIME=2m`, for example.
- `make differential-check` — compares complete SQLite and Pebble artifacts
  over generated cases for a bounded duration. Override it with
  `DIFFERENTIAL_TIME=5m`.
- `make bench-smoke` — executes the checkpoint cost curves once, catching
  crashes and making the current cost visible without running every
  benchmark in the repository.

These checks are opt-in because they use the race detector, build additional
SDK versions, launch and kill real processes, or have wall-clock runtimes
that are inappropriate for every pull request.

## Nightly checks

`make test-nightly` enables both opt-in tiers, runs `test-extra` with longer
fuzz durations, and then adds:

- `make scheduler-soak` — randomized scheduler fan-out and failure histories
  under the race detector.
- the complete deterministic chaos corpora and randomized source-cache,
  integrity, and WAL lifecycle checks.
- `make chaos-soak` — extended seeded chaos connector fan-out schedules.
- `make errorfs-soak` — randomized whole-sync Pebble failure-point sweeps
  against a crashable filesystem.

The defaults can be changed without editing test code:

```sh
make test-nightly \
  NIGHTLY_FUZZ_TIME=15m \
  NIGHTLY_DIFFERENTIAL_TIME=30m \
  SOAK_ITERATIONS=100
```

## Benchmarks

Do not use `go test ./... -bench=.` as a general confidence command. The
repository contains 10-million-edge benchmarks, multi-million-row
compactions, and benchmarks that require external c1z fixtures.

- `make bench-smoke` executes the bounded checkpoint curves once.
- `make bench` runs the repeatable checkpoint suite plus the medium
  end-to-end SQLite/Pebble sync benchmark.
- Specialized benchmark source files document their required fixture paths
  and scale variables.

Benchmark output is evidence, not a pass/fail regression gate. When comparing
branches, capture multiple runs and use `benchstat`.

## Production-scale experiments

Production-scale compactor experiments are deliberately excluded from
`test-extra` and `test-nightly`. They can consume hours and substantial disk:

- `make prodscale-check`
- `make prodscale-crossover`
- `make prodscale-topebble`

Their fixture directory and dimensions can be overridden with the
`BATON_PROD_SCALE_*`, `BATON_CROSSOVER_*`, and `BATON_TOPEBBLE_*` variables
documented in `pkg/synccompactor/prodscale*_test.go`.

## Fixture-driven diagnostics

Some tests are diagnostic tools rather than suites and therefore cannot be
part of an umbrella target:

- `BATON_PEBBLE_PATH` / `BATON_SQL_PATH` compare supplied expansion artifacts.
- `BATON_WHALE_PEBBLE_PATH` / `BATON_WHALE_OUT_PATH` expand a supplied whale
  fixture and retain the result.
- `BATON_MIGRATE_C1Z_PATH` migrates a supplied artifact.
- `BATON_INSPECT_*` variables drive low-level Pebble layout and hash
  inspection.

These remain close to the tests that define their input contract. They are
listed here so repository-wide confidence tooling is discoverable from one
place.
