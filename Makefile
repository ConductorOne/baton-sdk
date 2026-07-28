VERSION := $(shell git describe --tags)
GOOS = $(shell go env GOOS)
GOARCH = $(shell go env GOARCH)
BUILD_DIR = dist/${GOOS}_${GOARCH}
OUTPUT_PATH = ${BUILD_DIR}/baton

# Durations and iteration counts used by the opt-in confidence suites.
# Override at invocation time, for example: make fuzz-smoke FUZZ_TIME=2m.
FUZZ_TIME ?= 30s
DIFFERENTIAL_TIME ?= 30s
SOAK_ITERATIONS ?= 25
NIGHTLY_FUZZ_TIME ?= 5m
NIGHTLY_DIFFERENTIAL_TIME ?= 10m

.DEFAULT_GOAL := help

.PHONY: help
help: ## List build, test, and confidence targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: frontend ## Build the frontend and baton binary.
	rm -f ${OUTPUT_PATH}
	mkdir -p ${BUILD_DIR}
	go build -o ${OUTPUT_PATH} ./cmd/baton

.PHONY: frontend
frontend: ## Build the embedded React frontend.
	cd frontend && npm install && npm run build

.PHONY: lint
lint: ## Run golangci-lint.
	golangci-lint run --timeout=3m

.PHONY: update-deps
update-deps: ## Update and tidy Go dependencies.
	go get -d -u ./...
	go mod tidy -v

.PHONY: add-deps
add-dep: ## Tidy Go dependencies after adding one.
	go mod tidy -v

.PHONY: protogen
protogen: ## Generate protobuf code.
	buf generate

.PHONY: protofmt
protofmt: ## Format protobuf definitions.
	buf format -w

.PHONY: test
test: ## Run the Go test suite used by CI.
	go test -tags=baton_lambda_support -v ./...

# Two-artifact checkpoint compatibility matrix: builds the harness against
# HEAD and a pinned past release, and exchanges mid-flight checkpoints in
# both directions. See cmd/baton-compat-harness. Override the old release
# with BATON_COMPAT_OLD_REF=<tag>.
.PHONY: compat-check
compat-check: ## Exchange checkpoints with a pinned older SDK.
	BATON_COMPAT=1 go test -v -count=1 -run TestCheckpointCompatAcrossSDKVersions ./cmd/baton-compat-harness

# Real-binary interruption instrument: builds a deterministic connector from
# this tree, runs budget-bounded sync sessions, SIGKILLs them at varied
# offsets, and requires resumed syncs to seal a store content-identical to an
# uninterrupted baseline. See cmd/baton-crash-harness.
.PHONY: crash-check
crash-check: ## Exercise cross-process checkpoint/resume under hard kills.
	BATON_DEMO_CRASH=1 go test -v -count=1 -timeout=30m -run TestCrashResumeRealConnector ./cmd/baton-crash-harness

.PHONY: demo-crash-check
demo-crash-check: crash-check ## Deprecated alias for crash-check.

.PHONY: checkpoint-cut-check
checkpoint-cut-check: ## Resume from every durable checkpoint cut.
	BATON_CUT_SWEEP=full go test -v -count=1 -timeout=30m -run TestCheckpointCutEnumeration ./pkg/sync

.PHONY: interrupt-check
interrupt-check: checkpoint-cut-check crash-check ## Run in-process cut and real-process interruption checks.

.PHONY: race-check
race-check: ## Run the complete Go suite with the race detector.
	go test -race -tags=baton_lambda_support -count=1 -timeout=30m ./...

.PHONY: fuzz-smoke
fuzz-smoke: ## Run each native Go fuzzer for FUZZ_TIME (default 30s).
	go test -run '^$$' -fuzz '^FuzzCondenseFWBW_Cancellation$$' -fuzztime=$(FUZZ_TIME) ./pkg/sync/expand/scc
	go test -run '^$$' -fuzz '^FuzzCondenseFWBW_FromBytes$$' -fuzztime=$(FUZZ_TIME) ./pkg/sync/expand/scc

.PHONY: differential-check
differential-check: ## Differential-fuzz SQLite and Pebble for DIFFERENTIAL_TIME.
	BATON_EXPAND_FUZZ_DURATION=$(DIFFERENTIAL_TIME) go test -v -count=1 -timeout=30m -run '^TestFullPipelineDifferentialFuzz$$' ./pkg/sync/expand

.PHONY: bench-smoke
bench-smoke: ## Run the bounded checkpoint cost benchmarks once.
	go test -run '^$$' -bench 'Benchmark(CheckpointToken|SpawnedCursorAdmission)' -benchtime=1x -benchmem ./pkg/sync

.PHONY: bench
bench: ## Run curated checkpoint and medium full-sync benchmarks.
	go test -run '^$$' -bench 'Benchmark(CheckpointToken|SpawnedCursorAdmission)' -benchmem ./pkg/sync
	SYNC_BENCH_SCALE=medium go test -tags=baton_lambda_support,batonsdkv2 -run '^$$' -bench '^BenchmarkFullSync_BatonDemoShape$$' -benchtime=1x -benchmem ./pkg/sync

.PHONY: scheduler-soak
scheduler-soak: ## Run randomized scheduler cases under race detection.
	BATON_SOAK_ITERATIONS=$(SOAK_ITERATIONS) go test -race -v -count=1 -timeout=30m -run TestSchedulerSoakRandomizedFanoutWithFailures ./pkg/sync

.PHONY: errorfs-soak
errorfs-soak: ## Sweep whole-sync Pebble crash points using errorfs.
	BATON_SOAK=1 go test -v -count=1 -timeout=30m -run TestErrorFSWholeSyncRandomSweepSoak ./pkg/dotc1z/engine/pebble

.PHONY: test-extra
test-extra: race-check compat-check interrupt-check fuzz-smoke differential-check bench-smoke ## Run bounded confidence checks omitted from CI.

.PHONY: test-nightly
test-nightly: ## Run extended confidence, fuzz, scheduler, and errorfs checks.
	$(MAKE) test-extra FUZZ_TIME=$(NIGHTLY_FUZZ_TIME) DIFFERENTIAL_TIME=$(NIGHTLY_DIFFERENTIAL_TIME)
	$(MAKE) scheduler-soak
	$(MAKE) errorfs-soak

# Production-scale compactor experiments are deliberately excluded from
# test-extra and test-nightly: they create multi-million-row fixtures and may
# consume hours and substantial disk. Their BATON_* sizing variables remain
# available as documented in docs/TESTING.md and the test files.
.PHONY: prodscale-check
prodscale-check: ## Run the multi-million-row compactor experiment.
	BATON_PROD_SCALE_TEST=1 go test -v -count=1 -timeout=60m -run 'TestProdScale' ./pkg/synccompactor

.PHONY: prodscale-crossover
prodscale-crossover: ## Measure fold/overlay crossover at production scale.
	BATON_PROD_SCALE_CROSSOVER=1 go test -v -count=1 -timeout=60m -run TestProdScaleFoldOverlayCrossover ./pkg/synccompactor

.PHONY: prodscale-topebble
prodscale-topebble: ## Measure SQLite-to-Pebble conversion at production scale.
	BATON_PROD_SCALE_TOPEBBLE=1 go test -v -count=1 -timeout=180m -run TestProdScaleToPebbleCurve ./pkg/synccompactor

.PHONY: pkg/sdk/version.go
pkg/sdk/version.go:
	echo $(VERSION)
	echo "package sdk\n\nconst Version = \"$(VERSION)\"" > $@
