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
CHAOS_ITERATIONS ?= 25
NIGHTLY_FUZZ_TIME ?= 5m
NIGHTLY_DIFFERENTIAL_TIME ?= 10m
INCREMENTAL_SOAK_TIME ?= 10m

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
#
# The timeout has to clear the sum of this test's own sub-budgets rather than
# just its expected runtime: a worktree checkout at 5m, two harness builds at 5m
# each, and eight gen/resume runs at 3m come to about 39m of ceilings. Below that
# sum — and Go's 10m default is far below it — a slow-but-healthy run dies as
# "test timed out" and says nothing about which step was slow, which is the whole
# failure mode this value exists to avoid. A genuine hang is caught by the step's
# own context within a few minutes, so a global ceiling this high costs nothing.
# nightly.yaml keeps the job budget above it, so Go reports before the runner.
#
# For scale: the whole target takes about a minute on a warm developer machine,
# where the four exchange cells are 2-3s each and the two builds are nearly all
# of it. The ceiling is a backstop for a cold runner compiling two dependency
# sets, not a number this is expected to approach.
.PHONY: compat-check
compat-check: ## Exchange checkpoints with a pinned older SDK.
	go test -count=1 -run 'Test(EntitlementGraphTokenCompatibilityMatrix|GraphFromStore)' ./pkg/sync
	go test -count=1 -run 'TestCompactorGraphCompatibilityHealing' ./pkg/synccompactor
	BATON_COMPAT=1 go test -v -count=1 -timeout=45m -run TestCheckpointCompatAcrossSDKVersions ./cmd/baton-compat-harness
	$(MAKE) graph-compat-check

.PHONY: graph-compat-check
graph-compat-check: ## Exchange graph-sidecar c1z artifacts with a pinned older SDK.
	BATON_GRAPH_COMPAT=1 go test -v -count=1 -timeout=30m -run 'Test(GraphReuseCompatAcrossSDKVersions|DefaultPathPerformanceAgainstPinnedMain)' ./cmd/baton-compat-harness

# Real-binary interruption instrument: builds a deterministic connector from
# this tree, runs budget-bounded sync sessions, SIGKILLs them at varied
# offsets, and requires resumed syncs to seal a store content-identical to an
# uninterrupted baseline. See cmd/baton-crash-harness.
.PHONY: crash-check
crash-check: ## Exercise cross-process checkpoint/resume under hard kills.
	BATON_DEMO_CRASH=1 go test -v -count=1 -timeout=30m -run TestCrashResumeRealConnector ./cmd/baton-crash-harness
	go test -v -count=1 -run 'TestIncrementalExpansion(ProcessKillRetry|CrashRetry)' ./pkg/synccompactor

.PHONY: demo-crash-check
demo-crash-check: crash-check ## Deprecated alias for crash-check.

.PHONY: checkpoint-cut-check
checkpoint-cut-check: ## Resume from every durable checkpoint cut.
	BATON_TEST_EXTRA=1 BATON_CUT_SWEEP=full go test -v -count=1 -timeout=30m -run TestCheckpointCutEnumeration ./pkg/sync

.PHONY: interrupt-check
interrupt-check: checkpoint-cut-check crash-check ## Run in-process cut and real-process interruption checks.

.PHONY: race-check
race-check: ## Run the complete Go suite with the race detector.
	BATON_TEST_EXTRA=1 go test -race -tags=baton_lambda_support -count=1 -timeout=45m ./...

# Nightly race shards. A serial instrumented ./... sweep is hours of work, and
# nearly all of it sits in a handful of packages, so the nightly workflow runs
# these shards as concurrent jobs instead. That is what keeps a "nightly" suite
# finishing overnight rather than bleeding into the next workday, and it names
# the shard that broke instead of one red check covering everything.
#
# `rest` is the complement of the named shards, so every package belongs to
# exactly one shard by construction: a newly added package joins `rest`
# automatically rather than silently escaping the sweep. race-shard-audit
# proves that property by counting.
RACE_SHARD_NAMES := dotc1z engine sync expand compactor cmd rest

RACE_PKG := github.com/conductorone/baton-sdk
RACE_SHARD_dotc1z := ^$(RACE_PKG)/pkg/dotc1z$$
RACE_SHARD_engine := ^$(RACE_PKG)/pkg/dotc1z/engine($$|/)
RACE_SHARD_sync := ^$(RACE_PKG)/pkg/sync$$
RACE_SHARD_expand := ^$(RACE_PKG)/pkg/sync/expand($$|/)
RACE_SHARD_compactor := ^$(RACE_PKG)/(pkg/synccompactor($$|/)|pkg/c1zsanitize$$)
RACE_SHARD_cmd := ^$(RACE_PKG)/cmd($$|/)
RACE_SHARD_NAMED := $(RACE_SHARD_dotc1z)|$(RACE_SHARD_engine)|$(RACE_SHARD_sync)|$(RACE_SHARD_expand)|$(RACE_SHARD_compactor)|$(RACE_SHARD_cmd)

# The shards run under RACE_TAGS, so they are enumerated under it too. A
# package whose files all sit behind that tag would otherwise be missing from
# every shard and from the audit's total at once, and the audit would call that
# full coverage — the exact gap it exists to catch.
RACE_TAGS := baton_lambda_support

# A hang is only diagnosable if Go is the one that kills it: the runner's
# timeout stops the job and keeps nothing, while Go's dumps every goroutine's
# stack. Go applies this timeout per test binary rather than per invocation, so
# for a one-package shard a job budget above this value is enough, while a shard
# spanning many packages can legitimately run far longer in total than any one
# binary is allowed. nightly.yaml gives those shards budgets well clear of this
# value instead of the small margin that would do if the clock were per shard.
#
# The value is generous on purpose: the one-package sync shard takes about 12
# minutes on a developer machine, and CI runners are smaller, so a tighter
# timeout would start killing slow-but-healthy packages. If a shard ever does
# trip this, the dump says which test was still running, and that is the number
# to raise.
RACE_SHARD_TIMEOUT := 45m

NIGHTLY_WORKFLOW := .github/workflows/nightly.yaml

.PHONY: race-shard-list
race-shard-list: ## Print the packages in race shard SHARD.
	@test -n "$(SHARD)" || { echo "race-shard-list: set SHARD to one of: $(RACE_SHARD_NAMES)" >&2; exit 2; }
	@if [ "$(SHARD)" = "rest" ]; then \
		go list -tags=$(RACE_TAGS) ./... | grep -vE '$(RACE_SHARD_NAMED)'; \
	else \
		test -n '$(RACE_SHARD_$(SHARD))' || { echo "race-shard-list: unknown shard '$(SHARD)'" >&2; exit 2; }; \
		go list -tags=$(RACE_TAGS) ./... | grep -E '$(RACE_SHARD_$(SHARD))'; \
	fi

# The package list is captured before the test runs so a bad SHARD fails as a
# bad SHARD. Left inside the argument list, its exit status is swallowed by
# command substitution and go test reports a confusing "no packages" instead.
# An empty list cannot get past the capture — the list ends in grep, which exits
# 1 when it matches nothing — so this reports it here rather than testing $$pkgs
# afterwards, which would never run.
#
# The shards are the nightly instrumented sweep, so they run both opt-in tiers.
# nightly.yaml invokes these directly rather than through test-nightly, so
# without the tier variables here the guarded tests would skip in every shard
# and the sweep would quietly cover less than the un-sharded race-check does.
# Extra is implied by nightly: a tier that runs the randomized and full-corpus
# cases has no reason to drop the deterministic long ones.
.PHONY: race-check-shard
race-check-shard: ## Race-check one nightly shard, for example SHARD=dotc1z.
	@pkgs=$$($(MAKE) --no-print-directory race-shard-list SHARD=$(SHARD)) || { \
		echo "race-check-shard: no packages for SHARD='$(SHARD)' (see above)" >&2; \
		exit 1; \
	}; \
	set -x; BATON_TEST_EXTRA=1 BATON_TEST_NIGHTLY=1 go test -race -tags=$(RACE_TAGS) -count=1 -timeout=$(RACE_SHARD_TIMEOUT) $$pkgs

# Each shard's list is captured rather than piped straight into wc, for the same
# reason as race-check-shard above: at the head of a pipe its exit status is
# lost. That is worse than a confusing message. A named shard's list ends in
# grep, which exits 1 when its pattern matches nothing, and a pattern that has
# drifted from the package tree takes nothing out of `rest` — so `rest` absorbs
# the orphaned packages, the union still equals ./..., and the count-based audit
# passes while a whole nightly job runs an empty package set. Failing on the
# list's status is what catches that, which is why it says which shard and why.
.PHONY: race-shard-audit
race-shard-audit: ## Verify the race shards cover every package exactly once.
	@total=$$(go list -tags=$(RACE_TAGS) ./... | wc -l | tr -d ' '); sum=0; \
	test "$$total" -gt 0 || { echo "race-shard-audit: go list returned no packages" >&2; exit 1; }; \
	for s in $(RACE_SHARD_NAMES); do \
		out=$$($(MAKE) --no-print-directory race-shard-list SHARD=$$s) || { \
			if [ "$$s" = rest ]; then \
				echo "race-shard-audit: the 'rest' complement is empty: the named shards now match every package" >&2; \
			else \
				echo "race-shard-audit: shard '$$s' listed no packages: RACE_SHARD_$$s is either undefined or no longer matches anything" >&2; \
			fi; \
			exit 1; \
		}; \
		n=$$(printf '%s' "$$out" | grep -c '^'); \
		printf '  %-10s %3s packages\n' "$$s" "$$n"; \
		sum=$$((sum + n)); \
	done; \
	printf '  %-10s %3s packages\n' "union" "$$sum"; \
	printf '  %-10s %3s packages\n' "go list" "$$total"; \
	if [ "$$sum" != "$$total" ]; then \
		echo "race-shard-audit: union is $$sum but ./... is $$total — the shards overlap or leave a gap" >&2; \
		exit 1; \
	fi; \
	$(MAKE) --no-print-directory race-shard-matrix-audit; \
	echo "race-shard-audit: every package is covered exactly once"

# Counting packages proves the shards cover the repository, not that anything
# runs them: nightly.yaml names the shards in its matrix. A shard added here
# and not there would take its packages out of `rest` and into a job that does
# not exist, and the count would still balance.
.PHONY: race-shard-matrix-audit
race-shard-matrix-audit: ## Verify nightly.yaml runs exactly the declared shards.
	@declared=$$(printf '%s\n' $(RACE_SHARD_NAMES) | sort); \
	matrix=$$(sed -n 's/^ *- shard: *\([a-z0-9_-]*\).*/\1/p' $(NIGHTLY_WORKFLOW) | sort); \
	test -n "$$matrix" || { echo "race-shard-matrix-audit: no shards found in $(NIGHTLY_WORKFLOW)" >&2; exit 1; }; \
	if [ "$$declared" != "$$matrix" ]; then \
		echo "race-shard-matrix-audit: RACE_SHARD_NAMES and $(NIGHTLY_WORKFLOW) disagree" >&2; \
		echo "  Makefile:  $$(echo $$declared | tr '\n' ' ')" >&2; \
		echo "  workflow:  $$(echo $$matrix | tr '\n' ' ')" >&2; \
		exit 1; \
	fi

.PHONY: fuzz-smoke
fuzz-smoke: ## Run each native Go fuzzer for FUZZ_TIME (default 30s).
	go test -run '^$$' -fuzz '^FuzzIncrementalVsFullExpansion$$' -fuzztime=$(FUZZ_TIME) ./pkg/sync/expand
	go test -run '^$$' -fuzz '^FuzzCondenseFWBW_Cancellation$$' -fuzztime=$(FUZZ_TIME) ./pkg/sync/expand/scc
	go test -run '^$$' -fuzz '^FuzzCondenseFWBW_FromBytes$$' -fuzztime=$(FUZZ_TIME) ./pkg/sync/expand/scc

.PHONY: differential-check
differential-check: ## Differential-fuzz SQLite and Pebble for DIFFERENTIAL_TIME.
	BATON_TEST_EXTRA=1 BATON_EXPAND_FUZZ_DURATION=$(DIFFERENTIAL_TIME) go test -v -count=1 -timeout=30m -run '^TestFullPipelineDifferentialFuzz$$' ./pkg/sync/expand

.PHONY: bench-smoke
bench-smoke: ## Run the bounded checkpoint cost benchmarks once.
	go test -run '^$$' -bench 'Benchmark(CheckpointToken|SpawnedCursorAdmission)' -benchtime=1x -benchmem ./pkg/sync

.PHONY: incremental-performance-check
incremental-performance-check: ## Enforce 100k-node incremental allocation/work gates.
	BATON_INCREMENTAL_PERF=1 go test -v -count=1 -timeout=30m -run '^TestIncrementalPerformanceGates$$' ./pkg/sync/expand

.PHONY: incremental-soak
incremental-soak: ## Run the incremental differential fuzzers for INCREMENTAL_SOAK_TIME.
	BATON_EXPAND_FUZZ_DURATION=$(INCREMENTAL_SOAK_TIME) go test -count=1 -timeout=30m -run '^TestFullPipelineDifferentialFuzz$$' ./pkg/sync/expand
	go test -run '^$$' -fuzz '^FuzzIncrementalVsFullExpansion$$' -fuzztime=$(INCREMENTAL_SOAK_TIME) ./pkg/sync/expand

.PHONY: bench
bench: ## Run curated checkpoint and medium full-sync benchmarks.
	go test -run '^$$' -bench 'Benchmark(CheckpointToken|SpawnedCursorAdmission)' -benchmem ./pkg/sync
	SYNC_BENCH_SCALE=medium go test -tags=baton_lambda_support,batonsdkv2 -run '^$$' -bench '^BenchmarkFullSync_BatonDemoShape$$' -benchtime=1x -benchmem ./pkg/sync

.PHONY: scheduler-soak
scheduler-soak: ## Run randomized scheduler cases under race detection.
	BATON_TEST_NIGHTLY=1 BATON_SOAK_ITERATIONS=$(SOAK_ITERATIONS) go test -race -v -count=1 -timeout=30m -run TestSchedulerSoakRandomizedFanoutWithFailures ./pkg/sync

.PHONY: errorfs-soak
errorfs-soak: ## Sweep whole-sync Pebble crash points using errorfs.
	BATON_SOAK=1 go test -v -count=1 -timeout=30m -run TestErrorFSWholeSyncRandomSweepSoak ./pkg/dotc1z/engine/pebble

.PHONY: chaos-check
chaos-check: ## Run bounded representative chaos checks under race detection.
	go test -race -count=1 ./internal/chaosconnector/...
	go test -race -count=1 -timeout=10m -run '^TestChaosConnector(LostResponseThenFilesystemFailureResumes|ResourcesAndEntitlementsFaultMatrix|ListGrantsFaultMatrix|ReservedBatonIDOwnershipIsRejected|MalformedKnownAnnotationFailsWithoutSealing|ClearedNextPageTokenSealsOnlyVisiblePrefix|CancellationTerminatesAndColdResumes|DataPolicyLifecycleCorpus|ExternalPrincipalResumeUsesCurrentExternalAnswer|SQLiteExternalPrincipalResumeDegradesWithoutFailure|ExternalPrincipalCleanupUsesOnePassPerKeyspace)$$' ./pkg/sync
	go test -race -count=1 -timeout=10m -run '^TestChaosSourceCache(GateMatrix|CollectionSemantics|InterruptResume|GenerationalSteadyState|CompatDriftOnResume|ReplayWithoutHitFailsCold|DriftedResumeRejectsRestoredReplay|DuplicateReplayCursorsParallel|UnsupportedShapesBlockReplaySeed)$$' ./pkg/sync

.PHONY: chaos-full-check
chaos-full-check: ## Run every deterministic chaos corpus under race detection.
	BATON_TEST_NIGHTLY=1 go test -race -count=1 -timeout=30m -run '^TestChaos(Connector|SourceCache)' ./pkg/sync

.PHONY: chaos-soak
chaos-soak: ## Run extended seeded chaos connector fanout schedules.
	BATON_CHAOS_ITERATIONS=$(CHAOS_ITERATIONS) go test -race -v -count=1 -timeout=30m -run TestChaosConnectorSeededFanoutWithRetries ./pkg/sync

# Full deterministic chaos corpora are reserved for test-nightly.
.PHONY: test-extra
test-extra: export BATON_TEST_EXTRA=1
test-extra: race-check compat-check interrupt-check fuzz-smoke differential-check bench-smoke ## Run bounded confidence checks omitted from CI.

.PHONY: test-nightly
test-nightly: export BATON_TEST_NIGHTLY=1
test-nightly: ## Run extended confidence, fuzz, scheduler, and errorfs checks.
	$(MAKE) test-extra FUZZ_TIME=$(NIGHTLY_FUZZ_TIME) DIFFERENTIAL_TIME=$(NIGHTLY_DIFFERENTIAL_TIME)
	$(MAKE) scheduler-soak
	$(MAKE) chaos-soak
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

# Formal verification track (formal/). These targets have tool
# prerequisites this repo deliberately does not install: the P checker
# (`p` on PATH — https://p-org.github.io/P/) for the model sweeps, and a
# sibling engine checkout at ../occult (the host go.mod `replace` target)
# plus a Go 1.26 toolchain for the Occult suite. Each target checks its
# prerequisite and says what is missing rather than failing confusingly.
#
# The sweep targets REGENERATE the committed evidence summaries
# (formal/*/PCheckerOutput/*/summary.txt): a clean run reproduces them
# byte-for-byte apart from find-rate noise on RED cells, and a mismatch
# line or changed cell count is a calibration drift finding, not noise.
# The scripts' exit status carries that verdict — any mismatch (drifted
# cell, untagged red, wrong bake-off alarm, checker error) fails the
# target. Walker (55 cells) and graph (66 cells) each take on the order
# of half an hour at the default schedule budget; the bake-off (12
# cells) about twenty minutes.
P_SCHEDULES ?= 10000
OCCULT_TEST_TIMEOUT ?= 90m

.PHONY: p-checker-guard
p-checker-guard:
	@command -v p >/dev/null 2>&1 || { \
		echo "formal: the P checker ('p') is not on PATH — install it per https://p-org.github.io/P/ (no install target is provided)" >&2; \
		exit 2; \
	}

.PHONY: formal-walker-sweep
formal-walker-sweep: p-checker-guard ## Compile and sweep the walker model (55 cells; needs P).
	cd formal/walker && p compile -pp walker.pproj && tools/sweep.sh $(P_SCHEDULES)

.PHONY: formal-graph-sweep
formal-graph-sweep: p-checker-guard ## Compile and sweep the graph model (66 cells; needs P).
	cd formal/graph && p compile -pp graph.pproj && tools/sweep.sh $(P_SCHEDULES)

.PHONY: formal-graph-bakeoff
formal-graph-bakeoff: p-checker-guard ## Run the 12-cell bake-off phase (needs P).
	cd formal/graph && p compile -pp graph.pproj && tools/bakeoff.sh $(P_SCHEDULES)

.PHONY: formal-occult-check
formal-occult-check: ## Run the Occult host suite (needs ../occult and Go 1.26).
	@test -d ../occult || { \
		echo "formal-occult-check: sibling engine checkout not found at ../occult (the formal/occult/host go.mod 'replace' target); clone it beside this repo (no install target is provided)" >&2; \
		exit 2; \
	}
	cd formal/occult/host && go test -timeout $(OCCULT_TEST_TIMEOUT) ./...

.PHONY: formal-check
formal-check: formal-walker-sweep formal-graph-sweep formal-graph-bakeoff formal-occult-check ## Run every formal-track sweep and suite.

.PHONY: pkg/sdk/version.go
pkg/sdk/version.go:
	echo $(VERSION)
	echo "package sdk\n\nconst Version = \"$(VERSION)\"" > $@
