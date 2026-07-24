VERSION := $(shell git describe --tags)
GOOS = $(shell go env GOOS)
GOARCH = $(shell go env GOARCH)
BUILD_DIR = dist/${GOOS}_${GOARCH}
OUTPUT_PATH = ${BUILD_DIR}/baton

.PHONY: build
build: frontend
	rm -f ${OUTPUT_PATH}
	mkdir -p ${BUILD_DIR}
	go build -o ${OUTPUT_PATH} ./cmd/baton

.PHONY: frontend
frontend:
	cd frontend && npm install && npm run build

.PHONY: lint
lint:
	golangci-lint run --timeout=3m

.PHONY: update-deps
update-deps:
	go get -d -u ./...
	go mod tidy -v

.PHONY: add-deps
add-dep:
	go mod tidy -v

.PHONY: protogen
protogen:
	buf generate

.PHONY: protofmt
protofmt:
	buf format -w

.PHONY: test
test:
	go test -tags=baton_lambda_support -v ./...

# Two-artifact checkpoint compatibility matrix: builds the harness against
# HEAD and a pinned past release, and exchanges mid-flight checkpoints in
# both directions. See cmd/baton-compat-harness. Override the old release
# with BATON_COMPAT_OLD_REF=<tag>.
.PHONY: compat-check
compat-check:
	BATON_COMPAT=1 go test -v -count=1 -run TestCheckpointCompatAcrossSDKVersions ./cmd/baton-compat-harness

.PHONY: pkg/sdk/version.go
pkg/sdk/version.go:
	echo $(VERSION)
	echo "package sdk\n\nconst Version = \"$(VERSION)\"" > $@
