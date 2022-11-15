.PHONY: update-deps
update-deps:
	GOPRIVATE=github.com/ductone/connector-sdk go get -d -u ./...
	go mod tidy -v
	go mod vendor

.PHONY: add-deps
add-dep:
	go mod tidy -v
	go mod vendor

.PHONY: lint
lint:
	golangci-lint run --timeout=3m

.PHONY: proto-gen
proto-gen:
	buf generate

.PHONY: test
test:
	go test -v ./...

GOOS = $(shell go env GOOS)
GOARCH = $(shell go env GOARCH)
BUILD_DIR = build/${GOOS}_${GOARCH}
.PHONY: build-c1z
build-c1z:
	rm -f ${OUTPUT_PATH}
	mkdir -p ${BUILD_DIR}
	go build -o ${BUILD_DIR}/c1z cmd/c1z/*.go
