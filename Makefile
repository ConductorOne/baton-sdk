VERSION := $(shell git describe --tags)

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

.PHONY: build
build:
	go build -tags=protoopaque ./...

.PHONY: test
test:
	go test -v -tags=protoopaque ./...

.PHONY: pkg/sdk/version.go
pkg/sdk/version.go:
	echo $(VERSION)
	echo "package sdk\n\nconst Version = \"$(VERSION)\"" > $@
