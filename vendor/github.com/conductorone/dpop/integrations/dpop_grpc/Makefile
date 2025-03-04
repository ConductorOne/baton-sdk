.PHONY: generate
generate:
	buf generate

.PHONY: lint
lint:
	buf lint

.PHONY: test
test: generate
	go test -v ./... 