.PHONY: lint
lint:
	golangci-lint run --timeout=3m

.PHONY: update-deps
update-deps:
	GOPRIVATE=github.com/conductorone/baton-sdk go get -d -u ./...
	go mod tidy -v

.PHONY: add-deps
add-dep:
	go mod tidy -v

.PHONY: protogen
protogen:
	buf generate

.PHONY: test
test:
	go test -v ./...