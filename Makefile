.PHONY: build test
build:
	go build ./cmd/sic
test:
	go test ./...
