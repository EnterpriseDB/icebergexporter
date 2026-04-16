.PHONY: build test test-integration lint fmt vet tidy check cover install-hooks up down clean

build:
	go build ./...

test:
	go test -race -count=1 ./...

test-integration:
	go test -race -count=1 -tags=integration ./...

lint: vet
	@which golangci-lint > /dev/null 2>&1 || { echo "golangci-lint not installed"; exit 1; }
	golangci-lint run ./...

fmt:
	go run golang.org/x/tools/cmd/goimports@latest -w -local github.com/enterprisedb/icebergexporter .

vet:
	go vet ./...

tidy:
	go mod tidy

check:
	@which pre-commit > /dev/null 2>&1 || { echo "pre-commit not installed: brew install pre-commit"; exit 1; }
	pre-commit run --all-files --hook-stage pre-push

cover:
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

install-hooks:
	@which pre-commit > /dev/null 2>&1 || { echo "pre-commit not installed: brew install pre-commit"; exit 1; }
	pre-commit install --hook-type pre-commit --hook-type pre-push

up:
	docker compose -f example/docker-compose.yaml up --build -d

down:
	docker compose -f example/docker-compose.yaml down -v

clean:
	rm -rf dist/ build/ otelcol-iceberg
