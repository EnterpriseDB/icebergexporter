.PHONY: build test lint fmt vet tidy up down clean

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
	gofmt -w -s .

vet:
	go vet ./...

tidy:
	go mod tidy

up:
	docker compose -f example/docker-compose.yaml up --build -d

down:
	docker compose -f example/docker-compose.yaml down -v

clean:
	rm -rf dist/ build/ otelcol-iceberg
