CDC_TRIGGER_VERSION ?= $(shell git describe --tags --always)
OUTPUT := cdc-trigger
MAIN := cmd/cdc-trigger/main.go
# make build CDC_TRIGGER_VERSION=v1.0.0
build:
	@echo "Building $(OUTPUT) with version $(CDC_TRIGGER_VERSION) ..."
	GO111MODULE=on CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=$(CDC_TRIGGER_VERSION)" -o $(OUTPUT) $(MAIN)
	@echo "Build complete: $(OUTPUT)"

check:
	@echo "Running linters..."
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.3.1; \
	else \
		echo "golangci-lint already installed, skipping..."; \
	fi
	golangci-lint run ./...
	@echo "Linting passed"

	@if ! command -v errcheck >/dev/null 2>&1; then \
		echo "Installing errcheck..."; \
		go install github.com/kisielk/errcheck@latest; \
	else \
		echo "errcheck already installed, skipping..."; \
	fi
	errcheck ./...
	@echo "Error checks passed"

	@find . -name "*.go" -exec go fmt {} \;
	@go mod tidy

test:
	@echo "Running tests..."
	go test -v -coverpkg=./... -race -covermode=atomic -coverprofile=coverage.txt ./... -run . -timeout=2m
	@echo "Checking git status..."
	@git diff --quiet || (echo "❌ Uncommitted changes detected in working directory!" && git status && exit 1)
	@git diff --cached --quiet || (echo "❌ Staged but uncommitted changes detected!" && git status && exit 1)
	@echo "Git status clean"