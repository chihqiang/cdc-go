APP_VERSION ?= $(shell git describe --tags --always)
OUTPUT := cdc-go
MAIN := cmd/cdc-go/main.go
TASK_OUTPUT := cdc-go-task
TASK_MAIN := cmd/cdc-go-task/main.go
# make build APP_VERSION=v1.0.0
build:
	@echo "Building $(OUTPUT) with version $(APP_VERSION) ..."
	GO111MODULE=on CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=$(APP_VERSION)" -o $(OUTPUT) $(MAIN)
	@echo "Build complete: $(OUTPUT)"

# make build-task APP_VERSION=v1.0.0
build-task:
	@echo "Building $(TASK_OUTPUT) with version $(APP_VERSION) ..."
	GO111MODULE=on CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=$(APP_VERSION)" -o $(TASK_OUTPUT) $(TASK_MAIN)
	@echo "Build complete: $(TASK_OUTPUT)"

# make build-all APP_VERSION=v1.0.0
build-all: build build-task

# `go install` writes to GOBIN (GOPATH/bin when GOBIN is unset), which is often
# missing from PATH, so each tool is resolved to an absolute path before it runs.
check:
	@echo "Running linters..."
	@bin="$$(go env GOBIN)"; [ -n "$$bin" ] || bin="$$(go env GOPATH)/bin"; \
	lint="$$(command -v golangci-lint || echo "$$bin/golangci-lint")"; \
	if [ ! -x "$$lint" ]; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.3.1; \
	else \
		echo "golangci-lint already installed, skipping..."; \
	fi; \
	"$$lint" run -D staticcheck ./...
	@echo "Linting passed"

	@bin="$$(go env GOBIN)"; [ -n "$$bin" ] || bin="$$(go env GOPATH)/bin"; \
	check="$$(command -v errcheck || echo "$$bin/errcheck")"; \
	if [ ! -x "$$check" ]; then \
		echo "Installing errcheck..."; \
		go install github.com/kisielk/errcheck@v1.20.0; \
	else \
		echo "errcheck already installed, skipping..."; \
	fi; \
	"$$check" ./...
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