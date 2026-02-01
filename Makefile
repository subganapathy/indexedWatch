# IndexedWatch Makefile

.PHONY: all proto proto-lint proto-breaking clean test build help

# Default target
all: proto build

# ============================================================================
# Proto targets
# ============================================================================

# Generate Go code from proto files
proto:
	@echo "Generating protobuf code..."
	@buf generate
	@echo "Done."

# Lint proto files
proto-lint:
	@echo "Linting proto files..."
	@buf lint
	@echo "Done."

# Check for breaking changes (against git main branch)
proto-breaking:
	@echo "Checking for breaking changes..."
	@buf breaking --against '.git#branch=main'
	@echo "Done."

# Format proto files
proto-format:
	@echo "Formatting proto files..."
	@buf format -w
	@echo "Done."

# ============================================================================
# Go targets
# ============================================================================

# Build the binary
build:
	@echo "Building..."
	@go build -o bin/indexedwatch ./cmd/indexedwatch
	@echo "Done."

# Run tests
test:
	@echo "Running tests..."
	@go test -v -race ./...
	@echo "Done."

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -v -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run linter
lint:
	@echo "Running linter..."
	@golangci-lint run ./...
	@echo "Done."

# Format Go code
fmt:
	@echo "Formatting Go code..."
	@gofmt -s -w .
	@echo "Done."

# Tidy dependencies
tidy:
	@go mod tidy

# ============================================================================
# Development targets
# ============================================================================

# Run the server (development)
run:
	@go run ./cmd/indexedwatch

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -rf gen/
	@rm -f coverage.out coverage.html
	@echo "Done."

# ============================================================================
# Dependencies
# ============================================================================

# Install development tools
tools:
	@echo "Installing development tools..."
	@go install github.com/bufbuild/buf/cmd/buf@latest
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "Done."

# ============================================================================
# Help
# ============================================================================

help:
	@echo "IndexedWatch Makefile"
	@echo ""
	@echo "Proto targets:"
	@echo "  proto           - Generate Go code from proto files"
	@echo "  proto-lint      - Lint proto files"
	@echo "  proto-breaking  - Check for breaking changes"
	@echo "  proto-format    - Format proto files"
	@echo ""
	@echo "Go targets:"
	@echo "  build           - Build the binary"
	@echo "  test            - Run tests"
	@echo "  test-coverage   - Run tests with coverage"
	@echo "  lint            - Run linter"
	@echo "  fmt             - Format Go code"
	@echo "  tidy            - Tidy dependencies"
	@echo ""
	@echo "Development targets:"
	@echo "  run             - Run the server"
	@echo "  clean           - Clean build artifacts"
	@echo "  tools           - Install development tools"
	@echo ""
	@echo "Default: all (proto + build)"
