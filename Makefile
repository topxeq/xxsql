.PHONY: all build test clean install run

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Binary names
SERVER_NAME=xxsqls
CLIENT_NAME=xxsqlc

# Build directories
BUILD_DIR=bin
CMD_SERVER=./cmd/xxsqls
CMD_CLIENT=./cmd/xxsqlc

all: test build

build: build-server build-client

build-server:
	@echo "Building server..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(SERVER_NAME) $(CMD_SERVER)

build-client:
	@echo "Building client..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(CLIENT_NAME) $(CMD_CLIENT)

test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. ./...

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

install-deps:
	$(GOMOD) download

lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	golangci-lint run ./...

fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

run: build-server
	@echo "Starting server..."
	./$(BUILD_DIR)/$(SERVER_NAME) -config configs/xxsql.json.example

# Cross-compilation
build-linux:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(SERVER_NAME)-linux-amd64 $(CMD_SERVER)

build-darwin:
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(SERVER_NAME)-darwin-amd64 $(CMD_SERVER)

build-windows:
	GOOS=windows GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(SERVER_NAME)-windows-amd64.exe $(CMD_SERVER)

build-all: build-linux build-darwin build-windows
