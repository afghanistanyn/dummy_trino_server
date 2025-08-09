BINARY_NAME := dummy_trino_server
.PHONY: all build run test benchmark lint fmt vet clean coverage mod help

all: build

build:
	go build -o $(BINARY_NAME) .

build_gen:
	cd gen && go build -o ../ gen_data.go

run: build
	./$(BINARY_NAME)

test:
	go test -v ./

benchmark:
	go test -bench=BenchmarkTrinoServer -benchmem

fmt:
	go fmt ./...

vet:
	go vet ./...

mod:
	go mod tidy
	go mod vendor

help:
	@echo "Common targets:"
	@echo "  build      Build the binary"
	@echo "  run        Build and run the binary"
	@echo "  test       Run all tests"
	@echo "  benchmark  Run benchmarks"
	@echo "  fmt        Format code"
	@echo "  vet        Run go vet"
	@echo "  mod        Run go mod tidy and go mod vendor"
