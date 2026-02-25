LOCAL_BIN := $(CURDIR)/bin
GO_BIN := $(shell go env GOPATH)/bin
GOLANGCI_TAG ?= v2.6.2
OVERSEER_BIN := $(abspath $(CURDIR)/../skylr-overseer/bin)
# Prefer: PATH, sibling skylr-overseer/bin, local bin, GOPATH/bin. Override with make GOLANGCI_BIN=/path/to/golangci-lint
GOLANGCI_BIN ?= $(firstword $(shell PATH="$(OVERSEER_BIN):$$PATH" which golangci-lint 2>/dev/null) $(shell which golangci-lint 2>/dev/null) $(LOCAL_BIN)/golangci-lint $(GO_BIN)/golangci-lint)
PROTOC_GEN_GO_BIN := $(GO_BIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC_BIN := $(GO_BIN)/protoc-gen-go-grpc

PROTOVENDOR_SRC := $(CURDIR)/vendor.proto
PROTOC_DST := $(CURDIR)/internal/pb

.PHONY: all
all: test build lint

.PHONY: test
test:
	go test -v ./...

.PHONY: build
build: pbgen
	go build -v ./...

.PHONY: build-bin
build-bin: pbgen
	mkdir -p $(LOCAL_BIN)
	go build -o $(LOCAL_BIN)/skylr-client -v ./cmd/skylr-client

.PHONY: pbgen
pbgen: install-protoc-plugins
	mkdir -p $(PROTOC_DST)
	protoc \
		-I=$(PROTOVENDOR_SRC) \
		--plugin=protoc-gen-go=$(PROTOC_GEN_GO_BIN) \
		--plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC_BIN) \
		--go_out=$(PROTOC_DST) \
		--go-grpc_out=$(PROTOC_DST) \
		$(PROTOVENDOR_SRC)/skylr-overseer/skylr-overseer.proto \
		$(PROTOVENDOR_SRC)/skylr-shard/skylr-shard.proto

.PHONY: lint
lint: install-lint
	$(GOLANGCI_BIN) run --config=.golangci.yaml ./...

.PHONY: install-lint
install-lint: export GOBIN := $(LOCAL_BIN)
install-lint:
	@if ! test -x $(GOLANGCI_BIN) 2>/dev/null; then \
		echo "Installing golangci-lint to $(LOCAL_BIN)..."; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_TAG); \
	fi

.PHONY: install-protoc-plugins
install-protoc-plugins:
	@test -x $(PROTOC_GEN_GO_BIN) || { echo "run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"; exit 1; }
	@test -x $(PROTOC_GEN_GO_GRPC_BIN) || { echo "run: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"; exit 1; }
