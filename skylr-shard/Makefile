LOCAL_BIN := $(CURDIR)/bin
GO_BIN := $(shell go env GOPATH)/bin

GOLANGCI_BIN := $(LOCAL_BIN)/golangci-lint
GOLANGCI_TAG ?= v2.6.2

PROTOC_GEN_GO_GRPC_BIN := $(LOCAL_BIN)/protoc-gen-go-grpc
PROTOC_GEN_GO_GRPC_TAG ?= latest

PROTOC_GEN_GO_BIN := $(GO_BIN)/protoc-gen-go

PROTOC_SRC := $(CURDIR)/api
PROTOC_DST := $(CURDIR)/pkg/pb

.PHONY: run
run:
	go run cmd/main.go

.PHONY: all
all: test build lint

.PHONY: test
test: 
	go test -v ./...

.PHONY: build
build:
	go build -v ./...

.PHONY: pbgen
pbgen:
	protoc \
		-I=$(PROTOC_SRC) \
		--plugin=protoc-gen-go=$(PROTOC_GEN_GO_BIN) \
		--plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC_BIN) \
		--go_out=$(PROTOC_DST) \
		--go-grpc_out=$(PROTOC_DST) \
		$(PROTOC_SRC)/skylr-shard.proto

.PHONY: deps
deps: install-lint install-grpc-go

.PHONY: lint
lint: .lint

.lint: install-lint
	$(info Running lint against all project files...)
	$(GOLANGCI_BIN) run \
		--config=.golangci.yaml \
		--max-issues-per-linter=1000 \
		--max-same-issues=1000 \
		./...

install-lint: export GOBIN := $(LOCAL_BIN)
install-lint:
ifeq ($(wildcard $(GOLANGCI_BIN)),)
	$(info Installing golangci-lint v$(GOLANGCI_TAG))
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_TAG)
else
	$(info Golangci-lint is already installed to $(GOLANGCI_BIN))
endif

install-grpc-go: export GOBIN := $(LOCAL_BIN)
install-grpc-go:
ifeq ($(wildcard $(PROTOC_GEN_GO_GRPC_BIN)),)
	$(info Installing grpc-go v$(PROTOC_GEN_GO_GRPC_TAG))
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_TAG)
else
	$(info Golangci-lint is already installed to $(PROTOC_GEN_GO_GRPC_BIN))
endif


