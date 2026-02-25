LOCAL_BIN := $(CURDIR)/bin
GO_BIN := $(shell go env GOPATH)/bin
PROTOC_GEN_GO_BIN := $(GO_BIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC_BIN := $(GO_BIN)/protoc-gen-go-grpc

PROTOVENDOR_SRC := $(CURDIR)/vendor.proto
PROTOC_DST := $(CURDIR)/internal/pb

.PHONY: all
all: test build

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

.PHONY: install-protoc-plugins
install-protoc-plugins:
	@test -x $(PROTOC_GEN_GO_BIN) || { echo "run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"; exit 1; }
	@test -x $(PROTOC_GEN_GO_GRPC_BIN) || { echo "run: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"; exit 1; }
