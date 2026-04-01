LOCAL_BIN := $(CURDIR)/bin
GO_BIN := $(shell go env GOPATH)/bin

GOLANGCI_BIN := $(LOCAL_BIN)/golangci-lint
GOLANGCI_TAG ?= v2.6.2

PROTOC_GEN_GO_GRPC_BIN := $(LOCAL_BIN)/protoc-gen-go-grpc
PROTOC_GEN_GO_GRPC_TAG ?= latest

PROTOC_GEN_GO_BIN := $(LOCAL_BIN)/protoc-gen-go
PROTOC_GEN_GO_TAG ?= latest

PROTOC_GEN_GRPC_GATEWAY_BIN ?= $(LOCAL_BIN)/protoc-gen-grpc-gateway
PROTOC_GEN_GRPC_GATEWAY_TAG ?= latest

PROTOC_GEN_OPENAPI_BIN ?= $(LOCAL_BIN)/protoc-gen-openapiv2
PROTOC_GEN_OPENAPI_TAG ?= latest

PROTOC_SRC := $(CURDIR)/api
PROTOVENDOR_SRC := $(CURDIR)/vendor.proto
PROTOC_DST := $(CURDIR)/internal/pb
OPENAPI_OUT := $(CURDIR)/internal/pb/skylr-overseer

.PHONY: run
run: CONFIG ?= config/config.yaml
run:
	go run cmd/main.go --config=$(CONFIG)

KIND_CLUSTER ?= skylr

.PHONY: kind-setup
kind-setup:
	kind get clusters | grep -q $(KIND_CLUSTER) || kind create cluster --name $(KIND_CLUSTER)
	cd ../skylr-shard && docker build -t skylr-shard:latest .
	docker save skylr-shard:latest | docker exec -i $(KIND_CLUSTER)-control-plane ctr -n k8s.io images import -
	kubectl --context kind-$(KIND_CLUSTER) apply -f ../deploy/k8s-local/rbac.yaml
	$(MAKE) kind-route

.PHONY: kind-route
kind-route:
	@POD_CIDR=$$(kubectl --context kind-$(KIND_CLUSTER) get node $(KIND_CLUSTER)-control-plane -o jsonpath='{.spec.podCIDR}') && \
	 NODE_IP=$$(docker inspect $(KIND_CLUSTER)-control-plane --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}') && \
	 (ip route show | grep -q "$$POD_CIDR" && echo "route $$POD_CIDR already present" || (echo "adding route $$POD_CIDR via $$NODE_IP" && sudo ip route add "$$POD_CIDR" via "$$NODE_IP"))

.PHONY: run-k8s-local
run-k8s-local: kind-route
	go run cmd/main.go --config=config/config.k8s-local.yaml

.PHONY: all
all: test build lint

.PHONY: test
test: 
	go test -v ./...

.PHONY: build
build:
	go build -v ./...

.PHONY: format
format:
	smartimports -local "gitlab.ozon.ru/ob/marx/"

.PHONY: pbgen
pbgen:
	protoc \
		-I=$(PROTOC_SRC) \
		-I=$(PROTOVENDOR_SRC) \
		--plugin=protoc-gen-go=$(PROTOC_GEN_GO_BIN) \
		--plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC_BIN) \
		--plugin=protoc-gen-grpc-gateway=$(PROTOC_GEN_GRPC_GATEWAY_BIN) \
		--plugin=protoc-gen-openapiv2=$(PROTOC_GEN_OPENAPI_BIN) \
		--go_out=$(PROTOC_DST) \
		--go-grpc_out=$(PROTOC_DST) \
		--grpc-gateway_out=$(PROTOC_DST) \
		--grpc-gateway_opt generate_unbound_methods=true \
		--openapiv2_out=$(OPENAPI_OUT) \
		$(PROTOC_SRC)/skylr-overseer.proto

.PHONY: pbgen-shard
pbgen-shard:
	protoc \
		-I=$(PROTOC_SRC) \
		-I=$(PROTOVENDOR_SRC) \
		--plugin=protoc-gen-go=$(PROTOC_GEN_GO_BIN) \
		--plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC_BIN) \
		--plugin=protoc-gen-grpc-gateway=$(PROTOC_GEN_GRPC_GATEWAY_BIN) \
		--plugin=protoc-gen-openapiv2=$(PROTOC_GEN_OPENAPI_BIN) \
		--go_out=$(PROTOC_DST) \
		--go-grpc_out=$(PROTOC_DST) \
		--grpc-gateway_out=$(PROTOC_DST) \
		--grpc-gateway_opt generate_unbound_methods=true \
		--openapiv2_out=$(PROTOC_DST) \
		$(PROTOVENDOR_SRC)/skylr-shard/skylr-shard.proto

.PHONY: lint
lint: .lint

.lint: install-lint
	$(info Running lint against all project files...)
	$(GOLANGCI_BIN) run \
		--config=.golangci.yaml \
		./...

.PHONY: deps
deps: install-lint install-protoc install-protoc-grpc install-grpc-gateway install-smartimports

install-lint: export GOBIN := $(LOCAL_BIN)
install-lint:
ifeq ($(wildcard $(GOLANGCI_BIN)),)
	$(info Installing golangci-lint v$(GOLANGCI_TAG))
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_TAG)
else
	$(info Golangci-lint is already installed to $(GOLANGCI_BIN))
endif

install-protoc: export GOBIN := $(LOCAL_BIN)
install-protoc:
	$(info Installing protoc)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_TAG)

install-protoc-grpc: export GOBIN := $(LOCAL_BIN)
install-protoc-grpc:
	$(info Installing protoc-grpc)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_TAG) 

install-grpc-gateway: export GOBIN := $(LOCAL_BIN)
install-grpc-gateway:
	$(info Installing grpc-gateway)
	go install \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@$(PROTOC_GEN_GRPC_GATEWAY_TAG) \
    	github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@$(PROTOC_GEN_GRPC_GATEWAY_TAG)


install-smartimports: export GOBIN := $(LOCAL_BIN)
install-smartimports:
	$(info Installing smartimports)
	go install github.com/pav5000/smartimports/cmd/smartimports@v0.2.0