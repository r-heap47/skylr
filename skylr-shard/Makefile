# Директория, в которой хранятся исполняемые
# файлы проекта и зависимости, необходимые для сборки.
LOCAL_BIN := $(CURDIR)/bin

# Шорткат для golangci-lint
GOLANGCI_BIN := $(LOCAL_BIN)/golangci-lint

# Требуемая актуальная версия линтера.
GOLANGCI_TAG ?= 2.6.2

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

.PHONY: lint
lint: .lint ## Запуск golangci-lint по всем файлам проекта

.lint: install-lint
	$(info Running lint against all project files...)
	$(GOLANGCI_BIN) run \
		--config=.golangci.yaml \
		--max-issues-per-linter=1000 \
		--max-same-issues=1000 \
		./...

install-lint: export GOBIN := $(LOCAL_BIN)
install-lint: ## Установить golangci-lint в текущую директорию с исполняемыми файлами
ifeq ($(wildcard $(GOLANGCI_BIN)),)
	@# Если в предыдущих шагах не был найден исполняемый
	@# файл линтера - устанавливаем его из исходников локально.
	$(info Installing golangci-lint v$(GOLANGCI_TAG))
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$(GOLANGCI_TAG)
# Устанавливаем текущий путь для исполняемого файла линтера.
GOLANGCI_BIN := $(LOCAL_BIN)/golangci-lint
else
	$(info Golangci-lint is already installed to $(GOLANGCI_BIN))
endif


