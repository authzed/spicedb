SHELL := /bin/bash

SCRIPTS := "./.github/workflows/scripts"

.PHONY: deps
deps: ## Install all the build and lint dependencies
	bash $(SCRIPTS)/deps.sh

.PHONY: fmt
fmt: ## Run format tools on all go files
	gci write --skip-vendor --skip-generated \
        -s standard -s default -s "prefix(github.com/maypok86/otter)" .
	gofumpt -l -w .

.PHONY: lint
lint: ## Run all the linters
	golangci-lint run -v ./...

.PHONY: test
test: test.unit ## Run all the tests

.PHONY: test.unit
test.unit: ## Run all unit tests
	bash $(SCRIPTS)/run-tests.sh

.PHONY: test.32-bit
test.32-bit: ## Run tests on 32-bit arch
	GOARCH=386 go test -v ./...

.PHONY: cover
cover: test.unit ## Run all the tests and opens the coverage report
	go tool cover -html=coverage.txt

.PHONY: ci
ci: lint test ## Run all the tests and code checks

.PHONY: generate
generate: gennode fmt ## Generate files for the project

.PHONY: gennode
gennode: ## Generate nodes
	go run ./cmd/generator ./internal/generated/node

.PHONY: clean
clean: ## Remove temporary files
	@go clean
	@rm -rf bin/
	@rm -rf coverage.txt lint.txt
	@echo "SUCCESS!"

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL:= help
