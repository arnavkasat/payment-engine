# Targets a contributor runs locally; CI runs the same ones.

GO        ?= go
PKGS      := ./...
TEST_DB   ?= postgres://payments:payments@localhost:5432/payments?sslmode=disable

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build every binary
	$(GO) build -trimpath ./...

.PHONY: test
test: ## Run unit tests, including the chaos scenarios
	$(GO) test -race -count=1 $(PKGS)

.PHONY: cover
cover: ## Run tests and open a coverage summary
	$(GO) test -count=1 -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -30

.PHONY: test-integration
test-integration: ## Run the PostgreSQL conformance tests (needs a database)
	TEST_DATABASE_URL="$(TEST_DB)" $(GO) test -race -count=1 -tags=integration ./internal/storage/postgres/...

.PHONY: vet
vet: ## Run go vet, including the integration-tagged files
	$(GO) vet $(PKGS)
	$(GO) vet -tags=integration $(PKGS)

.PHONY: fmt
fmt: ## Format every Go file
	gofmt -s -w .

.PHONY: fmt-check
fmt-check: ## Fail if anything is unformatted
	@unformatted=$$(gofmt -s -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt'd:"; echo "$$unformatted"; exit 1; \
	fi

.PHONY: check
check: fmt-check vet test ## Everything CI runs on a pull request

.PHONY: chaos
chaos: ## Run the failure-injection harness against the in-memory store
	$(GO) run ./cmd/chaos -payments 500 -workers 8 -fault-probability 0.15 -retry-rate 0.3

.PHONY: chaos-postgres
chaos-postgres: ## Run the failure-injection harness against PostgreSQL
	$(GO) run ./cmd/chaos -dsn "$(TEST_DB)" -payments 500 -workers 8 -fault-probability 0.15 -retry-rate 0.3

.PHONY: load
load: ## Drive the running API and report throughput and latency
	$(GO) run ./cmd/loadgen -url http://localhost:8080 -concurrency 32 -duration 30s

.PHONY: demo
demo: ## Run the whole engine in one process, with no dependencies
	DATABASE_URL=memory $(GO) run ./cmd/api

.PHONY: up
up: ## Start PostgreSQL, Kafka, the API and the worker
	docker compose up --build -d

.PHONY: down
down: ## Stop everything and delete the volumes
	docker compose down -v

.PHONY: logs
logs: ## Follow the API and worker logs
	docker compose logs -f api worker

.PHONY: psql
psql: ## Open a shell on the database
	docker compose exec postgres psql -U payments -d payments
