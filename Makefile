GO_IMAGE := golang:1.26-alpine
GO := docker run --rm -v $(CURDIR):/src -v gomodcache:/go/pkg/mod -w /src
DB_ENV := -e POSTGRES_URL='postgres://ingestor:ingestor@postgres:5432/ingestor?sslmode=disable' -e REDIS_ADDR=redis:6379

.PHONY: demo up down test test-integration bench chaos observability k8s-validate

demo:          ## Guided tour: starts the stack and checks each behaviour (~2 min)
	./demo/run.sh

up:            ## Start API, workers, Postgres and Redis
	docker compose up -d --build --wait

observability: ## Start Prometheus (9090) and Grafana (3000) next to the stack
	docker compose --profile observability up -d --build --wait

down:
	docker compose --profile observability down -v

test:          ## Unit tests (no external services)
	$(GO) $(GO_IMAGE) go test -count=1 ./...

test-integration: ## Unit + integration tests against the compose Postgres/Redis
	docker compose up -d --wait postgres redis
	$(GO) --network ingestor_default $(DB_ENV) $(GO_IMAGE) go test -count=1 -tags integration ./...

bench:         ## direct vs buffered load test with k6, results in bench/results
	./bench/run.sh 30s 100

chaos:         ## SIGKILL workers and API under load, then verify every event
	./bench/chaos.sh

k8s-validate:
	docker run --rm -v $(CURDIR)/deploy/k8s:/k registry.k8s.io/kubectl:v1.34.1 kustomize /k > rendered.yaml
	docker run --rm -v $(CURDIR)/rendered.yaml:/r.yaml ghcr.io/yannh/kubeconform:latest -strict -summary /r.yaml
