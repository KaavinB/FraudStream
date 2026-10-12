PYTHON ?= .venv/bin/python
UV_CACHE_DIR ?= $(CURDIR)/.cache/uv
export UV_CACHE_DIR
# Use a local Go toolchain when present, otherwise the same image the services build with.
GO := $(shell command -v go >/dev/null 2>&1 && echo go || echo 'docker run --rm -v $(CURDIR):/src -w /src golang:1.26.8-bookworm go')
GOFMT := $(shell command -v gofmt >/dev/null 2>&1 && echo gofmt || echo 'docker run --rm -v $(CURDIR):/src -w /src golang:1.26.8-bookworm gofmt')

.PHONY: setup data train train-local up down migrate demo smoke test test-python test-go lint integration benchmark benchmark-local report monitoring fmt
setup:
	uv venv --python 3.12 .venv
	uv pip install --python $(PYTHON) -e '.[dev]'
	$(PYTHON) scripts/setup.py
	$(GO) mod tidy
data:
	$(PYTHON) -m ml.data.generate
train: data
	OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2 $(PYTHON) -m ml.training.train --mlflow
train-local:
	OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2 $(PYTHON) -m ml.training.train --allow-missing-xgboost
up:
	docker compose up -d --build
down:
	docker compose down
# Fresh volumes run every migration via docker-entrypoint-initdb.d; existing volumes need this.
migrate:
	for f in database/migrations/*.sql; do docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -q -U fraudstream -d fraudstream < $$f || exit 1; done
monitoring:
	docker compose --profile monitoring up -d
demo:
	$(PYTHON) -m streaming.replay --count 1000 --concurrency 1 --wait-each
smoke:
	$(PYTHON) -m streaming.replay --count 20 --concurrency 1 --wait-each
test: lint test-python test-go
test-python:
	OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2 $(PYTHON) -m pytest -q
test-go:
	$(GO) test -race -count=1 ./...
	$(GO) vet ./...
lint:
	$(PYTHON) -m ruff check ml services/inference streaming benchmarks scripts tests
	$(PYTHON) -m mypy ml services/inference/app streaming benchmarks
fmt:
	$(GOFMT) -w internal services/ingestion services/processor services/alerts
	$(PYTHON) -m ruff format ml services/inference streaming benchmarks scripts tests
integration:
	$(PYTHON) -m scripts.integration
benchmark:
	$(PYTHON) -m benchmarks.pipeline --concurrency 1 10 25 50 100 --count 500
	$(PYTHON) -m benchmarks.report
benchmark-local:
	OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2 $(PYTHON) -m benchmarks.local
	$(PYTHON) -m benchmarks.report
report:
	$(PYTHON) -m benchmarks.report
