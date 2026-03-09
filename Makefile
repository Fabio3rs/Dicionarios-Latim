# Makefile for Dicionários Latim-Português project

.PHONY: help install install-dev test lint format type-check clean example
.PHONY: data-export-v2 data-shards data-render search-index data-all web-build dev

help:  ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install production dependencies
	pip install -r requirements.txt

install-dev:  ## Install development dependencies
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

test:  ## Run basic tests
	python test_basic.py

test-pytest:  ## Run tests with pytest (if available)
	python -m pytest test_basic.py -v

lint:  ## Run linting checks
	flake8 *.py scripts/*.py

format:  ## Format code with black
	black *.py scripts/*.py

format-check:  ## Check if code is formatted correctly
	black --check *.py scripts/*.py

type-check:  ## Run type checking with mypy
	mypy config.py example_usage.py

clean:  ## Clean up generated files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -name "*.db-wal" -delete
	find . -name "*.db-shm" -delete
	rm -rf web/.astro web/.cache web/node_modules

example:  ## Run the example usage script
	python example_usage.py

# Frontend / data pipeline
data-export-v2: ## Exporta retificado_v2.db para normalized_results_v2.json
	python scripts/export_normalized_from_retificado_v2.py --db dicionarios/retificado_v2.db --out resultados/normalized_results_v2.json

data-shards:  ## Generate NDJSON shards from normalized_results via catalog
	python scripts/export_shards_from_normalized.py --catalog resultados/catalog.json --outdir resultados/shards --shard-size 1000

data-render: ## Render public JSON artifacts from shards into web/public/data
	python scripts/render_publication_from_shards.py --catalog resultados/catalog.json --shards resultados/shards --out web/public/data --pages-per-block 200

search-index: ## Build Pagefind index into web/public/pagefind
	cd web && npm run search:index

data-all: ## Run full data pipeline (shards + render + pagefind)
	make data-export-v2
	cd web && npm run data:all

web-build: ## Build Astro site
	cd web && npm run build

dev: ## Start Astro dev server (ensure deps installed)
	cd web && npm run dev

# Database operations
create-lexicon:  ## Create lexicon database from normalized results
	python scripts/ingest_normalized.py \
		--json resultados/normalized_results.json \
		--db resultados/lexicon.db \
		--schema scripts/schema_normalizado.sql \
		--batch-fts

query:  ## Run interactive query on lexicon
	python scripts/query_lexicon.py --db resultados/lexicon.db --interactive

export-diff:  ## Generate deterministic exports for diffs
	python scripts/export_for_diff.py --db resultados/lexicon.db --out-dir exports

# Development workflow
dev-setup:  ## Complete development setup
	make install-dev
	make test
	@echo "✅ Development environment ready!"

quality-check:  ## Run all quality checks
	make format-check
	make lint
	make type-check
	make test
	@echo "✅ All quality checks passed!"

# Documentation
docs:  ## Generate documentation (placeholder)
	@echo "📚 Documentation would be generated here"
	@echo "Consider using Sphinx or similar for the future"

# Project info
info:  ## Show project information
	@echo "🏛️  Dicionários de Latim para Português"
	@echo "📍 Directory: $(PWD)"
	@echo "🐍 Python: $(shell python --version)"
	@echo "📦 Dependencies: $(shell wc -l < requirements.txt) production, $(shell wc -l < requirements-dev.txt) development"
	@echo "📁 Scripts: $(shell ls scripts/*.py | wc -l) Python files"
	@echo "📊 Database files: $(shell find . -name "*.db" -o -name "*.sqlite" | wc -l) found"
