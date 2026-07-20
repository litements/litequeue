SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

.DEFAULT_GOAL := help
.PHONY: help
help: ## Display this message
	@grep -E \
		'^[a-zA-Z\.\$$/]+.*:.*?##\s.*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-38s\033[0m %s\n", $$1, $$2}'


.PHONY: install
install: ## Create the environment and install development dependencies
	uv sync --group dev

.PHONY: dist
dist: ## Build source and wheel distributions
	uv build --no-sources

.PHONY: clean
clean: ## Clean artifacts
	@rm -rf build/ dist/ src/*.egg-info/


.PHONY: test
test: ## Run tests
	uv run --group dev pytest

.PHONY: typecheck
typecheck: ## Run static type checks
	uv run --group dev mypy .

.PHONY: lint
lint: ## Run lint checks
	uv run --group dev ruff check .


.PHONY: benchmark
benchmark: ## Run benchmarks
	uv run benchmark.py


.PHONY: publish
publish: ## Test, bump the minor version, build, and publish to PyPI
	@: "$${UV_PUBLISH_TOKEN:?Set UV_PUBLISH_TOKEN before publishing}"
	$(MAKE) test
	@rm -rf dist/
	uv build --no-sources
	uv publish
	
tag: _TAG := $${TAG:?'FAIL. TAG variable not set'}
tag:
	git tag $(_TAG)
	git push --atomic --set-upstream origin main $(_TAG)
