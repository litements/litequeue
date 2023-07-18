SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

py = $(CURDIR)/.venv/bin/python3
pip = $(py) -m pip
venv_bin = .venv/bin


.DEFAULT_GOAL := help
.PHONY: help
help: ## Display this message
	@grep -E \
		'^[a-zA-Z\.\$$/]+.*:.*?##\s.*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-38s\033[0m %s\n", $$1, $$2}'



.venv:  ## Create venv
	python3 -m venv .venv
	$(pip) install -U pip setuptools
	touch .venv



.init: .venv
	@$(pip) install -U wheel build twine black mypy ruff pytest
	@$(pip) install -U --force-reinstall -e .
	touch .init

.PHONY: install
install: .init ## Create .venv and install basic packages

dist:  ## Build package for distribution
	$(py) -m build --sdist --wheel --outdir dist/ .

.PHONY: clean
clean:  ## Clean artifacts
	@rm -rf *.egg-info/ build/ dist/


.PHONY: fix
fix:  ## Run ruff and black
	$(venv_bin)/ruff --ignore E501 .
	$(venv_bin)/black .


.PHONY: test
test: .venv .init  ## Run tests
	$(py) -m pytest test.py


.PHONY: publish
publish: TWINE_USERNAME = __token__
publish: .init dist  ## Publish to PyPi
	$(venv_bin)/twine check dist/*
	$(venv_bin)/twine upload --non-interactive dist/*
	
tag: _TAG := $${TAG:?'FAIL. TAG variable not set'}
tag:
	git tag $(_TAG)
	git push --atomic --set-upstream origin main $(_TAG)

