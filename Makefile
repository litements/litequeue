SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

py = $$(if [ -d $(PWD)/'.venv' ]; then echo $(PWD)/".venv/bin/python3"; else echo "python3"; fi)
pip = $(py) -m pip
venv_bin = .venv/bin


.DEFAULT_GOAL := help
.PHONY: help
help: ## Display this help section
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z\$$/]+.*:.*?##\s/ {printf "\033[36m%-38s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)


.venv:  ## Create venv
	$(py) -m venv .venv
	$(pip) install -U pip setuptools



.init: .venv
	@$(pip) install -U wheel build twine black mypy ruff pytest
	@$(pip) install -U --force-reinstall -e .
	touch .init

.PHONY: install
dev: .init ## Create .venv and install basic packages

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
test: .venv .init
	$(py) -m pytest test.py


.PHONY: publish
publish: TWINE_USERNAME = __token__
publish: .init dist  ## Publish to PyPi
	$(venv_bin)/twine check dist/*
	$(venv_bin)/twine --non-interactive dist/*
	

tag:
	@if [ -z $${TAG+x} ]; then echo "TAG variable not set" && exit 1; fi
	git tag $(TAG)
	git push --atomic --set-upstream origin main $(TAG)

