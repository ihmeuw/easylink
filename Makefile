this_makefile := $(lastword $(MAKEFILE_LIST)) # Used to automatically list targets
.DEFAULT_GOAL := list # If someone runs "make", run "make list"

# Source files to format, lint, and type check.
LOCATIONS=src tests

# Unless overridden, build conda environment using the package name.
PACKAGE_NAME = easylink
SAFE_NAME = $(shell python -c "from pkg_resources import safe_name; print(safe_name(\"$(PACKAGE_NAME)\"))")

about_file   = $(shell find src -name __about__.py)
version_file = $(shell find src -name _version.py)
version_line = $(shell grep "__version__ = " ${version_file})
PACKAGE_VERSION = $(shell echo ${version_line} | cut -d "=" -f 2 | xargs)

# Use this URL to pull IHME Python packages and deploy this package to PyPi.
IHME_PYPI := https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/

# If CONDA_ENV_PATH is set (from a Jenkins build), use the -p flag when making Conda env in
# order to make env at specific path. Otherwise, make a named env at the default path using
# the -n flag.
# TODO: [MIC-4953] build w/ multiple python versions
PYTHON_VERSION ?= 3.11
CONDA_ENV_NAME ?= ${PACKAGE_NAME}_py${PYTHON_VERSION}
CONDA_ENV_CREATION_FLAG = $(if $(CONDA_ENV_PATH),-p ${CONDA_ENV_PATH},-n ${CONDA_ENV_NAME})

# These are the doc and source code files in this repo.
# When one of these files changes, it means that Make targets need to run again.
MAKE_SOURCES := $(shell find . -type d -name "*" ! -path "./.git*" ! -path "./.vscode" ! -path "./output" ! -path "./output/*" ! -path "./archive" ! -path "./dist" ! -path "./output/htmlcov*" ! -path "**/.pytest_cache*" ! -path "**/__pycache__" ! -path "./output/docs_build*" ! -path "./.pytype*" ! -path "." ! -path "./src/${PACKAGE_NAME}/legacy*" ! -path ./.history ! -path "./.history/*" ! -path "./src/${PACKAGE_NAME}.egg-info" ! -path ./.idea ! -path "./.idea/*" )


# Phony targets don't produce artifacts.
.PHONY: .list-targets build-env clean debug deploy-doc deploy-package full help install list quick

# List of Make targets is generated dynamically. To add description of target, use a # on the target definition.
list help: debug .list-targets

.list-targets: # Print available Make targets
	@echo
	@echo "Make targets:"
	@grep -i "^[a-zA-Z][a-zA-Z0-9_ \.\-]*: .*[#].*" ${this_makefile} | sort | sed 's/:.*#/ : /g' | column -t -s:
	@echo

debug: # Print debug information (environment variables)
	@echo "'make' invoked with these environment variables:"
	@echo "CONDA_ENV_NAME:                   ${CONDA_ENV_NAME}"
	@echo "IHME_PYPI:                        ${IHME_PYPI}"
	@echo "LOCATIONS:                        ${LOCATIONS}"
	@echo "PACKAGE_NAME:                     ${PACKAGE_NAME}"
	@echo "PACKAGE_VERSION:                  ${PACKAGE_VERSION}"
	@echo "PYPI_ARTIFACTORY_CREDENTIALS_USR: ${PYPI_ARTIFACTORY_CREDENTIALS_USR} "
	@echo "Make sources:                     ${MAKE_SOURCES}"

build-env: # Make a new conda environment
	@[ "${CONDA_ENV_NAME}" ] && echo "" > /dev/null || ( echo "CONDA_ENV_NAME is not set"; exit 1 )
	conda create ${CONDA_ENV_CREATION_FLAG} python=${PYTHON_VERSION} --yes

install: # Install setuptools, install this package in editable mode
	conda install python-graphviz
	pip install --upgrade pip setuptools
	pip install -e .[DEV]

format: setup.py pyproject.toml $(MAKE_SOURCES) # Run the code formatter and import sorter
	-black $(LOCATIONS)
	-isort $(LOCATIONS)
	@echo "Ignore, Created by Makefile, `date`" > $@

lint: .flake8 .bandit $(MAKE_SOURCES) # Run the code linter and package security vulnerability checker
	-flake8 $(LOCATIONS)
	-safety check
	@echo "Ignore, Created by Makefile, `date`" > $@

typecheck: pytype.cfg $(MAKE_SOURCES) # Run the type checker
	-pytype --config=pytype.cfg $(LOCATIONS)
	@echo "Ignore, Created by Makefile, `date`" > $@

e2e: $(MAKE_SOURCES) # Run the e2e tests
	export COVERAGE_FILE=./output/.coverage.e2e
	pytest -vvv --runslow --cov --cov-report term --cov-report html:./output/htmlcov_e2e tests/e2e/
	@echo "Ignore, Created by Makefile, `date`" > $@

integration: $(MAKE_SOURCES) # Run unit tests
	export COVERAGE_FILE=./output/.coverage.integration
	pytest -vvv --runslow --cov --cov-report term --cov-report html:./output/htmlcov_integration tests/integration/
	@echo "Ignore, Created by Makefile, `date`" > $@

unit: $(MAKE_SOURCES) # Run unit tests
	export COVERAGE_FILE=./output/.coverage.unit
	pytest -vvv --runslow --cov --cov-report term --cov-report html:./output/htmlcov_unit tests/unit/
	@echo "Ignore, Created by Makefile, `date`" > $@

build-doc: $(MAKE_SOURCES) # Build the Sphinx docs
	$(MAKE) -C docs/ html
	@echo "Ignore, Created by Makefile, `date`" > $@

clean: # Delete build artifacts and do any custom cleanup such as spinning down services
	@rm -rf format lint typecheck build-doc build-package unit e2e integration .pytest_cache .pytype
	@rm -rf dist output
	$(shell find . -type f -name '*py[co]' -delete -o -type d -name __pycache__ -delete)

quick: # Run a "quick" build
	$(MAKE) format lint typecheck unit build-doc

full: clean # Run a "full" build
	$(MAKE) install quick e2e build-package
