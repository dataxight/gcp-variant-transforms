#!/bin/bash

# Settings
SHELL := /bin/bash
.DEFAULT_GOAL := info

.PHONY: venv build

# Python package manager `uv` configures
export UV_LINK_MODE=copy
export UV_CONCURRENT_DOWNLOAD=5
export UV_CONCURRENT_BUILDS=5
export UV_CONCURRENT_INSTALLS=5
export UV_REQUEST_TIMEOUT=360

venv:
	uv venv --seed --python 3.9.16 venv

install-dev:
	@uv pip install --upgrade "pip>=24.3.1,<=25.0.1" "setuptools>=75.8.0,<=77.0.3" "wheel>=0.45.1,<=0.45.10" "hatch>=1.9.1,<1.10.0" --no-cache;

install-application:
	@uv pip install -r requirements.txt --no-cache;

install: install-dev install-application

build:
	uv pip install -e .

run-local:
	python3 -m gcp_variant_transforms.vcf_to_bq \
		--input_pattern gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf \
		--output_table GOOGLE_CLOUD_PROJECT:BIGQUERY_DATASET.BIGQUERY_TABLE \
		--job_name vcf-to-bigquery-direct-runner \
		--temp_location "${TEMP_LOCATION}"

clean-refresh:
	@rm -rf venv;

print-info:
	@echo "on-behalf-of: @dataxight baott@dataxight.com"
