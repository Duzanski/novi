.PHONY: help install install-dev test lint format type-check clean run

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install production dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements.txt
	pip install -e ".[dev]"

test: ## Run tests
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term

lint: ## Run linting
	flake8 src/ tests/ main.py
	
format: ## Format code with black
	black src/ tests/ main.py

type-check: ## Run type checking
	mypy src/ main.py

clean: ## Clean up generated files
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf src/__pycache__/
	rm -rf tests/__pycache__/
	rm -rf .mypy_cache/
	find . -type d -name "__pycache__" -delete
	find . -type f -name "*.pyc" -delete

run: ## Run the ETL pipeline
	python main.py

run-debug: ## Run the ETL pipeline with debug logging
	python main.py --log-level DEBUG

check: lint type-check test ## Run all checks (lint, type-check, test)

setup-dev: install-dev ## Set up development environment
	@echo "Development environment set up successfully!"
	@echo "Run 'make test' to run tests"
	@echo "Run 'make run' to run the ETL pipeline" 