.PHONY: install dev test test-unit test-integration test-all test-cov test-watch clean run help lint format check install-editable ci-test ci-lint

PYTHON := python3
VENV := .venv
BIN := $(VENV)/bin

help:
	@echo "Available commands:"
	@echo ""
	@echo "Setup:"
	@echo "  make install       - Create venv and install dependencies"
	@echo "  make dev           - Install development dependencies"
	@echo "  make install-editable - Install package in editable mode"
	@echo ""
	@echo "Testing:"
	@echo "  make test          - Run unit tests (default)"
	@echo "  make test-unit     - Run unit tests explicitly"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-all      - Run all tests"
	@echo "  make test-cov      - Run tests with coverage report"
	@echo "  make test-watch    - Run tests in watch mode"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint          - Run linting (ruff + mypy)"
	@echo "  make format        - Format code with ruff"
	@echo "  make check         - Run lint + test (quick verification)"
	@echo ""
	@echo "CI/CD:"
	@echo "  make ci-test       - Run tests with CI-friendly output"
	@echo "  make ci-lint       - Run linting with CI-friendly output"
	@echo ""
	@echo "Operations:"
	@echo "  make run           - Run the k8s-scanner CLI"
	@echo "  make clean         - Remove virtual environment and cache files"

install:
	@echo "Creating virtual environment..."
	$(PYTHON) -m venv $(VENV)
	@echo "Installing dependencies..."
	$(BIN)/pip install -r requirements.txt

dev: install
	@echo "Installing development dependencies..."
	$(BIN)/pip install -r requirements-dev.txt

test:
	@echo "Running unit tests..."
	$(BIN)/pytest tests/ -m "not integration and not slow"

test-unit:
	@echo "Running unit tests only..."
	$(BIN)/pytest tests/ -m "unit or (not integration and not slow)"

test-integration:
	@echo "Running integration tests..."
	$(BIN)/pytest tests/ -m "integration"

test-all:
	@echo "Running all tests..."
	$(BIN)/pytest tests/

test-cov:
	@echo "Running tests with coverage..."
	$(BIN)/pytest tests/ -m "not integration and not slow" --cov=src --cov-report=term-missing --cov-report=html:htmlcov --cov-fail-under=80

test-watch:
	@echo "Running tests in watch mode..."
	$(BIN)/pytest-watch tests/ -m "not integration and not slow"

clean:
	@echo "Cleaning up..."
	rm -rf $(VENV)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

run:
	$(BIN)/python -m src.main $(ARGS)

scan:
	$(BIN)/python -m src.main scan $(ARGS)

report:
	$(BIN)/python -m src.main report $(ARGS)

upgrade-path:
	$(BIN)/python -m src.main upgrade-path $(ARGS)

drift:
	$(BIN)/python -m src.main drift $(ARGS)

# Historical tracking commands
history:
	$(BIN)/python -m src.main history $(ARGS)

changes:
	$(BIN)/python -m src.main changes $(ARGS)

compare:
	$(BIN)/python -m src.main compare $(ARGS)

resource-history:
	$(BIN)/python -m src.main resource-history $(ARGS)

summary:
	$(BIN)/python -m src.main summary $(ARGS)

cleanup:
	$(BIN)/python -m src.main cleanup $(ARGS)

db-info:
	$(BIN)/python -m src.main db-info

lint:
	@echo "Running linting..."
	$(BIN)/ruff check src/ tests/
	$(BIN)/mypy src/

format:
	@echo "Formatting code..."
	$(BIN)/ruff format src/ tests/

# Convenience targets
check: lint test
	@echo "All checks passed!"

install-editable: dev
	@echo "Installing package in editable mode..."
	$(BIN)/pip install -e .

# CI-friendly targets
ci-test: dev
	@echo "Running CI tests..."
	$(BIN)/pytest tests/ -v --tb=short

ci-lint: dev
	@echo "Running CI linting..."
	$(BIN)/ruff check src/ tests/ --output-format=github
	$(BIN)/mypy src/ --no-error-summary