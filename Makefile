.PHONY: all install dev proto test test-integration test-e2e test-load test-all lint format clean docker docker-up docker-down test-docker-up test-docker-down fixtures ci

PYTHON := python3
PIP := $(PYTHON) -m pip

all: install proto

# Install the package
install:
	$(PIP) install -e .

# Install with development dependencies
dev:
	$(PIP) install -e ".[dev]"

# Generate protobuf bindings
proto:
	mkdir -p src/aas_uns_bridge/proto
	protoc --python_out=src/aas_uns_bridge/proto proto/sparkplug_b.proto
	touch src/aas_uns_bridge/proto/__init__.py

# Generate test fixtures
fixtures:
	$(PYTHON) tests/fixtures/generate_fixtures.py

# Run unit tests
test:
	pytest tests/unit -v

# Run integration tests (requires MQTT broker)
test-integration:
	pytest tests/integration -v -m integration

# Run E2E tests (requires MQTT broker)
test-e2e:
	pytest tests/e2e -v -m e2e

# Run load tests (requires MQTT broker)
test-load:
	pytest tests/load -v -m load --timeout=120

# Run all tests except load
test-all:
	pytest tests -v --ignore=tests/load

# Run full test suite including load tests
test-full:
	pytest tests -v --timeout=120

# Run linting
lint:
	ruff check src tests
	mypy src

# Format code
format:
	ruff format src tests
	ruff check --fix src tests

# Clean build artifacts
clean:
	rm -rf build dist *.egg-info
	rm -rf src/aas_uns_bridge/proto/sparkplug_b_pb2.py
	rm -rf tests/fixtures/*.aasx
	rm -rf tests/fixtures/large_aas_5k_properties.json
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# Build Docker image
docker:
	docker build -f docker/Dockerfile -t aas-uns-bridge:latest .

# Start Docker Compose stack
docker-up:
	cd docker && docker-compose up -d

# Stop Docker Compose stack
docker-down:
	cd docker && docker-compose down

# Start test Docker Compose (broker only)
test-docker-up:
	cd docker && docker-compose -f docker-compose.test.yml up -d
	@echo "Waiting for Mosquitto to be ready..."
	@for i in $$(seq 1 30); do \
		if mosquitto_pub -h localhost -p 1883 -t test/health -m "ok" 2>/dev/null; then \
			echo "Mosquitto is ready"; \
			break; \
		fi; \
		echo "Waiting... ($$i/30)"; \
		sleep 1; \
	done

# Stop test Docker Compose
test-docker-down:
	cd docker && docker-compose -f docker-compose.test.yml down -v

# View Docker logs
docker-logs:
	cd docker && docker-compose logs -f bridge

# Copy example configs
init-config:
	mkdir -p config
	cp config/config.example.yaml config/config.yaml
	cp config/mappings.example.yaml config/mappings.yaml

# Run the bridge locally
run:
	aas-uns-bridge run --config config/config.yaml --mappings config/mappings.yaml

# Validate configuration
validate:
	aas-uns-bridge validate --config config/config.yaml --mappings config/mappings.yaml

# Run local CI (lint + all tests)
ci: lint fixtures test test-integration test-e2e
	@echo "Local CI passed!"

# Quick CI check (lint + unit tests only)
ci-quick: lint test
	@echo "Quick CI passed!"

# Help
help:
	@echo "Available targets:"
	@echo "  install          - Install the package"
	@echo "  dev              - Install with dev dependencies"
	@echo "  proto            - Generate protobuf bindings"
	@echo "  fixtures         - Generate test fixtures"
	@echo "  test             - Run unit tests"
	@echo "  test-integration - Run integration tests (requires broker)"
	@echo "  test-e2e         - Run E2E tests (requires broker)"
	@echo "  test-load        - Run load tests (requires broker)"
	@echo "  test-all         - Run all tests except load"
	@echo "  test-full        - Run full test suite including load"
	@echo "  lint             - Run linting and type checks"
	@echo "  format           - Format code"
	@echo "  clean            - Clean build artifacts"
	@echo "  test-docker-up   - Start test broker"
	@echo "  test-docker-down - Stop test broker"
	@echo "  ci               - Run local CI suite"
	@echo "  ci-quick         - Run quick CI check"
