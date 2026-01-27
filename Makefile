.PHONY: all install dev proto test lint format clean docker docker-up docker-down

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

# Run unit tests
test:
	pytest tests/unit -v

# Run integration tests (requires MQTT broker)
test-integration:
	pytest tests/integration -v -m integration

# Run all tests
test-all:
	pytest tests -v

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
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Build Docker image
docker:
	docker build -f docker/Dockerfile -t aas-uns-bridge:latest .

# Start Docker Compose stack
docker-up:
	cd docker && docker-compose up -d

# Stop Docker Compose stack
docker-down:
	cd docker && docker-compose down

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
