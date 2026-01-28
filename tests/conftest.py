"""Shared pytest fixtures for AAS-UNS Bridge tests."""

import os
import time
from pathlib import Path

import pytest

# Test fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers.

    Note: Markers are also defined in pyproject.toml [tool.pytest.ini_options].
    This function ensures they're registered even when running pytest directly.
    """
    config.addinivalue_line("markers", "integration: integration tests requiring MQTT broker")
    config.addinivalue_line("markers", "e2e: end-to-end integration tests")
    config.addinivalue_line("markers", "slow: slow-running tests")
    config.addinivalue_line("markers", "load: load/performance tests")
    config.addinivalue_line("markers", "chaos: chaos engineering tests for failure resilience")
    config.addinivalue_line("markers", "stability: long-running stability and endurance tests")
    config.addinivalue_line("markers", "security: security and input validation tests")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-mark tests based on their location."""
    for item in items:
        # Mark tests in integration/ directory
        if "/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)

        # Mark tests in e2e/ directory
        if "/e2e/" in str(item.fspath):
            item.add_marker(pytest.mark.e2e)

        # Mark tests in load/ directory
        if "/load/" in str(item.fspath):
            item.add_marker(pytest.mark.load)

        # Mark tests in chaos/ directory
        if "/chaos/" in str(item.fspath):
            item.add_marker(pytest.mark.chaos)

        # Mark tests in stability/ directory
        if "/stability/" in str(item.fspath):
            item.add_marker(pytest.mark.stability)

        # Mark tests in security/ directory
        if "/security/" in str(item.fspath):
            item.add_marker(pytest.mark.security)


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """Return the fixtures directory path."""
    return FIXTURES_DIR


@pytest.fixture
def unique_id() -> str:
    """Generate a unique ID for test isolation."""
    return f"{time.time()}-{os.getpid()}"
