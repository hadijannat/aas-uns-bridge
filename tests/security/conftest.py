"""Fixtures for security tests."""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register security test markers."""
    config.addinivalue_line("markers", "security: security and input validation tests")


@pytest.fixture
def malicious_inputs() -> dict[str, list[str]]:
    """Provide malicious input test cases."""
    return {
        "mqtt_injection": [
            "../../../etc/passwd",
            "topic/#/injection",
            "topic/+/wildcard",
            "topic\x00null",
            "topic\nwith\nnewlines",
            "a" * 10000,  # Very long topic
        ],
        "json_bombs": [
            '{"a":' * 100 + '1' + '}' * 100,  # Deeply nested
            '["' + 'a' * 10000 + '"]',  # Large string
        ],
        "path_traversal": [
            "../../etc/passwd",
            "..\\..\\windows\\system32",
            "/absolute/path",
            "valid/../../../escape",
        ],
    }
