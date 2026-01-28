"""Fixtures for chaos engineering tests."""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register chaos test markers."""
    config.addinivalue_line("markers", "chaos: chaos engineering tests")


@pytest.fixture
def fault_injector():
    """Create a fault injector for simulating failures."""
    class FaultInjector:
        def __init__(self):
            self._active_faults: list[str] = []

        def inject(self, fault_type: str) -> None:
            self._active_faults.append(fault_type)

        def clear(self) -> None:
            self._active_faults.clear()

        def is_active(self, fault_type: str) -> bool:
            return fault_type in self._active_faults

    injector = FaultInjector()
    yield injector
    injector.clear()
