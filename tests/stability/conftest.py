"""Fixtures for stability tests."""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register stability test markers."""
    config.addinivalue_line("markers", "stability: long-running stability tests")


@pytest.fixture
def memory_tracker():
    """Track memory usage over time."""
    import tracemalloc

    class MemoryTracker:
        def __init__(self):
            self._snapshots: list[tuple[float, int]] = []
            tracemalloc.start()

        def snapshot(self, timestamp: float) -> int:
            """Take a memory snapshot and return current usage."""
            current, peak = tracemalloc.get_traced_memory()
            self._snapshots.append((timestamp, current))
            return current

        def get_growth_ratio(self) -> float:
            """Get memory growth ratio (final / initial)."""
            if len(self._snapshots) < 2:
                return 1.0
            initial = self._snapshots[0][1]
            final = self._snapshots[-1][1]
            return final / initial if initial > 0 else 1.0

        def stop(self) -> None:
            tracemalloc.stop()

    tracker = MemoryTracker()
    yield tracker
    tracker.stop()
