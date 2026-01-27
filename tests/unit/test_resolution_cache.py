"""Unit tests for SemanticResolutionCache."""

import tempfile
from pathlib import Path

import pytest

from aas_uns_bridge.semantic.models import SemanticContext, SemanticPointer
from aas_uns_bridge.semantic.resolution_cache import SemanticResolutionCache


@pytest.fixture
def temp_db() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "cache.db"


@pytest.fixture
def sample_context() -> SemanticContext:
    """Create a sample semantic context."""
    return SemanticContext(
        semantic_id="0173-1#02-AAO677#002",
        dictionary="ECLASS",
        version="14.0",
        definition="Operating temperature in degrees Celsius",
        preferred_name="Temperature",
        unit="degC",
        data_type="xs:double",
    )


@pytest.fixture
def cache(temp_db: Path) -> SemanticResolutionCache:
    """Create a resolution cache for testing."""
    return SemanticResolutionCache(temp_db, max_memory_entries=100, preload=False)


class TestSemanticResolutionCacheBasic:
    """Basic tests for SemanticResolutionCache."""

    def test_register_and_resolve(
        self, cache: SemanticResolutionCache, sample_context: SemanticContext
    ) -> None:
        """Test registering and resolving a context."""
        pointer = cache.register(sample_context)

        resolved = cache.resolve(pointer)

        assert resolved is not None
        assert resolved.semantic_id == sample_context.semantic_id
        assert resolved.unit == sample_context.unit

    def test_resolve_nonexistent_returns_none(self, cache: SemanticResolutionCache) -> None:
        """Test resolving a non-existent pointer returns None."""
        pointer = SemanticPointer(hash="nonexistent1234", dictionary="test", version="1.0")

        result = cache.resolve(pointer)

        assert result is None

    def test_resolve_by_hash(
        self, cache: SemanticResolutionCache, sample_context: SemanticContext
    ) -> None:
        """Test resolving by hash string directly."""
        pointer = cache.register(sample_context)

        resolved = cache.resolve_by_hash(pointer.hash)

        assert resolved is not None
        assert resolved.semantic_id == sample_context.semantic_id

    def test_resolve_by_semantic_id(
        self, cache: SemanticResolutionCache, sample_context: SemanticContext
    ) -> None:
        """Test resolving by semantic ID (reverse lookup)."""
        cache.register(sample_context)

        resolved = cache.resolve_by_semantic_id("0173-1#02-AAO677#002")

        assert resolved is not None
        assert resolved.semantic_id == "0173-1#02-AAO677#002"

    def test_contains(
        self, cache: SemanticResolutionCache, sample_context: SemanticContext
    ) -> None:
        """Test checking if pointer is registered."""
        pointer = cache.register(sample_context)
        unknown = SemanticPointer(hash="unknown12345678", dictionary="test", version="1.0")

        assert cache.contains(pointer)
        assert not cache.contains(unknown)

    def test_get_pointer(
        self, cache: SemanticResolutionCache, sample_context: SemanticContext
    ) -> None:
        """Test getting pointer by semantic ID."""
        cache.register(sample_context)

        pointer = cache.get_pointer("0173-1#02-AAO677#002")

        assert pointer is not None
        assert pointer.dictionary == "ECLASS"


class TestSemanticResolutionCacheBatch:
    """Tests for batch operations."""

    def test_register_batch(self, cache: SemanticResolutionCache) -> None:
        """Test registering multiple contexts efficiently."""
        contexts = [
            SemanticContext.from_semantic_id(f"0173-1#02-TEST{i:03d}#001") for i in range(10)
        ]

        pointers = cache.register_batch(contexts)

        assert len(pointers) == 10
        for pointer in pointers:
            resolved = cache.resolve(pointer)
            assert resolved is not None

    def test_register_batch_idempotent(
        self, cache: SemanticResolutionCache, sample_context: SemanticContext
    ) -> None:
        """Test that registering same context twice returns same pointer."""
        # Register once
        pointer1 = cache.register(sample_context)

        # Register again in batch
        pointers = cache.register_batch([sample_context])

        assert len(pointers) == 1
        assert pointers[0].hash == pointer1.hash


class TestSemanticResolutionCacheLRU:
    """Tests for LRU eviction behavior."""

    def test_lru_eviction(self, temp_db: Path) -> None:
        """Test that LRU eviction works when cache is full."""
        cache = SemanticResolutionCache(temp_db, max_memory_entries=5, preload=False)

        # Register 5 contexts to fill cache
        contexts = [
            SemanticContext.from_semantic_id(f"0173-1#02-TEST{i:03d}#001") for i in range(5)
        ]
        pointers = cache.register_batch(contexts)

        assert cache.memory_size == 5

        # Register one more to trigger eviction
        new_context = SemanticContext.from_semantic_id("0173-1#02-NEWXX#001")
        cache.register(new_context)

        # Memory cache should still be at max
        assert cache.memory_size == 5

        # First entry should be evicted from memory but still in DB
        resolved = cache.resolve(pointers[0])
        assert resolved is not None  # Loaded from DB

    def test_access_updates_lru_order(self, temp_db: Path) -> None:
        """Test that accessing an entry moves it to end of LRU."""
        cache = SemanticResolutionCache(temp_db, max_memory_entries=3, preload=False)

        # Register 3 contexts
        c1 = SemanticContext.from_semantic_id("0173-1#02-TEST001#001")
        c2 = SemanticContext.from_semantic_id("0173-1#02-TEST002#001")
        c3 = SemanticContext.from_semantic_id("0173-1#02-TEST003#001")

        p1 = cache.register(c1)
        cache.register(c2)  # p2 not used later
        cache.register(c3)  # p3 not used later

        # Access p1 to make it most recently used
        cache.resolve(p1)

        # Add new context - should evict p2 (oldest), not p1
        c4 = SemanticContext.from_semantic_id("0173-1#02-TEST004#001")
        cache.register(c4)

        # p1 should still be in memory
        assert p1.hash in [h for h, _ in cache.iter_all()]


class TestSemanticResolutionCachePersistence:
    """Tests for database persistence."""

    def test_persistence_across_instances(
        self, temp_db: Path, sample_context: SemanticContext
    ) -> None:
        """Test that contexts persist across cache instances."""
        # First instance
        cache1 = SemanticResolutionCache(temp_db, preload=False)
        pointer = cache1.register(sample_context)

        # Second instance
        cache2 = SemanticResolutionCache(temp_db, preload=True)
        resolved = cache2.resolve(pointer)

        assert resolved is not None
        assert resolved.semantic_id == sample_context.semantic_id

    def test_preload_on_startup(self, temp_db: Path) -> None:
        """Test that entries are preloaded from DB on startup."""
        # First instance - register some contexts
        cache1 = SemanticResolutionCache(temp_db, max_memory_entries=100, preload=False)
        for i in range(5):
            cache1.register(SemanticContext.from_semantic_id(f"0173-1#02-TEST{i:03d}#001"))

        # Second instance with preload
        cache2 = SemanticResolutionCache(temp_db, max_memory_entries=100, preload=True)

        assert cache2.memory_size == 5

    def test_total_size_includes_db(self, temp_db: Path) -> None:
        """Test that total_size includes all DB entries."""
        cache = SemanticResolutionCache(temp_db, max_memory_entries=3, preload=False)

        # Register 5 contexts (more than memory limit)
        for i in range(5):
            cache.register(SemanticContext.from_semantic_id(f"0173-1#02-TEST{i:03d}#001"))

        assert cache.memory_size == 3  # Limited by max_memory_entries
        assert cache.total_size == 5  # All are in DB

    def test_clear(self, cache: SemanticResolutionCache, sample_context: SemanticContext) -> None:
        """Test clearing all cached contexts."""
        cache.register(sample_context)
        assert cache.memory_size > 0

        cache.clear()

        assert cache.memory_size == 0
        assert cache.total_size == 0


class TestSemanticResolutionCacheIteration:
    """Tests for iterating over cached contexts."""

    def test_iter_all(self, cache: SemanticResolutionCache) -> None:
        """Test iterating over all cached contexts."""
        contexts = [
            SemanticContext.from_semantic_id(f"0173-1#02-TEST{i:03d}#001") for i in range(3)
        ]
        cache.register_batch(contexts)

        entries = list(cache.iter_all())

        assert len(entries) == 3
        for hash_val, context in entries:
            assert len(hash_val) == 16
            assert context.semantic_id is not None
