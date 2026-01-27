"""Semantic resolution cache with LRU memory and SQLite persistence.

This module provides sub-millisecond semantic context resolution through
a two-tier caching strategy:
1. In-memory LRU cache for hot-path resolution
2. SQLite backing store for persistence across restarts

The cache enables the pointer mode payload optimization by allowing
subscribers to resolve 16-char hash references to full semantic contexts.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import TYPE_CHECKING

from aas_uns_bridge.observability.metrics import METRICS
from aas_uns_bridge.semantic.models import SemanticContext, SemanticPointer

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = logging.getLogger(__name__)


class SemanticResolutionCache:
    """LRU cache with SQLite backing for semantic contexts.

    Provides sub-millisecond resolution of SemanticPointer to SemanticContext
    through an in-memory LRU cache, with SQLite persistence for durability.

    Thread-safe for concurrent reads and writes.

    Attributes:
        db_path: Path to SQLite database file.
        max_memory_entries: Maximum entries in memory cache.
    """

    def __init__(
        self,
        db_path: Path,
        max_memory_entries: int = 10000,
        preload: bool = True,
    ):
        """Initialize the resolution cache.

        Args:
            db_path: Path to SQLite database file.
            max_memory_entries: Maximum entries in memory LRU cache.
            preload: Whether to preload existing entries from DB on startup.
        """
        self.db_path = db_path
        self.max_memory_entries = max_memory_entries
        self._lock = threading.RLock()

        # LRU cache using OrderedDict (move to end on access)
        self._memory_cache: OrderedDict[str, SemanticContext] = OrderedDict()

        # Reverse lookup: semantic_id -> hash
        self._semantic_id_to_hash: dict[str, str] = {}

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

        if preload:
            self._preload_from_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS semantic_context (
                    hash TEXT PRIMARY KEY,
                    semantic_id TEXT NOT NULL,
                    dictionary TEXT,
                    version TEXT,
                    definition TEXT,
                    preferred_name TEXT,
                    unit TEXT,
                    data_type TEXT,
                    hierarchy_json TEXT,
                    created_at INTEGER NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_semantic_id
                ON semantic_context(semantic_id)
            """)
            conn.commit()

    def _preload_from_db(self) -> None:
        """Preload entries from database into memory cache."""
        loaded = 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT hash, semantic_id, dictionary, version, definition,
                       preferred_name, unit, data_type, hierarchy_json
                FROM semantic_context
                ORDER BY created_at DESC
                LIMIT ?
            """,
                (self.max_memory_entries,),
            )

            for row in cursor:
                (
                    hash_val,
                    semantic_id,
                    dictionary,
                    version,
                    definition,
                    preferred_name,
                    unit,
                    data_type,
                    hierarchy_json,
                ) = row

                hierarchy = tuple(json.loads(hierarchy_json)) if hierarchy_json else ()

                context = SemanticContext(
                    semantic_id=semantic_id,
                    dictionary=dictionary or "unknown",
                    version=version or "1.0",
                    definition=definition,
                    preferred_name=preferred_name,
                    unit=unit,
                    data_type=data_type,
                    hierarchy=hierarchy,
                )

                self._memory_cache[hash_val] = context
                self._semantic_id_to_hash[semantic_id] = hash_val
                loaded += 1

        if loaded > 0:
            logger.info("Preloaded %d semantic contexts from cache", loaded)

    def resolve(self, pointer: SemanticPointer) -> SemanticContext | None:
        """Resolve a semantic pointer to its full context.

        This is the hot-path operation, optimized for sub-millisecond latency
        through the in-memory LRU cache.

        Args:
            pointer: The semantic pointer to resolve.

        Returns:
            The SemanticContext if found, None otherwise.
        """
        with self._lock:
            context = self._memory_cache.get(pointer.hash)
            if context is not None:
                # Move to end (most recently used)
                self._memory_cache.move_to_end(pointer.hash)
                METRICS.semantic_cache_hits_total.labels(cache_tier="memory").inc()
                return context

        # Cache miss in memory - try database
        result = self._load_from_db(pointer.hash)
        if result is not None:
            METRICS.semantic_cache_hits_total.labels(cache_tier="sqlite").inc()
        else:
            METRICS.semantic_cache_misses_total.inc()
        return result

    def resolve_by_hash(self, hash_value: str) -> SemanticContext | None:
        """Resolve by hash string directly.

        Args:
            hash_value: The 16-character hash string.

        Returns:
            The SemanticContext if found, None otherwise.
        """
        with self._lock:
            context = self._memory_cache.get(hash_value)
            if context is not None:
                self._memory_cache.move_to_end(hash_value)
                return context

        return self._load_from_db(hash_value)

    def resolve_by_semantic_id(self, semantic_id: str) -> SemanticContext | None:
        """Resolve by semantic ID (reverse lookup).

        Args:
            semantic_id: The full semantic identifier.

        Returns:
            The SemanticContext if found, None otherwise.
        """
        with self._lock:
            hash_value = self._semantic_id_to_hash.get(semantic_id)
            if hash_value:
                return self.resolve_by_hash(hash_value)

        # Try database lookup
        return self._load_by_semantic_id(semantic_id)

    def _load_from_db(self, hash_value: str) -> SemanticContext | None:
        """Load a context from the database and cache it.

        Args:
            hash_value: The hash to look up.

        Returns:
            The SemanticContext if found, None otherwise.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT semantic_id, dictionary, version, definition,
                       preferred_name, unit, data_type, hierarchy_json
                FROM semantic_context
                WHERE hash = ?
            """,
                (hash_value,),
            )

            row = cursor.fetchone()
            if row is None:
                return None

            (
                semantic_id,
                dictionary,
                version,
                definition,
                preferred_name,
                unit,
                data_type,
                hierarchy_json,
            ) = row

            hierarchy = tuple(json.loads(hierarchy_json)) if hierarchy_json else ()

            context = SemanticContext(
                semantic_id=semantic_id,
                dictionary=dictionary or "unknown",
                version=version or "1.0",
                definition=definition,
                preferred_name=preferred_name,
                unit=unit,
                data_type=data_type,
                hierarchy=hierarchy,
            )

            # Add to memory cache
            with self._lock:
                self._add_to_memory_cache(hash_value, context)

            return context

    def _load_by_semantic_id(self, semantic_id: str) -> SemanticContext | None:
        """Load a context by semantic ID from database.

        Args:
            semantic_id: The semantic identifier.

        Returns:
            The SemanticContext if found, None otherwise.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT hash, dictionary, version, definition,
                       preferred_name, unit, data_type, hierarchy_json
                FROM semantic_context
                WHERE semantic_id = ?
            """,
                (semantic_id,),
            )

            row = cursor.fetchone()
            if row is None:
                return None

            (
                hash_value,
                dictionary,
                version,
                definition,
                preferred_name,
                unit,
                data_type,
                hierarchy_json,
            ) = row

            hierarchy = tuple(json.loads(hierarchy_json)) if hierarchy_json else ()

            context = SemanticContext(
                semantic_id=semantic_id,
                dictionary=dictionary or "unknown",
                version=version or "1.0",
                definition=definition,
                preferred_name=preferred_name,
                unit=unit,
                data_type=data_type,
                hierarchy=hierarchy,
            )

            # Add to memory cache
            with self._lock:
                self._add_to_memory_cache(hash_value, context)

            return context

    def register(self, context: SemanticContext) -> SemanticPointer:
        """Register a semantic context and return its pointer.

        If the context is already registered (by semantic_id), returns
        the existing pointer without modification.

        Args:
            context: The semantic context to register.

        Returns:
            A SemanticPointer referencing the registered context.
        """
        pointer = context.to_pointer()
        hash_value = pointer.hash

        with self._lock:
            # Check if already in memory cache
            if hash_value in self._memory_cache:
                return pointer

            # Add to memory cache
            self._add_to_memory_cache(hash_value, context)
            self._semantic_id_to_hash[context.semantic_id] = hash_value

        # Persist to database
        self._persist(hash_value, context)

        # Update metrics
        METRICS.semantic_pointers_registered_total.inc()
        METRICS.semantic_cache_size.labels(tier="memory").set(self.memory_size)

        return pointer

    def register_batch(self, contexts: list[SemanticContext]) -> list[SemanticPointer]:
        """Register multiple contexts efficiently.

        Args:
            contexts: List of semantic contexts to register.

        Returns:
            List of corresponding SemanticPointers.
        """
        pointers: list[SemanticPointer] = []
        to_persist: list[tuple[str, SemanticContext]] = []

        with self._lock:
            for context in contexts:
                pointer = context.to_pointer()
                hash_value = pointer.hash
                pointers.append(pointer)

                if hash_value not in self._memory_cache:
                    self._add_to_memory_cache(hash_value, context)
                    self._semantic_id_to_hash[context.semantic_id] = hash_value
                    to_persist.append((hash_value, context))

        # Batch persist
        if to_persist:
            self._persist_batch(to_persist)

        return pointers

    def _add_to_memory_cache(self, hash_value: str, context: SemanticContext) -> None:
        """Add to memory cache with LRU eviction.

        Must be called with lock held.
        """
        # Evict if at capacity
        while len(self._memory_cache) >= self.max_memory_entries:
            evicted_hash, evicted_ctx = self._memory_cache.popitem(last=False)
            self._semantic_id_to_hash.pop(evicted_ctx.semantic_id, None)

        self._memory_cache[hash_value] = context
        self._semantic_id_to_hash[context.semantic_id] = hash_value

    def _persist(self, hash_value: str, context: SemanticContext) -> None:
        """Persist a single context to database."""
        hierarchy_json = json.dumps(list(context.hierarchy))
        now = int(time.time())

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO semantic_context
                (hash, semantic_id, dictionary, version, definition,
                 preferred_name, unit, data_type, hierarchy_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    hash_value,
                    context.semantic_id,
                    context.dictionary,
                    context.version,
                    context.definition,
                    context.preferred_name,
                    context.unit,
                    context.data_type,
                    hierarchy_json,
                    now,
                ),
            )
            conn.commit()

    def _persist_batch(self, items: list[tuple[str, SemanticContext]]) -> None:
        """Persist multiple contexts to database."""
        now = int(time.time())

        rows = [
            (
                hash_value,
                ctx.semantic_id,
                ctx.dictionary,
                ctx.version,
                ctx.definition,
                ctx.preferred_name,
                ctx.unit,
                ctx.data_type,
                json.dumps(list(ctx.hierarchy)),
                now,
            )
            for hash_value, ctx in items
        ]

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO semantic_context
                (hash, semantic_id, dictionary, version, definition,
                 preferred_name, unit, data_type, hierarchy_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                rows,
            )
            conn.commit()

        logger.debug("Persisted %d semantic contexts", len(rows))

    def contains(self, pointer: SemanticPointer) -> bool:
        """Check if a pointer is registered.

        Args:
            pointer: The semantic pointer to check.

        Returns:
            True if the pointer is registered.
        """
        with self._lock:
            if pointer.hash in self._memory_cache:
                return True

        # Check database
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT 1 FROM semantic_context WHERE hash = ?", (pointer.hash,))
            return cursor.fetchone() is not None

    def get_pointer(self, semantic_id: str) -> SemanticPointer | None:
        """Get the pointer for a semantic ID if registered.

        Args:
            semantic_id: The semantic identifier.

        Returns:
            The SemanticPointer if found, None otherwise.
        """
        context = self.resolve_by_semantic_id(semantic_id)
        return context.to_pointer() if context else None

    def iter_all(self) -> Iterator[tuple[str, SemanticContext]]:
        """Iterate over all cached contexts.

        Yields:
            Tuples of (hash, context).
        """
        with self._lock:
            yield from self._memory_cache.items()

    @property
    def memory_size(self) -> int:
        """Number of entries in memory cache."""
        with self._lock:
            return len(self._memory_cache)

    @property
    def total_size(self) -> int:
        """Total number of entries in database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM semantic_context")
            row = cursor.fetchone()
            return row[0] if row else 0

    def clear(self) -> None:
        """Clear all cached contexts."""
        with self._lock:
            self._memory_cache.clear()
            self._semantic_id_to_hash.clear()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM semantic_context")
            conn.commit()

        logger.info("Semantic resolution cache cleared")
