"""Semantic resolution and context management for the AAS-UNS Bridge.

This package provides:
- SemanticPointer: Hash-based references to semantic contexts (90% payload reduction)
- SemanticContext: Full semantic definitions for caching and distribution
- SemanticResolutionCache: Sub-ms semantic context resolution
- FidelityCalculator: Information-theoretic fidelity metrics
- FidelityReport: Comprehensive fidelity assessment with recommendations
"""

from aas_uns_bridge.semantic.fidelity import FidelityCalculator, FidelityReport
from aas_uns_bridge.semantic.models import SemanticContext, SemanticPointer
from aas_uns_bridge.semantic.resolution_cache import SemanticResolutionCache

__all__ = [
    "SemanticPointer",
    "SemanticContext",
    "SemanticResolutionCache",
    "FidelityCalculator",
    "FidelityReport",
]
