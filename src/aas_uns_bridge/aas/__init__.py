"""AAS ingestion layer for loading and traversing Asset Administration Shells."""

from aas_uns_bridge.aas.loader import load_aasx, load_json
from aas_uns_bridge.aas.traversal import flatten_submodel

__all__ = ["load_aasx", "load_json", "flatten_submodel"]
