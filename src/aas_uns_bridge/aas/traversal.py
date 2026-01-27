"""Recursive traversal of AAS submodels to flatten elements into metrics."""

import logging
import time
from typing import Any, Iterator

from basyx.aas import model

from aas_uns_bridge.domain.models import ContextMetric

logger = logging.getLogger(__name__)


def _extract_semantic_id(element: model.HasSemantics) -> str | None:
    """Extract semantic ID from an element if present."""
    if element.semantic_id is not None:
        keys = list(element.semantic_id.key)
        if keys:
            return keys[0].value
    return None


def _extract_unit(element: model.HasDataSpecification) -> str | None:
    """Extract unit from DataSpecificationIEC61360 if present."""
    if not hasattr(element, "embedded_data_specifications"):
        return None

    for spec in element.embedded_data_specifications or []:
        content = spec.data_specification_content
        if hasattr(content, "unit") and content.unit:
            return content.unit
    return None


def _get_value_type(element: model.SubmodelElement) -> str:
    """Get the XSD value type string for an element.

    BaSyx v2.0 uses Python types directly (str, int, float, etc.)
    instead of an enum, so we map them to XSD type strings.
    """
    # Mapping of Python types to XSD type strings
    type_map = {
        str: "xs:string",
        int: "xs:int",
        float: "xs:double",
        bool: "xs:boolean",
        bytes: "xs:base64Binary",
    }

    value_type = None
    if isinstance(element, model.Property):
        value_type = element.value_type
    elif isinstance(element, model.Range):
        value_type = element.value_type

    if value_type is not None:
        # Check if it's a Python type
        if value_type in type_map:
            return type_map[value_type]
        # Check for BaSyx specific types
        type_name = getattr(value_type, "__name__", str(value_type))
        # Map common BaSyx datatypes
        basyx_map = {
            "Double": "xs:double",
            "Float": "xs:float",
            "Int": "xs:int",
            "Long": "xs:long",
            "Short": "xs:short",
            "Byte": "xs:byte",
            "String": "xs:string",
            "NormalizedString": "xs:string",
            "AnyURI": "xs:anyURI",
        }
        return basyx_map.get(type_name, f"xs:{type_name.lower()}")

    return "xs:string"


def _get_value(element: model.SubmodelElement, preferred_lang: str = "en") -> Any:
    """Extract the value from a submodel element."""
    if isinstance(element, model.Property):
        return element.value
    elif isinstance(element, model.MultiLanguageProperty):
        if element.value:
            # BaSyx v2.0 uses MultiLanguageTextType which is dict-like
            # Try preferred language first
            if preferred_lang in element.value:
                return element.value[preferred_lang]
            # Fall back to first available
            for lang, text in element.value.items():
                return text
        return None
    elif isinstance(element, model.Range):
        # Return as dict for range values
        return {"min": element.min, "max": element.max}
    return None


def _flatten_element(
    element: model.SubmodelElement,
    path_prefix: str,
    aas_source: str,
    timestamp_ms: int,
    preferred_lang: str,
) -> Iterator[ContextMetric]:
    """Recursively flatten a submodel element into metrics.

    Args:
        element: The submodel element to process.
        path_prefix: Dot-separated path to this element.
        aas_source: Source identifier (file path or URL).
        timestamp_ms: Extraction timestamp.
        preferred_lang: Preferred language for MultiLanguageProperty.

    Yields:
        ContextMetric for each leaf element.
    """
    id_short = element.id_short or "unnamed"
    current_path = f"{path_prefix}.{id_short}" if path_prefix else id_short

    # Skip File and Blob types as per requirements
    if isinstance(element, (model.File, model.Blob)):
        logger.debug("Skipping File/Blob element: %s", current_path)
        return

    # Handle collections recursively
    if isinstance(element, model.SubmodelElementCollection):
        for child in element.value or []:
            yield from _flatten_element(
                child, current_path, aas_source, timestamp_ms, preferred_lang
            )
        return

    # Handle lists with index-based paths
    if isinstance(element, model.SubmodelElementList):
        for idx, child in enumerate(element.value or []):
            indexed_path = f"{current_path}[{idx}]"
            # For list items, we process them but use the indexed path
            if isinstance(child, (model.SubmodelElementCollection, model.SubmodelElementList)):
                # Nested structure in list
                for nested in child.value or []:
                    yield from _flatten_element(
                        nested, indexed_path, aas_source, timestamp_ms, preferred_lang
                    )
            else:
                # Leaf element in list
                yield from _flatten_leaf(
                    child, indexed_path, aas_source, timestamp_ms, preferred_lang
                )
        return

    # Handle Range specially - emit min and max as separate metrics
    if isinstance(element, model.Range):
        semantic_id = _extract_semantic_id(element)
        unit = _extract_unit(element)
        value_type = _get_value_type(element)

        if element.min is not None:
            yield ContextMetric(
                path=f"{current_path}.min",
                value=element.min,
                aas_type="Range.min",
                value_type=value_type,
                semantic_id=semantic_id,
                unit=unit,
                aas_source=aas_source,
                timestamp_ms=timestamp_ms,
            )
        if element.max is not None:
            yield ContextMetric(
                path=f"{current_path}.max",
                value=element.max,
                aas_type="Range.max",
                value_type=value_type,
                semantic_id=semantic_id,
                unit=unit,
                aas_source=aas_source,
                timestamp_ms=timestamp_ms,
            )
        return

    # Handle leaf elements (Property, MultiLanguageProperty, etc.)
    yield from _flatten_leaf(element, current_path, aas_source, timestamp_ms, preferred_lang)


def _flatten_leaf(
    element: model.SubmodelElement,
    path: str,
    aas_source: str,
    timestamp_ms: int,
    preferred_lang: str,
) -> Iterator[ContextMetric]:
    """Create a metric from a leaf element."""
    aas_type = type(element).__name__
    value = _get_value(element, preferred_lang)
    value_type = _get_value_type(element)
    semantic_id = _extract_semantic_id(element)
    unit = _extract_unit(element)

    yield ContextMetric(
        path=path,
        value=value,
        aas_type=aas_type,
        value_type=value_type,
        semantic_id=semantic_id,
        unit=unit,
        aas_source=aas_source,
        timestamp_ms=timestamp_ms,
    )


def flatten_submodel(
    submodel: model.Submodel,
    aas_source: str = "",
    preferred_lang: str = "en",
) -> list[ContextMetric]:
    """Flatten a submodel into a list of metrics.

    Recursively traverses the submodel structure and produces a ContextMetric
    for each leaf element (Property, MultiLanguageProperty) and Range bounds.

    Args:
        submodel: The AAS submodel to flatten.
        aas_source: Source identifier for provenance.
        preferred_lang: Preferred language code for MultiLanguageProperty.

    Returns:
        List of ContextMetric objects representing all leaf values.
    """
    timestamp_ms = int(time.time() * 1000)
    metrics: list[ContextMetric] = []
    submodel_path = submodel.id_short or "unnamed"

    for element in submodel.submodel_element or []:
        for metric in _flatten_element(
            element, submodel_path, aas_source, timestamp_ms, preferred_lang
        ):
            metrics.append(metric)

    logger.debug(
        "Flattened submodel %s: %d metrics", submodel.id_short, len(metrics)
    )
    return metrics


def get_global_asset_id(aas: model.AssetAdministrationShell) -> str | None:
    """Extract the globalAssetId from an AAS."""
    if aas.asset_information and aas.asset_information.global_asset_id:
        return aas.asset_information.global_asset_id
    return None


def iter_submodels(
    object_store: model.DictObjectStore,
) -> Iterator[tuple[model.Submodel, str | None]]:
    """Iterate over all submodels in an object store with their asset IDs.

    Yields:
        Tuples of (submodel, global_asset_id) for each submodel.
    """
    # Build mapping of submodel references to asset IDs
    submodel_to_asset: dict[str, str] = {}

    for obj in object_store:
        if isinstance(obj, model.AssetAdministrationShell):
            asset_id = get_global_asset_id(obj)
            if asset_id and obj.submodel:
                for ref in obj.submodel:
                    keys = list(ref.key)
                    if keys:
                        submodel_to_asset[keys[0].value] = asset_id

    # Yield submodels with their asset IDs
    for obj in object_store:
        if isinstance(obj, model.Submodel):
            asset_id = submodel_to_asset.get(obj.id)
            yield obj, asset_id
