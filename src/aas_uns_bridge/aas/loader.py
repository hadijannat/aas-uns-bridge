"""AAS file loaders for AASX packages and JSON files."""

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from basyx.aas import model
from basyx.aas.adapter import aasx
from basyx.aas.adapter import json as aas_json

logger = logging.getLogger(__name__)


class AASLoadError(Exception):
    """Raised when AAS content cannot be loaded."""

    pass


def load_aasx(path: Path) -> model.DictObjectStore[model.Identifiable]:
    """Load an AASX package file.

    Args:
        path: Path to the .aasx file.

    Returns:
        ObjectStore containing all AAS objects from the package.

    Raises:
        AASLoadError: If the file cannot be loaded or parsed.
    """
    if not path.exists():
        raise AASLoadError(f"AASX file not found: {path}")

    try:
        object_store: model.DictObjectStore[model.Identifiable] = model.DictObjectStore()
        file_store = aasx.DictSupplementaryFileContainer()  # type: ignore[no-untyped-call]
        with aasx.AASXReader(str(path)) as reader:
            reader.read_into(object_store, file_store)
        logger.info("Loaded AASX: %s (%d objects)", path.name, len(object_store))
        return object_store
    except Exception as e:
        raise AASLoadError(f"Failed to load AASX {path}: {e}") from e


def load_json(path: Path) -> model.DictObjectStore[model.Identifiable]:
    """Load an AAS JSON file.

    Args:
        path: Path to the .json file containing AAS content.

    Returns:
        ObjectStore containing all AAS objects from the file.

    Raises:
        AASLoadError: If the file cannot be loaded or parsed.
    """
    if not path.exists():
        raise AASLoadError(f"JSON file not found: {path}")

    try:
        with open(path) as f:
            data = json.load(f)

        object_store: model.DictObjectStore[model.Identifiable] = model.DictObjectStore()
        read_aas_json_file_into = cast(
            Callable[[Any, str], Any],
            aas_json.read_aas_json_file_into,  # type: ignore[attr-defined]
        )

        # Handle both single-object and collection formats
        if isinstance(data, dict):
            if "assetAdministrationShells" in data or "submodels" in data:
                # Environment format
                read_aas_json_file_into(object_store, str(path))
            else:
                # Try as single object
                read_aas_json_file_into(object_store, str(path))
        elif isinstance(data, list):
            # List of objects
            read_aas_json_file_into(object_store, str(path))
        else:
            raise AASLoadError(f"Unexpected JSON structure in {path}")

        logger.info("Loaded JSON: %s (%d objects)", path.name, len(object_store))
        return object_store
    except json.JSONDecodeError as e:
        raise AASLoadError(f"Invalid JSON in {path}: {e}") from e
    except Exception as e:
        raise AASLoadError(f"Failed to load JSON {path}: {e}") from e


def load_file(path: Path) -> model.DictObjectStore[model.Identifiable]:
    """Load an AAS file (AASX or JSON) based on extension.

    Args:
        path: Path to the AAS file.

    Returns:
        ObjectStore containing all AAS objects.

    Raises:
        AASLoadError: If the file type is unknown or loading fails.
    """
    suffix = path.suffix.lower()
    if suffix == ".aasx":
        return load_aasx(path)
    elif suffix == ".json":
        return load_json(path)
    else:
        raise AASLoadError(f"Unknown file type: {suffix}")
