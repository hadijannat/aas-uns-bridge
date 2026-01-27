#!/usr/bin/env python3
"""Generate test fixtures for AAS-UNS Bridge verification harness.

This script generates:
1. Sample AASX files from JSON fixtures
2. Large AAS JSON for load testing (5000+ properties)
"""

import json
from pathlib import Path

# Constants
FIXTURES_DIR = Path(__file__).parent


def generate_large_aas(num_properties: int = 5000) -> dict:
    """Generate a large AAS with many properties for load testing.

    Args:
        num_properties: Number of properties to generate.

    Returns:
        AAS environment dict.
    """
    properties = []
    for i in range(num_properties):
        # BaSyx requires string values in JSON
        value_type = ["xs:double", "xs:int", "xs:string"][i % 3]
        if i % 3 == 0:
            value = str(float(i) * 1.5)
        elif i % 3 == 1:
            value = str(i)
        else:
            value = f"value_{i}"

        prop: dict = {
            "idShort": f"Property{i:05d}",
            "modelType": "Property",
            "valueType": value_type,
            "value": value,
        }

        # Add semantic ID to ~50% of properties
        if i % 2 == 0:
            prop["semanticId"] = {
                "type": "ExternalReference",
                "keys": [
                    {
                        "type": "GlobalReference",
                        "value": f"0173-1#02-AAB{i % 1000:03d}#001",
                    }
                ],
            }

        properties.append(prop)

    # Group properties into collections of 50 each
    collections = []
    for chunk_idx in range(0, num_properties, 50):
        chunk_properties = properties[chunk_idx : chunk_idx + 50]
        collections.append(
            {
                "idShort": f"Group{chunk_idx // 50:03d}",
                "modelType": "SubmodelElementCollection",
                "value": chunk_properties,
            }
        )

    return {
        "assetAdministrationShells": [
            {
                "modelType": "AssetAdministrationShell",
                "idShort": "LoadTestAAS",
                "id": "https://example.com/aas/load-test-aas",
                "assetInformation": {
                    "assetKind": "Instance",
                    "globalAssetId": "https://example.com/aas/load-test-asset",
                },
                "submodels": [
                    {
                        "type": "ModelReference",
                        "keys": [
                            {
                                "type": "Submodel",
                                "value": "https://example.com/submodel/load-test",
                            }
                        ],
                    }
                ],
            }
        ],
        "submodels": [
            {
                "modelType": "Submodel",
                "idShort": "LoadTestData",
                "id": "https://example.com/submodel/load-test",
                "submodelElements": collections,
            }
        ],
        "conceptDescriptions": [],
    }


def generate_aasx_from_json(json_path: Path, aasx_path: Path) -> None:
    """Generate an AASX package from a JSON AAS file.

    Uses the BaSyx SDK to create proper AASX files.

    Args:
        json_path: Path to source JSON file.
        aasx_path: Path for output AASX file.
    """
    try:
        from basyx.aas import model
        from basyx.aas.adapter import aasx
        from basyx.aas.adapter import json as aas_json

        # Load JSON into object store
        object_store: model.DictObjectStore = model.DictObjectStore()
        file_store = aasx.DictSupplementaryFileContainer()
        aas_json.read_aas_json_file_into(object_store, str(json_path))

        # Write to AASX
        with aasx.AASXWriter(str(aasx_path)) as writer:
            writer.write_all_aas_objects("/aas", object_store, file_store)

        print(f"Generated AASX: {aasx_path}")
    except ImportError:
        print(f"Skipping AASX generation (BaSyx SDK not available): {aasx_path}")
    except Exception as e:
        print(f"Error generating AASX {aasx_path}: {e}")


def main() -> None:
    """Generate all test fixtures."""
    print("Generating test fixtures...")

    # Generate large AAS for load testing
    large_aas_path = FIXTURES_DIR / "large_aas_5k_properties.json"
    print(f"Generating large AAS with 5000 properties: {large_aas_path}")
    large_aas = generate_large_aas(5000)
    with open(large_aas_path, "w") as f:
        json.dump(large_aas, f, indent=2)
    print(f"Generated: {large_aas_path}")

    # Generate AASX files from JSON fixtures
    json_files = [
        "sample_robot.json",
        "sample_sensor.json",
    ]

    for json_file in json_files:
        json_path = FIXTURES_DIR / json_file
        aasx_path = FIXTURES_DIR / json_file.replace(".json", ".aasx")
        if json_path.exists():
            generate_aasx_from_json(json_path, aasx_path)

    print("Done generating fixtures!")


if __name__ == "__main__":
    main()
