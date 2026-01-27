"""Unit tests for AAS submodel traversal."""

from basyx.aas import model
from basyx.aas.model.base import MultiLanguageTextType

from aas_uns_bridge.aas.traversal import _ref_to_string, flatten_submodel, get_global_asset_id


def create_property(id_short: str, value: str, value_type: type = str) -> model.Property:
    """Helper to create a Property element."""
    return model.Property(
        id_short=id_short,
        value_type=value_type,
        value=value,
    )


def create_multilang_property(id_short: str, values: dict[str, str]) -> model.MultiLanguageProperty:
    """Helper to create a MultiLanguageProperty element."""
    lang_text = MultiLanguageTextType(values)
    return model.MultiLanguageProperty(
        id_short=id_short,
        value=lang_text,
    )


def create_collection(
    id_short: str, elements: list[model.SubmodelElement]
) -> model.SubmodelElementCollection:
    """Helper to create a SubmodelElementCollection."""
    return model.SubmodelElementCollection(
        id_short=id_short,
        value=elements,
    )


def create_range(id_short: str, min_val: float, max_val: float) -> model.Range:
    """Helper to create a Range element."""
    return model.Range(
        id_short=id_short,
        value_type=float,
        min=min_val,
        max=max_val,
    )


class TestFlattenSubmodel:
    """Tests for flatten_submodel function."""

    def test_single_property(self) -> None:
        """Test flattening a submodel with a single property."""
        submodel = model.Submodel(
            id_="https://example.com/sm/test",
            id_short="TestSubmodel",
            submodel_element=[
                create_property("Manufacturer", "Acme Corp"),
            ],
        )

        metrics = flatten_submodel(submodel, aas_source="test.aasx")

        assert len(metrics) == 1
        assert metrics[0].path == "TestSubmodel.Manufacturer"
        assert metrics[0].value == "Acme Corp"
        assert metrics[0].aas_type == "Property"
        assert metrics[0].value_type == "xs:string"
        assert metrics[0].aas_source == "test.aasx"

    def test_nested_collection(self) -> None:
        """Test flattening nested SubmodelElementCollections."""
        submodel = model.Submodel(
            id_="https://example.com/sm/nested",
            id_short="TechnicalData",
            submodel_element=[
                create_collection(
                    "GeneralInformation",
                    [
                        create_property("ManufacturerName", "Acme"),
                        create_collection(
                            "Address",
                            [
                                create_property("Street", "123 Main St"),
                                create_property("City", "Springfield"),
                            ],
                        ),
                    ],
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 3
        paths = [m.path for m in metrics]
        assert "TechnicalData.GeneralInformation.ManufacturerName" in paths
        assert "TechnicalData.GeneralInformation.Address.Street" in paths
        assert "TechnicalData.GeneralInformation.Address.City" in paths

    def test_multilanguage_property_preferred_lang(self) -> None:
        """Test that preferred language is extracted from MultiLanguageProperty."""
        submodel = model.Submodel(
            id_="https://example.com/sm/ml",
            id_short="Nameplate",
            submodel_element=[
                create_multilang_property(
                    "ProductName",
                    {
                        "en": "Robot Arm",
                        "de": "Roboterarm",
                    },
                ),
            ],
        )

        # Test English preference
        metrics = flatten_submodel(submodel, preferred_lang="en")
        assert len(metrics) == 1
        assert metrics[0].value == "Robot Arm"

        # Test German preference
        metrics = flatten_submodel(submodel, preferred_lang="de")
        assert len(metrics) == 1
        assert metrics[0].value == "Roboterarm"

    def test_multilanguage_property_fallback(self) -> None:
        """Test fallback when preferred language is not available."""
        submodel = model.Submodel(
            id_="https://example.com/sm/ml",
            id_short="Nameplate",
            submodel_element=[
                create_multilang_property(
                    "ProductName",
                    {
                        "de": "Roboterarm",
                    },
                ),
            ],
        )

        # Prefer English but only German available - should fall back
        metrics = flatten_submodel(submodel, preferred_lang="en")
        assert len(metrics) == 1
        assert metrics[0].value == "Roboterarm"

    def test_range_emits_min_max(self) -> None:
        """Test that Range elements emit separate min and max metrics."""
        submodel = model.Submodel(
            id_="https://example.com/sm/range",
            id_short="Specifications",
            submodel_element=[
                create_range("Temperature", -20.0, 80.0),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 2
        paths = {m.path: m.value for m in metrics}
        assert paths["Specifications.Temperature.min"] == -20.0
        assert paths["Specifications.Temperature.max"] == 80.0

    def test_submodel_element_list(self) -> None:
        """Test flattening SubmodelElementList with indexed paths."""
        # SubmodelElementList items must NOT have id_short (AASd-120)
        list_elements = [
            model.Property(id_short=None, value_type=str, value="First"),
            model.Property(id_short=None, value_type=str, value="Second"),
        ]
        submodel = model.Submodel(
            id_="https://example.com/sm/list",
            id_short="Config",
            submodel_element=[
                model.SubmodelElementList(
                    id_short="Items",
                    type_value_list_element=model.Property,
                    value_type_list_element=str,
                    value=list_elements,
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 2
        paths = [m.path for m in metrics]
        assert "Config.Items[0]" in paths
        assert "Config.Items[1]" in paths

    def test_file_and_blob_skipped(self) -> None:
        """Test that File and Blob elements are skipped."""
        submodel = model.Submodel(
            id_="https://example.com/sm/media",
            id_short="Documentation",
            submodel_element=[
                create_property("Title", "Manual"),
                model.File(id_short="Manual", content_type="application/pdf"),
                model.Blob(id_short="Icon", content_type="image/png"),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 1
        assert metrics[0].path == "Documentation.Title"

    def test_numeric_value_types(self) -> None:
        """Test that numeric value types are correctly identified."""
        submodel = model.Submodel(
            id_="https://example.com/sm/nums",
            id_short="Measurements",
            submodel_element=[
                model.Property(
                    id_short="Temperature",
                    value_type=float,
                    value=25.5,
                ),
                model.Property(
                    id_short="Count",
                    value_type=int,
                    value=42,
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        temp_metric = next(m for m in metrics if "Temperature" in m.path)
        count_metric = next(m for m in metrics if "Count" in m.path)

        assert temp_metric.value_type == "xs:double"
        assert temp_metric.value == 25.5
        assert count_metric.value_type == "xs:int"
        assert count_metric.value == 42

    def test_timestamp_set(self) -> None:
        """Test that timestamp is set on all metrics."""
        submodel = model.Submodel(
            id_="https://example.com/sm/ts",
            id_short="Test",
            submodel_element=[
                create_property("Value", "test"),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 1
        assert metrics[0].timestamp_ms > 0


class TestGetGlobalAssetId:
    """Tests for get_global_asset_id function."""

    def test_extracts_global_asset_id(self) -> None:
        """Test extraction of globalAssetId from AAS."""
        aas = model.AssetAdministrationShell(
            id_="https://example.com/aas/test",
            id_short="TestAAS",
            asset_information=model.AssetInformation(
                asset_kind=model.AssetKind.INSTANCE,
                global_asset_id="https://example.com/asset/robot-001",
            ),
        )

        asset_id = get_global_asset_id(aas)

        assert asset_id == "https://example.com/asset/robot-001"

    def test_returns_none_if_missing(self) -> None:
        """Test that None is returned when globalAssetId is not set.

        Note: BaSyx v2.0 requires either globalAssetId or specificAssetId (AASd-131),
        so we use specificAssetId instead to test the missing globalAssetId case.
        """
        aas = model.AssetAdministrationShell(
            id_="https://example.com/aas/test",
            id_short="TestAAS",
            asset_information=model.AssetInformation(
                asset_kind=model.AssetKind.INSTANCE,
                specific_asset_id=[
                    model.SpecificAssetId(
                        name="serialNumber",
                        value="12345",
                    )
                ],
            ),
        )

        asset_id = get_global_asset_id(aas)

        assert asset_id is None


class TestReferenceElement:
    """Tests for ReferenceElement flattening."""

    def test_reference_element_flattening(self) -> None:
        """Test that ReferenceElement is flattened with reference path as value."""
        # Create a reference pointing to another element using BaSyx v2.0 API
        ref = model.ModelReference(
            key=(
                model.Key(model.KeyTypes.SUBMODEL, "https://example.com/sm/target"),
                model.Key(model.KeyTypes.PROPERTY, "TargetProperty"),
            ),
            type_=model.Submodel,
        )
        submodel = model.Submodel(
            id_="https://example.com/sm/ref",
            id_short="References",
            submodel_element=[
                model.ReferenceElement(
                    id_short="PointsTo",
                    value=ref,
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 1
        assert metrics[0].path == "References.PointsTo"
        assert metrics[0].value == "https://example.com/sm/target/TargetProperty"
        assert metrics[0].aas_type == "ReferenceElement"
        assert metrics[0].value_type == "xs:string"

    def test_reference_element_empty_reference(self) -> None:
        """Test that ReferenceElement with no value produces None."""
        submodel = model.Submodel(
            id_="https://example.com/sm/ref",
            id_short="References",
            submodel_element=[
                model.ReferenceElement(
                    id_short="EmptyRef",
                    value=None,
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 1
        assert metrics[0].value is None
        assert metrics[0].aas_type == "ReferenceElement"


class TestEntityElement:
    """Tests for Entity element flattening."""

    def test_entity_flattening_self_managed(self) -> None:
        """Test that self-managed Entity emits entityType and globalAssetId."""
        # AASd-014: SELF_MANAGED_ENTITY must have globalAssetId or specificAssetId
        submodel = model.Submodel(
            id_="https://example.com/sm/entity",
            id_short="Assets",
            submodel_element=[
                model.Entity(
                    id_short="LinkedAsset",
                    entity_type=model.EntityType.SELF_MANAGED_ENTITY,
                    global_asset_id="https://example.com/asset/linked-001",
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 2
        paths = {m.path: m for m in metrics}
        assert "Assets.LinkedAsset.entityType" in paths
        assert "Assets.LinkedAsset.globalAssetId" in paths
        assert paths["Assets.LinkedAsset.entityType"].value == "SELF_MANAGED_ENTITY"
        assert (
            paths["Assets.LinkedAsset.globalAssetId"].value
            == "https://example.com/asset/linked-001"
        )
        assert paths["Assets.LinkedAsset.entityType"].aas_type == "Entity.entityType"
        assert paths["Assets.LinkedAsset.globalAssetId"].aas_type == "Entity.globalAssetId"

    def test_entity_with_statements(self) -> None:
        """Test that Entity statements (nested elements) are recursively flattened."""
        submodel = model.Submodel(
            id_="https://example.com/sm/entity",
            id_short="Assets",
            submodel_element=[
                model.Entity(
                    id_short="Component",
                    entity_type=model.EntityType.SELF_MANAGED_ENTITY,
                    global_asset_id="https://example.com/asset/component-001",
                    statement=[
                        model.Property(
                            id_short="SerialNumber",
                            value_type=str,
                            value="SN12345",
                        ),
                        model.Property(
                            id_short="Status",
                            value_type=str,
                            value="Active",
                        ),
                    ],
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        # Should have: entityType, globalAssetId, SerialNumber, Status
        assert len(metrics) == 4
        paths = {m.path: m.value for m in metrics}
        assert paths["Assets.Component.entityType"] == "SELF_MANAGED_ENTITY"
        assert paths["Assets.Component.globalAssetId"] == "https://example.com/asset/component-001"
        assert paths["Assets.Component.SerialNumber.SerialNumber"] == "SN12345"
        assert paths["Assets.Component.Status.Status"] == "Active"

    def test_entity_co_managed_without_asset_id(self) -> None:
        """Test CO_MANAGED_ENTITY without globalAssetId only emits entityType."""
        # AASd-014: CO_MANAGED_ENTITY must NOT have globalAssetId or specificAssetId
        submodel = model.Submodel(
            id_="https://example.com/sm/entity",
            id_short="Assets",
            submodel_element=[
                model.Entity(
                    id_short="LocalEntity",
                    entity_type=model.EntityType.CO_MANAGED_ENTITY,
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 1
        assert metrics[0].path == "Assets.LocalEntity.entityType"
        assert metrics[0].value == "CO_MANAGED_ENTITY"


class TestRelationshipElement:
    """Tests for RelationshipElement flattening."""

    def test_relationship_element_flattening(self) -> None:
        """Test that RelationshipElement is flattened with first -> second format."""
        first_ref = model.ModelReference(
            key=(model.Key(model.KeyTypes.SUBMODEL, "https://example.com/sm/source"),),
            type_=model.Submodel,
        )
        second_ref = model.ModelReference(
            key=(model.Key(model.KeyTypes.SUBMODEL, "https://example.com/sm/target"),),
            type_=model.Submodel,
        )
        submodel = model.Submodel(
            id_="https://example.com/sm/rel",
            id_short="Relations",
            submodel_element=[
                model.RelationshipElement(
                    id_short="ComponentConnection",
                    first=first_ref,
                    second=second_ref,
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 1
        assert metrics[0].path == "Relations.ComponentConnection"
        assert metrics[0].value == "https://example.com/sm/source -> https://example.com/sm/target"
        assert metrics[0].aas_type == "RelationshipElement"
        assert metrics[0].value_type == "xs:string"

    def test_relationship_element_multikey_references(self) -> None:
        """Test RelationshipElement with multi-key references."""
        first_ref = model.ModelReference(
            key=(
                model.Key(model.KeyTypes.SUBMODEL, "SM1"),
                model.Key(model.KeyTypes.PROPERTY, "PropA"),
            ),
            type_=model.Submodel,
        )
        second_ref = model.ModelReference(
            key=(
                model.Key(model.KeyTypes.SUBMODEL, "SM2"),
                model.Key(model.KeyTypes.PROPERTY, "PropB"),
            ),
            type_=model.Submodel,
        )
        submodel = model.Submodel(
            id_="https://example.com/sm/rel",
            id_short="Relations",
            submodel_element=[
                model.RelationshipElement(
                    id_short="Link",
                    first=first_ref,
                    second=second_ref,
                ),
            ],
        )

        metrics = flatten_submodel(submodel)

        assert len(metrics) == 1
        assert metrics[0].value == "SM1/PropA -> SM2/PropB"


class TestRefToString:
    """Tests for _ref_to_string helper function."""

    def test_ref_to_string_single_key(self) -> None:
        """Test converting reference with single key."""
        ref = model.ModelReference(
            key=(model.Key(model.KeyTypes.SUBMODEL, "SM1"),),
            type_=model.Submodel,
        )
        assert _ref_to_string(ref) == "SM1"

    def test_ref_to_string_multiple_keys(self) -> None:
        """Test converting reference with multiple keys."""
        ref = model.ModelReference(
            key=(
                model.Key(model.KeyTypes.SUBMODEL, "SM1"),
                model.Key(model.KeyTypes.SUBMODEL_ELEMENT_COLLECTION, "Collection"),
                model.Key(model.KeyTypes.PROPERTY, "Prop"),
            ),
            type_=model.Submodel,
        )
        assert _ref_to_string(ref) == "SM1/Collection/Prop"

    def test_ref_to_string_none(self) -> None:
        """Test that None reference returns None."""
        assert _ref_to_string(None) is None
