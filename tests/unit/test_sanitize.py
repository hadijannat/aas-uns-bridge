"""Unit tests for topic sanitization."""

from aas_uns_bridge.mapping.sanitize import (
    sanitize_metric_path,
    sanitize_segment,
    sanitize_topic,
)


class TestSanitizeSegment:
    """Tests for sanitize_segment function."""

    def test_basic_string(self) -> None:
        """Test that basic strings pass through unchanged."""
        assert sanitize_segment("SimpleString") == "SimpleString"
        assert sanitize_segment("Device001") == "Device001"

    def test_whitespace_to_underscore(self) -> None:
        """Test that whitespace is converted to underscore."""
        assert sanitize_segment("My Device") == "My_Device"
        assert sanitize_segment("Multiple   Spaces") == "Multiple_Spaces"
        assert sanitize_segment("Tab\tHere") == "Tab_Here"

    def test_mqtt_wildcards_removed(self) -> None:
        """Test that MQTT wildcard characters are removed."""
        assert sanitize_segment("Sensor+Temp") == "Sensor_Temp"
        assert sanitize_segment("Level#1") == "Level_1"
        assert sanitize_segment("Path/Part") == "Path_Part"

    def test_null_bytes_removed(self) -> None:
        """Test that null bytes are removed."""
        assert sanitize_segment("Test\x00String") == "Test_String"

    def test_truncation(self) -> None:
        """Test that long strings are truncated."""
        long_string = "A" * 100
        result = sanitize_segment(long_string, max_length=64)
        assert len(result) <= 64

    def test_unicode_preserved(self) -> None:
        """Test that valid Unicode is preserved."""
        assert sanitize_segment("Gerät") == "Gerät"
        assert sanitize_segment("设备") == "设备"
        assert sanitize_segment("センサー") == "センサー"

    def test_empty_string(self) -> None:
        """Test that empty strings become 'unnamed'."""
        assert sanitize_segment("") == "unnamed"
        assert sanitize_segment("   ") == "unnamed"

    def test_multiple_invalid_chars(self) -> None:
        """Test handling of multiple consecutive invalid characters."""
        assert sanitize_segment("A++B##C") == "A_B_C"
        assert sanitize_segment("+++") == "unnamed"

    def test_leading_trailing_underscores_stripped(self) -> None:
        """Test that leading/trailing underscores are stripped."""
        assert sanitize_segment("_Leading") == "Leading"
        assert sanitize_segment("Trailing_") == "Trailing"
        assert sanitize_segment("_Both_") == "Both"


class TestSanitizeTopic:
    """Tests for sanitize_topic function."""

    def test_basic_path(self) -> None:
        """Test that basic paths work correctly."""
        assert sanitize_topic("Level1/Level2/Level3") == "Level1/Level2/Level3"

    def test_segments_sanitized(self) -> None:
        """Test that each segment is sanitized."""
        assert (
            sanitize_topic("Level One/Level+Two/Level#Three") == "Level_One/Level_Two/Level_Three"
        )

    def test_empty_segments_removed(self) -> None:
        """Test that empty segments are removed."""
        assert sanitize_topic("Level1//Level2") == "Level1/Level2"
        assert sanitize_topic("/Leading/Middle/") == "Leading/Middle"

    def test_empty_topic(self) -> None:
        """Test that empty topics become 'unnamed'."""
        assert sanitize_topic("") == "unnamed"
        assert sanitize_topic("/") == "unnamed"


class TestSanitizeMetricPath:
    """Tests for sanitize_metric_path function."""

    def test_dot_to_slash_conversion(self) -> None:
        """Test that dots are converted to slashes."""
        assert sanitize_metric_path("Submodel.Element.Property") == "Submodel/Element/Property"

    def test_array_index_conversion(self) -> None:
        """Test that array indices are converted to path segments."""
        assert sanitize_metric_path("List[0].Value") == "List/0/Value"
        assert sanitize_metric_path("Collection.Items[5]") == "Collection/Items/5"

    def test_combined_sanitization(self) -> None:
        """Test that all transformations are applied."""
        path = "Technical Data.General Info[0].Manufacturer Name"
        expected = "Technical_Data/General_Info/0/Manufacturer_Name"
        assert sanitize_metric_path(path) == expected
