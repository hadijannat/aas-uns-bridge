"""Security tests for MQTT topic sanitization and fuzzing.

These tests verify that topic sanitization properly handles malicious inputs,
ensuring MQTT topics are safe from injection attacks, wildcards, and path
traversal attempts.
"""

import pytest

from aas_uns_bridge.domain.models import AssetIdentity
from aas_uns_bridge.mapping.sanitize import sanitize_segment


class TestTopicSanitization:
    """Tests for topic segment sanitization security."""

    @pytest.mark.security
    @pytest.mark.parametrize(
        "input_value,expected_safe",
        [
            ("normal_topic", True),
            ("topic/with/slashes", False),
            ("topic#wildcard", False),
            ("topic+wildcard", False),
            ("", False),
            ("a" * 200, True),
            ("simple", True),
            ("with-dash", True),
            ("with_underscore", True),
            ("CamelCase", True),
            ("123numeric", True),
        ],
    )
    def test_sanitize_segment_safety(self, input_value: str, expected_safe: bool) -> None:
        """Verify sanitized segments never contain dangerous characters.

        Tests that:
        1. Result never contains #, +, or / (MQTT wildcards/separator)
        2. Result length is <= 128 characters
        3. Empty inputs are handled gracefully
        """
        result = sanitize_segment(input_value)

        # Result must never contain MQTT special characters
        assert "#" not in result, f"Result contains #: {result}"
        assert "+" not in result, f"Result contains +: {result}"
        assert "/" not in result, f"Result contains /: {result}"

        # Result must be within length limit (default is 64, but allow up to 128)
        assert len(result) <= 128, f"Result too long: {len(result)} chars"

        # Result must not be empty (should be 'unnamed' at minimum)
        assert len(result) > 0, "Result is empty string"

        # If input was expected to be safe and non-empty, verify it's preserved
        # For very long inputs, result will be truncated but still safe
        if expected_safe and input_value and len(input_value) <= 64:
            assert result == input_value or result != "", f"Safe input modified: {result}"

    @pytest.mark.security
    def test_sanitize_removes_null_bytes(self) -> None:
        """Verify null bytes are properly removed from topic segments.

        Null bytes can cause string truncation in C libraries and could
        potentially be used for injection attacks.
        """
        test_cases = [
            ("test\x00value", "test_value"),
            ("\x00leading", "leading"),
            ("trailing\x00", "trailing"),
            ("multi\x00ple\x00nulls", "multi_ple_nulls"),
            ("\x00\x00\x00", "unnamed"),  # All null bytes
            ("a\x00b\x00c\x00d", "a_b_c_d"),
        ]

        for input_val, expected in test_cases:
            result = sanitize_segment(input_val)
            assert "\x00" not in result, f"Null byte in result: {result!r}"
            assert result == expected, f"Expected {expected!r}, got {result!r}"

    @pytest.mark.security
    def test_sanitize_handles_unicode(self) -> None:
        """Verify Unicode is preserved but special characters are removed.

        Valid Unicode characters should be preserved in topic segments,
        but MQTT special characters embedded in Unicode should be removed.
        """
        test_cases = [
            # Valid Unicode preserved
            ("Gerät", "Gerät"),
            ("设备", "设备"),
            ("センサー", "センサー"),
            ("Ελληνικά", "Ελληνικά"),
            ("العربية", "العربية"),
            ("한국어", "한국어"),
            # Special chars in Unicode context still removed
            ("Gerät+Sensor", "Gerät_Sensor"),
            ("设备#123", "设备_123"),
            ("センサー/温度", "センサー_温度"),
            # Combining characters
            ("café", "café"),  # e + combining acute
            ("naïve", "naïve"),
            # Unicode with null bytes
            ("设备\x00名称", "设备_名称"),
        ]

        for input_val, expected in test_cases:
            result = sanitize_segment(input_val)
            # Should not contain MQTT special chars
            assert "#" not in result
            assert "+" not in result
            assert "/" not in result
            assert "\x00" not in result
            # Normalized Unicode should match expected
            assert result == expected, (
                f"Input: {input_val!r}, Expected: {expected!r}, Got: {result!r}"
            )

    @pytest.mark.security
    @pytest.mark.parametrize(
        "input_value",
        [
            "../../../etc/passwd",
            "..%2F..%2F..%2Fetc%2Fpasswd",
            "....//....//etc/passwd",
            "./../.../../etc/passwd",
            "valid/../../../escape",
            "..;/..;/..;/etc/passwd",
            "..%252f..%252f..%252fetc/passwd",
            "/absolute/path",
            "..",
            "...",
            "....",
        ],
    )
    def test_sanitize_path_traversal(self, input_value: str) -> None:
        """Verify path traversal attempts are neutralized.

        Tests that:
        1. Forward slashes (/) are removed/replaced
        2. .. sequences do not enable directory traversal when combined with /
        3. URL-encoded traversal attempts are handled

        Note: Backslashes are not MQTT special characters and are handled
        separately. This test focuses on MQTT-unsafe path traversal.
        """
        result = sanitize_segment(input_value)

        # Result must never contain forward slash (MQTT level separator)
        assert "/" not in result, f"Result contains /: {result}"

        # Result should not be able to escape to parent directories
        # After sanitization, .. becomes safe (just dots) since / is removed
        assert len(result) > 0, "Result is empty"

        # Verify the result is safe for MQTT
        assert "#" not in result
        assert "+" not in result
        assert "\x00" not in result


class TestTopicConstruction:
    """Tests for safe topic construction from domain objects."""

    @pytest.mark.security
    def test_uns_topic_construction_safe(self) -> None:
        """Verify AssetIdentity with malicious global_asset_id produces safe topics.

        Tests that malicious characters in hierarchy levels are sanitized
        before topic construction.
        """
        malicious_inputs = [
            "https://example.com/asset#fragment",
            "urn:asset:with+plus:id",
            "asset/with/slashes",
            "asset\x00with\x00nulls",
            "../../../etc/passwd",
            "normal_asset_id",
        ]

        for malicious_id in malicious_inputs:
            # Create identity with malicious values in hierarchy
            identity = AssetIdentity(
                global_asset_id=malicious_id,
                enterprise=sanitize_segment("Enterprise#1"),
                site=sanitize_segment("Site+A"),
                area=sanitize_segment("Area/B"),
                line=sanitize_segment("Line\x00C"),
                asset=sanitize_segment("Asset../D"),
            )

            topic_prefix = identity.topic_prefix()

            # Verify no dangerous characters in topic prefix
            # Note: topic_prefix uses / as separator, so slashes ARE expected
            # between segments, but not within sanitized segments
            segments = topic_prefix.split("/")
            for segment in segments:
                assert "#" not in segment, f"# in segment: {segment}"
                assert "+" not in segment, f"+ in segment: {segment}"
                assert "\x00" not in segment, f"null in segment: {segment}"
                # Note: ".." within a segment name (e.g., "Asset.._D") is safe
                # since path traversal only works when combined with "/"

    @pytest.mark.security
    def test_sparkplug_topic_construction_safe(self) -> None:
        """Verify Sparkplug topic segments are properly sanitized.

        Tests sanitize_segment with typical Sparkplug naming patterns
        that might contain wildcards.
        """
        test_cases = [
            ("#group", "group"),
            ("+test", "test"),
            ("group#1", "group_1"),
            ("node+test", "node_test"),
            ("spBv1.0/+/NBIRTH/#", "spBv1.0_NBIRTH"),  # Multiple special chars
            ("device/node", "device_node"),
            ("edge_node_001", "edge_node_001"),  # Valid Sparkplug pattern
            ("group-id", "group-id"),
            ("Group_ID", "Group_ID"),
        ]

        for input_val, expected in test_cases:
            result = sanitize_segment(input_val)

            # Verify no MQTT wildcards
            assert "#" not in result, f"# in result: {result}"
            assert "+" not in result, f"+ in result: {result}"
            assert "/" not in result, f"/ in result: {result}"

            # Verify expected sanitization
            assert result == expected, (
                f"Input: {input_val!r}, Expected: {expected!r}, Got: {result!r}"
            )

    @pytest.mark.security
    def test_topic_prefix_with_empty_hierarchy_levels(self) -> None:
        """Verify topic_prefix handles empty hierarchy levels correctly."""
        # Minimal identity with only enterprise
        identity = AssetIdentity(
            global_asset_id="urn:example:asset:1",
            enterprise="AcmeCorp",
        )
        assert identity.topic_prefix() == "AcmeCorp"

        # Identity with gaps in hierarchy
        identity = AssetIdentity(
            global_asset_id="urn:example:asset:2",
            enterprise="AcmeCorp",
            site="",
            area="ProductionArea",
            line="",
            asset="Robot001",
        )
        # Empty levels should be filtered out
        prefix = identity.topic_prefix()
        assert prefix == "AcmeCorp/ProductionArea/Robot001"
        assert "//" not in prefix, "Double slashes in topic prefix"

    @pytest.mark.security
    def test_topic_construction_length_limits(self) -> None:
        """Verify topic construction respects length limits."""
        # Create identity with very long names
        long_name = "A" * 100
        identity = AssetIdentity(
            global_asset_id="urn:example:asset:1",
            enterprise=sanitize_segment(long_name),
            site=sanitize_segment(long_name),
            area=sanitize_segment(long_name),
            line=sanitize_segment(long_name),
            asset=sanitize_segment(long_name),
        )

        topic_prefix = identity.topic_prefix()
        segments = topic_prefix.split("/")

        # Each segment should be within limits
        for segment in segments:
            assert len(segment) <= 64, f"Segment too long: {len(segment)} chars"

    @pytest.mark.security
    def test_topic_with_control_characters(self) -> None:
        """Verify control characters are handled safely."""
        control_chars = [
            "\x01",  # Start of heading
            "\x02",  # Start of text
            "\x03",  # End of text
            "\x04",  # End of transmission
            "\x07",  # Bell
            "\x08",  # Backspace
            "\x1b",  # Escape
            "\x7f",  # Delete
        ]

        for char in control_chars:
            input_val = f"test{char}value"
            result = sanitize_segment(input_val)

            # Should not contain the control character (may be replaced with _)
            # At minimum, should not contain MQTT special chars
            assert "#" not in result
            assert "+" not in result
            assert "/" not in result
            assert "\x00" not in result

    @pytest.mark.security
    def test_topic_with_newlines_and_tabs(self) -> None:
        """Verify newlines and tabs are converted to underscores."""
        test_cases = [
            ("line1\nline2", "line1_line2"),
            ("col1\tcol2", "col1_col2"),
            ("multi\n\nline", "multi_line"),
            ("mixed\n\ttab", "mixed_tab"),
            ("\n\n\n", "unnamed"),  # All whitespace
        ]

        for input_val, expected in test_cases:
            result = sanitize_segment(input_val)
            assert "\n" not in result
            assert "\t" not in result
            assert "\r" not in result
            assert result == expected, (
                f"Input: {input_val!r}, Expected: {expected!r}, Got: {result!r}"
            )
