"""Topic segment sanitization for MQTT compliance."""

import re
import unicodedata

# Maximum length for a single topic segment
MAX_SEGMENT_LENGTH = 64

# Characters that are invalid in MQTT topic segments
# + and # are wildcards, / is the level separator
INVALID_CHARS = re.compile(r"[+#/\x00]")

# Whitespace pattern for replacement
WHITESPACE = re.compile(r"\s+")


def sanitize_segment(segment: str, max_length: int = MAX_SEGMENT_LENGTH) -> str:
    """Sanitize a single topic segment for MQTT compliance.

    Performs the following transformations:
    1. Normalize Unicode to NFC form
    2. Replace whitespace sequences with underscore
    3. Remove/escape MQTT special characters (+, #, /)
    4. Remove null bytes
    5. Truncate to max length
    6. Strip leading/trailing whitespace and underscores

    Args:
        segment: The raw segment string to sanitize.
        max_length: Maximum length for the segment (default 64).

    Returns:
        Sanitized segment safe for MQTT topics.

    Examples:
        >>> sanitize_segment("My Device Name")
        'My_Device_Name'
        >>> sanitize_segment("Sensor+Temperature")
        'Sensor_Temperature'
        >>> sanitize_segment("Level/SubLevel")
        'Level_SubLevel'
    """
    if not segment:
        return "unnamed"

    # Normalize Unicode
    result = unicodedata.normalize("NFC", segment)

    # Encode to UTF-8 and back to ensure valid UTF-8
    try:
        result = result.encode("utf-8").decode("utf-8")
    except UnicodeError:
        # Replace invalid characters
        result = result.encode("utf-8", errors="replace").decode("utf-8")

    # Replace whitespace with underscore
    result = WHITESPACE.sub("_", result)

    # Replace invalid MQTT characters with underscore
    result = INVALID_CHARS.sub("_", result)

    # Collapse multiple underscores
    result = re.sub(r"_+", "_", result)

    # Strip leading/trailing underscores
    result = result.strip("_")

    # Truncate to max length (preserving valid UTF-8)
    if len(result) > max_length:
        # Truncate carefully to avoid breaking UTF-8 sequences
        truncated = result[:max_length]
        result = truncated.rsplit("_", 1)[0] if "_" in truncated else truncated

    # Ensure non-empty
    return result if result else "unnamed"


def sanitize_topic(topic: str, separator: str = "/") -> str:
    """Sanitize a full topic path.

    Splits on the separator, sanitizes each segment, and rejoins.

    Args:
        topic: The full topic path to sanitize.
        separator: The path separator (default '/').

    Returns:
        Sanitized topic path.

    Examples:
        >>> sanitize_topic("AcmeCorp/Plant A/Line+1/Robot")
        'AcmeCorp/Plant_A/Line_1/Robot'
    """
    if not topic:
        return "unnamed"

    segments = topic.split(separator)
    sanitized = [sanitize_segment(s) for s in segments if s]

    return separator.join(sanitized) if sanitized else "unnamed"


def sanitize_metric_path(path: str) -> str:
    """Sanitize an AAS metric path for topic conversion.

    Converts dot-separated AAS paths to slash-separated topic paths.
    Array indices use a distinguishable format (idx_N) to avoid
    ambiguity with numeric idShorts.

    Args:
        path: Dot-separated AAS element path.

    Returns:
        Slash-separated, sanitized topic path.

    Examples:
        >>> sanitize_metric_path("TechnicalData.GeneralInfo.Manufacturer Name")
        'TechnicalData/GeneralInfo/Manufacturer_Name'
        >>> sanitize_metric_path("List[0].Value")
        'List/idx_0/Value'
        >>> sanitize_metric_path("Config.123.Name")
        'Config/123/Name'
    """
    # Replace dots with slashes
    topic_path = path.replace(".", "/")

    # Handle array indices: convert [0] to /idx_0 (distinguishable from numeric idShorts)
    # Using idx_N format (no leading underscore) to survive sanitization
    topic_path = re.sub(r"\[(\d+)\]", r"/idx_\1", topic_path)

    return sanitize_topic(topic_path)
