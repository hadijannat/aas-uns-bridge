"""Sparkplug B data types and constants."""

from enum import IntEnum
from typing import Any


class SparkplugDataType(IntEnum):
    """Sparkplug B data types as defined in the specification."""

    Unknown = 0
    Int8 = 1
    Int16 = 2
    Int32 = 3
    Int64 = 4
    UInt8 = 5
    UInt16 = 6
    UInt32 = 7
    UInt64 = 8
    Float = 9
    Double = 10
    Boolean = 11
    String = 12
    DateTime = 13
    Text = 14
    UUID = 15
    DataSet = 16
    Bytes = 17
    File = 18
    Template = 19
    PropertySet = 20
    PropertySetList = 21


class SparkplugMessageType:
    """Sparkplug B message types."""

    NBIRTH = "NBIRTH"
    NDEATH = "NDEATH"
    DBIRTH = "DBIRTH"
    DDEATH = "DDEATH"
    NDATA = "NDATA"
    DDATA = "DDATA"
    NCMD = "NCMD"
    DCMD = "DCMD"
    STATE = "STATE"


# XSD type to Sparkplug data type mapping
XSD_TO_SPARKPLUG: dict[str, SparkplugDataType] = {
    "xs:string": SparkplugDataType.String,
    "xs:boolean": SparkplugDataType.Boolean,
    "xs:int": SparkplugDataType.Int32,
    "xs:integer": SparkplugDataType.Int64,
    "xs:long": SparkplugDataType.Int64,
    "xs:short": SparkplugDataType.Int16,
    "xs:byte": SparkplugDataType.Int8,
    "xs:unsignedint": SparkplugDataType.UInt32,
    "xs:unsignedlong": SparkplugDataType.UInt64,
    "xs:unsignedshort": SparkplugDataType.UInt16,
    "xs:unsignedbyte": SparkplugDataType.UInt8,
    "xs:float": SparkplugDataType.Float,
    "xs:double": SparkplugDataType.Double,
    "xs:datetime": SparkplugDataType.DateTime,
    "xs:date": SparkplugDataType.DateTime,
    "xs:time": SparkplugDataType.DateTime,
    "xs:decimal": SparkplugDataType.Double,
    "xs:base64binary": SparkplugDataType.Bytes,
    "xs:hexbinary": SparkplugDataType.Bytes,
    "xs:anyuri": SparkplugDataType.String,
}


def xsd_to_sparkplug_type(xsd_type: str) -> SparkplugDataType:
    """Convert XSD type string to Sparkplug data type.

    Args:
        xsd_type: XSD type string (e.g., 'xs:string', 'xs:int').

    Returns:
        Corresponding Sparkplug data type.
    """
    return XSD_TO_SPARKPLUG.get(xsd_type.lower(), SparkplugDataType.String)


def python_to_sparkplug_type(value: Any) -> SparkplugDataType:
    """Infer Sparkplug data type from Python value.

    Args:
        value: The Python value.

    Returns:
        Corresponding Sparkplug data type.
    """
    if value is None:
        return SparkplugDataType.Unknown
    if isinstance(value, bool):
        return SparkplugDataType.Boolean
    if isinstance(value, int):
        if -2147483648 <= value <= 2147483647:
            return SparkplugDataType.Int32
        return SparkplugDataType.Int64
    if isinstance(value, float):
        return SparkplugDataType.Double
    if isinstance(value, bytes):
        return SparkplugDataType.Bytes
    return SparkplugDataType.String
