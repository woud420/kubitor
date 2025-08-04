"""Export-related models."""

from enum import Enum


class ExportFormat(str, Enum):
    """Supported export formats."""

    YAML = "yaml"
    JSON = "json"


class OrganizeBy(str, Enum):
    """Resource organization strategies."""

    SERVICE = "service"
    NAMESPACE = "namespace"
    TYPE = "type"
    ANNOTATION = "annotation"
