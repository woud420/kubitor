"""Annotation filtering and rules configuration."""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum
import re
from pathlib import Path
import json
import yaml

from ..utils.logger import get_logger

logger = get_logger(__name__)


class AnnotationOperator(Enum):
    """Annotation filter operators."""

    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    IN = "in"
    NOT_IN = "not_in"


@dataclass
class AnnotationFilter:
    """Single annotation filter."""

    key: str
    operator: AnnotationOperator
    value: Optional[Any] = None
    case_sensitive: bool = True

    def matches(self, annotations: Dict[str, str]) -> bool:
        """Check if annotations match this filter."""
        # Dictionary mapping operators to their evaluation functions
        operator_evaluators = {
            AnnotationOperator.EXISTS: self._check_exists,
            AnnotationOperator.NOT_EXISTS: self._check_not_exists,
            AnnotationOperator.EQUALS: self._check_equals,
            AnnotationOperator.NOT_EQUALS: self._check_not_equals,
            AnnotationOperator.CONTAINS: self._check_contains,
            AnnotationOperator.NOT_CONTAINS: self._check_not_contains,
            AnnotationOperator.STARTS_WITH: self._check_starts_with,
            AnnotationOperator.ENDS_WITH: self._check_ends_with,
            AnnotationOperator.REGEX: self._check_regex,
            AnnotationOperator.IN: self._check_in,
            AnnotationOperator.NOT_IN: self._check_not_in,
        }

        evaluator = operator_evaluators.get(self.operator)
        if evaluator:
            return evaluator(annotations)

        logger.warning(f"Unknown operator: {self.operator}")
        return False

    def _check_exists(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation key exists."""
        return self.key in annotations

    def _check_not_exists(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation key does not exist."""
        return self.key not in annotations

    def _check_equals(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value equals."""
        if self.key not in annotations:
            return False

        actual = annotations[self.key]
        expected = str(self.value)

        if not self.case_sensitive:
            actual = actual.lower()
            expected = expected.lower()

        return actual == expected

    def _check_not_equals(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value not equals."""
        return not self._check_equals(annotations)

    def _check_contains(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value contains substring."""
        if self.key not in annotations:
            return False

        actual = annotations[self.key]
        substring = str(self.value)

        if not self.case_sensitive:
            actual = actual.lower()
            substring = substring.lower()

        return substring in actual

    def _check_not_contains(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value does not contain substring."""
        return not self._check_contains(annotations)

    def _check_starts_with(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value starts with prefix."""
        if self.key not in annotations:
            return False

        actual = annotations[self.key]
        prefix = str(self.value)

        if not self.case_sensitive:
            actual = actual.lower()
            prefix = prefix.lower()

        return actual.startswith(prefix)

    def _check_ends_with(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value ends with suffix."""
        if self.key not in annotations:
            return False

        actual = annotations[self.key]
        suffix = str(self.value)

        if not self.case_sensitive:
            actual = actual.lower()
            suffix = suffix.lower()

        return actual.endswith(suffix)

    def _check_regex(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value matches regex."""
        if self.key not in annotations:
            return False

        try:
            flags = 0 if self.case_sensitive else re.IGNORECASE
            pattern = re.compile(str(self.value), flags)
            return bool(pattern.match(annotations[self.key]))
        except re.error as e:
            logger.error(f"Invalid regex pattern: {self.value} - {e}")
            return False

    def _check_in(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value is in list."""
        if self.key not in annotations:
            return False

        actual = annotations[self.key]
        if not self.case_sensitive:
            actual = actual.lower()

        if isinstance(self.value, list):
            values = [str(v).lower() if not self.case_sensitive else str(v) for v in self.value]
            return actual in values

        return False

    def _check_not_in(self, annotations: Dict[str, str]) -> bool:
        """Check if annotation value is not in list."""
        return not self._check_in(annotations)


class FilterLogic(Enum):
    """Filter combination logic."""

    AND = "and"
    OR = "or"


@dataclass
class AnnotationRule:
    """Company-specific annotation rule."""

    name: str
    description: str
    filters: List[AnnotationFilter]
    logic: FilterLogic = FilterLogic.AND
    action: Optional[str] = None  # e.g., "require", "warn", "exclude"
    metadata: Dict[str, Any] = None

    def matches(self, annotations: Dict[str, str]) -> bool:
        """Check if annotations match this rule."""
        if not self.filters:
            return True

        results = [f.matches(annotations) for f in self.filters]

        if self.logic == FilterLogic.AND:
            return all(results)
        else:  # OR
            return any(results)


class AnnotationConfig:
    """Configuration for company-specific annotation rules."""

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize with optional config file."""
        self.rules: List[AnnotationRule] = []
        self.required_annotations: List[str] = []
        self.forbidden_annotations: List[str] = []
        self.annotation_patterns: Dict[str, str] = {}  # key -> regex pattern

        if config_path and config_path.exists():
            self.load_config(config_path)

    def load_config(self, config_path: Path):
        """Load configuration from file."""
        try:
            with open(config_path, "r") as f:
                if config_path.suffix == ".yaml" or config_path.suffix == ".yml":
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)

            self._parse_config(config)
            logger.info(f"Loaded annotation config from {config_path}")
        except Exception as e:
            logger.error(f"Failed to load annotation config: {e}")

    def _parse_config(self, config: Dict[str, Any]):
        """Parse configuration dictionary."""
        # Required annotations
        self.required_annotations = config.get("required_annotations", [])

        # Forbidden annotations
        self.forbidden_annotations = config.get("forbidden_annotations", [])

        # Annotation patterns
        self.annotation_patterns = config.get("annotation_patterns", {})

        # Rules
        for rule_config in config.get("rules", []):
            rule = self._parse_rule(rule_config)
            if rule:
                self.rules.append(rule)

    def _parse_rule(self, rule_config: Dict[str, Any]) -> Optional[AnnotationRule]:
        """Parse a single rule configuration."""
        try:
            filters = []
            for filter_config in rule_config.get("filters", []):
                filter_obj = AnnotationFilter(
                    key=filter_config["key"],
                    operator=AnnotationOperator(filter_config["operator"]),
                    value=filter_config.get("value"),
                    case_sensitive=filter_config.get("case_sensitive", True),
                )
                filters.append(filter_obj)

            return AnnotationRule(
                name=rule_config["name"],
                description=rule_config.get("description", ""),
                filters=filters,
                logic=FilterLogic(rule_config.get("logic", "and")),
                action=rule_config.get("action"),
                metadata=rule_config.get("metadata", {}),
            )
        except Exception as e:
            logger.error(f"Failed to parse rule: {e}")
            return None

    def validate_annotations(self, annotations: Dict[str, str]) -> Dict[str, List[str]]:
        """Validate annotations against configured rules."""
        errors = []
        warnings = []

        # Check required annotations
        for required in self.required_annotations:
            if required not in annotations:
                errors.append(f"Missing required annotation: {required}")

        # Check forbidden annotations
        for forbidden in self.forbidden_annotations:
            if forbidden in annotations:
                errors.append(f"Forbidden annotation found: {forbidden}")

        # Check annotation patterns
        for key, pattern in self.annotation_patterns.items():
            if key in annotations:
                try:
                    if not re.match(pattern, annotations[key]):
                        errors.append(
                            f"Annotation {key} value does not match required pattern: {pattern}"
                        )
                except re.error:
                    errors.append(f"Invalid regex pattern for annotation {key}: {pattern}")

        # Check custom rules
        for rule in self.rules:
            if rule.matches(annotations):
                if rule.action == "require":
                    # Rule matched, requirement satisfied
                    pass
                elif rule.action == "warn":
                    warnings.append(f"Rule '{rule.name}' matched: {rule.description}")
                elif rule.action == "exclude":
                    errors.append(f"Exclusion rule '{rule.name}' matched: {rule.description}")

        return {"errors": errors, "warnings": warnings}


def parse_annotation_filters(filter_strings: List[str]) -> List[AnnotationFilter]:
    """Parse CLI annotation filter strings into AnnotationFilter objects.

    Format: key:operator:value or key:operator for exists/not_exists
    Examples:
        - "team:equals:platform"
        - "env:in:prod,staging"
        - "cost-center:exists"
        - "deprecated:not_exists"
        - "owner:regex:.*@company.com"
    """
    filters = []

    for filter_str in filter_strings:
        parts = filter_str.split(":", 2)
        if len(parts) < 2:
            logger.warning(f"Invalid annotation filter format: {filter_str}")
            continue

        key = parts[0]
        operator_str = parts[1]

        try:
            operator = AnnotationOperator(operator_str)
        except ValueError:
            logger.warning(f"Unknown operator: {operator_str}")
            continue

        value = None
        if len(parts) > 2:
            value = parts[2]
            # Handle list values for IN/NOT_IN operators
            if operator in [AnnotationOperator.IN, AnnotationOperator.NOT_IN]:
                value = value.split(",")

        filters.append(
            AnnotationFilter(key=key, operator=operator, value=value, case_sensitive=True)
        )

    return filters
