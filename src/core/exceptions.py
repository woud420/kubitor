"""Custom exception hierarchy for kubitor.

This module defines specific exception types for different error scenarios
to improve error handling and debugging throughout the application.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class KubitorError(Exception):
    """Base exception for all kubitor-specific errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        if not isinstance(message, str):
            raise TypeError(f"Message must be a string, got {type(message).__name__}")
        if message.strip() == "":
            raise ValueError("Message cannot be empty")
        
        self.message: str = message
        self.details: Dict[str, Any] = details or {}
        
        # Log the error for debugging
        logger.debug(f"KubitorError created: {message}", extra={"details": self.details})
        
        super().__init__(message)


class ValidationError(KubitorError):
    """Raised when input validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None) -> None:
        self.field: Optional[str] = field
        self.value: Any = value
        details: Dict[str, Any] = {}
        if field is not None:
            details["field"] = field
        if value is not None:
            details["value"] = value
        super().__init__(message, details)


class ConfigurationError(KubitorError):
    """Raised when configuration is invalid or missing."""
    pass


class KubernetesError(KubitorError):
    """Base class for Kubernetes-related errors."""
    pass


class KubernetesConnectionError(KubernetesError):
    """Raised when unable to connect to Kubernetes cluster."""
    
    def __init__(self, message: str, context: Optional[str] = None) -> None:
        self.context = context
        details = {}
        if context:
            details["context"] = context
        super().__init__(message, details)


class KubernetesCommandError(KubernetesError):
    """Raised when kubectl command execution fails."""
    
    def __init__(self, message: str, command: List[str], exit_code: int, stderr: str = "") -> None:
        if not isinstance(command, list):
            raise TypeError(f"Command must be a list, got {type(command).__name__}")
        if not command:
            raise ValueError("Command list cannot be empty")
        if not isinstance(exit_code, int):
            raise TypeError(f"Exit code must be an integer, got {type(exit_code).__name__}")
        if not isinstance(stderr, str):
            raise TypeError(f"Stderr must be a string, got {type(stderr).__name__}")
        
        self.command: List[str] = command
        self.exit_code: int = exit_code
        self.stderr: str = stderr
        details: Dict[str, Any] = {
            "command": command,
            "exit_code": exit_code,
            "stderr": stderr
        }
        super().__init__(message, details)


class ResourceNotFoundError(KubernetesError):
    """Raised when a Kubernetes resource is not found."""
    
    def __init__(self, resource_type: str, name: str, namespace: Optional[str] = None) -> None:
        self.resource_type = resource_type
        self.name = name
        self.namespace = namespace
        message = f"{resource_type} '{name}' not found"
        if namespace:
            message += f" in namespace '{namespace}'"
        details = {
            "resource_type": resource_type,
            "name": name,
            "namespace": namespace
        }
        super().__init__(message, details)


class DatabaseError(KubitorError):
    """Base class for database-related errors."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Raised when database connection fails."""
    
    def __init__(self, message: str, database_url: Optional[str] = None) -> None:
        self.database_url = database_url
        details = {}
        if database_url:
            details["database_url"] = database_url
        super().__init__(message, details)


class DatabaseOperationError(DatabaseError):
    """Raised when database operations fail."""
    
    def __init__(self, message: str, operation: str, table: Optional[str] = None) -> None:
        self.operation = operation
        self.table = table
        details = {
            "operation": operation
        }
        if table:
            details["table"] = table
        super().__init__(message, details)


class ScanError(KubitorError):
    """Base class for scanning-related errors."""
    pass


class ScanConfigurationError(ScanError):
    """Raised when scan configuration is invalid."""
    pass


class ScanExecutionError(ScanError):
    """Raised when scan execution fails."""
    
    def __init__(self, message: str, phase: str, resource_count: int = 0) -> None:
        self.phase = phase
        self.resource_count = resource_count
        details = {
            "phase": phase,
            "resource_count": resource_count
        }
        super().__init__(message, details)


class ExportError(KubitorError):
    """Base class for export-related errors."""
    pass


class ExportConfigurationError(ExportError):
    """Raised when export configuration is invalid."""
    pass


class ExportExecutionError(ExportError):
    """Raised when export execution fails."""
    
    def __init__(self, message: str, format_type: str, output_path: Optional[str] = None) -> None:
        self.format_type = format_type
        self.output_path = output_path
        details = {
            "format_type": format_type
        }
        if output_path:
            details["output_path"] = output_path
        super().__init__(message, details)


class AnnotationError(KubitorError):
    """Base class for annotation-related errors."""
    pass


class AnnotationValidationError(AnnotationError):
    """Raised when annotation validation fails."""
    
    def __init__(self, message: str, annotation_key: str, annotation_value: Any = None) -> None:
        self.annotation_key = annotation_key
        self.annotation_value = annotation_value
        details = {
            "annotation_key": annotation_key
        }
        if annotation_value is not None:
            details["annotation_value"] = annotation_value
        super().__init__(message, details)


class AnnotationConfigError(AnnotationError):
    """Raised when annotation configuration is invalid."""
    pass


class SecurityError(KubitorError):
    """Raised when security-related issues are detected."""
    pass


class InputSanitizationError(SecurityError):
    """Raised when input sanitization fails or detects malicious input."""
    
    def __init__(self, message: str, input_value: str, input_type: str) -> None:
        self.input_value = input_value
        self.input_type = input_type
        details = {
            "input_value": input_value,
            "input_type": input_type
        }
        super().__init__(message, details)


class HistoryError(KubitorError):
    """Base class for history-related errors."""
    pass


class HistoryNotFoundError(HistoryError):
    """Raised when historical data is not found."""
    
    def __init__(self, message: str, scan_id: Optional[int] = None, time_range: Optional[str] = None) -> None:
        self.scan_id: Optional[int] = scan_id
        self.time_range: Optional[str] = time_range
        details: Dict[str, Any] = {}
        if scan_id is not None:
            details["scan_id"] = scan_id
        if time_range is not None:
            details["time_range"] = time_range
        super().__init__(message, details)


class AnalysisError(KubitorError):
    """Base class for analysis-related errors."""
    pass


class AnalysisExecutionError(AnalysisError):
    """Raised when analysis execution fails."""
    
    def __init__(self, message: str, analysis_type: str, data_points: int = 0) -> None:
        self.analysis_type = analysis_type
        self.data_points = data_points
        details = {
            "analysis_type": analysis_type,
            "data_points": data_points
        }
        super().__init__(message, details)


class DriftAnalysisError(AnalysisError):
    """Raised when drift analysis fails."""
    
    def __init__(self, message: str, baseline_scan_id: Optional[int] = None, compare_scan_id: Optional[int] = None) -> None:
        self.baseline_scan_id: Optional[int] = baseline_scan_id
        self.compare_scan_id: Optional[int] = compare_scan_id
        details: Dict[str, Any] = {}
        if baseline_scan_id is not None:
            details["baseline_scan_id"] = baseline_scan_id
        if compare_scan_id is not None:
            details["compare_scan_id"] = compare_scan_id
        super().__init__(message, details)