# Testing Structure

The test directory structure mirrors the source code structure for better organization and maintainability.

## Test Directory Structure

```
tests/
├── api/                    # Tests for API services
│   └── test_scan_service.py
├── cli/                    # Tests for CLI commands
├── core/                   # Tests for core business logic
│   ├── test_inventory.py
│   ├── test_management_detector.py
│   └── test_reporter.py
├── database/               # Tests for database layer
│   ├── test_connection.py
│   └── test_service.py
├── integration/            # Integration tests
│   └── test_database_integration.py
├── k8s/                    # Tests for Kubernetes client
│   └── test_client.py
├── model/                  # Tests for data models
│   ├── dao/               # Tests for DAOs
│   │   └── test_dao.py
│   ├── repository/        # Tests for repositories
│   │   └── test_repository.py
│   ├── test_inventory.py
│   ├── test_management.py
│   └── test_models.py
├── upgrade/                # Tests for upgrade features
│   ├── test_advisor.py
│   └── test_deprecated_api_detection.py
└── conftest.py            # Pytest configuration and fixtures
```

## Test Organization Principles

### 1. Mirror Source Structure
Each source module should have a corresponding test module:
- `src/core/inventory.py` → `tests/core/test_inventory.py`
- `src/model/management.py` → `tests/model/test_management.py`
- `src/k8s/client.py` → `tests/k8s/test_client.py`

### 2. Test Naming Convention
- Test files: `test_<module_name>.py`
- Test classes: `Test<ClassName>`
- Test methods: `test_<functionality_description>`

### 3. Test Categories

#### Unit Tests
Located in their respective module directories, testing individual components in isolation:
- `tests/core/` - Core business logic tests
- `tests/model/` - Model validation and behavior tests
- `tests/k8s/` - Kubernetes client tests

#### Integration Tests
Located in `tests/integration/`, testing interactions between components:
- Database integration tests
- End-to-end workflow tests

#### API Tests
Located in `tests/api/`, testing service layer functionality:
- Scan service tests
- Analysis service tests

## Running Tests

### Run All Tests
```bash
pytest tests/
```

### Run Tests for Specific Module
```bash
# Test core module
pytest tests/core/

# Test models
pytest tests/model/

# Test Kubernetes client
pytest tests/k8s/
```

### Run Specific Test File
```bash
pytest tests/core/test_inventory.py
```

### Run with Coverage
```bash
pytest tests/ --cov=src --cov-report=html
```

### Run with Verbose Output
```bash
pytest tests/ -v
```

## Writing Tests

### Test Template
```python
"""Tests for <module_name>."""

import pytest
from unittest.mock import Mock, patch

from src.<module_path> import <ClassToTest>


class Test<ClassName>:
    """Test <ClassName> functionality."""
    
    def test_<functionality>(self):
        """Test <specific functionality>."""
        # Arrange
        ...
        
        # Act
        ...
        
        # Assert
        ...
```

### Using Fixtures
Common fixtures are defined in `conftest.py`:
```python
@pytest.fixture
def mock_k8s_client():
    """Mock Kubernetes client."""
    client = Mock()
    client.get_json.return_value = {"items": []}
    return client
```

### Mocking Guidelines
1. Mock external dependencies (filesystem, network, database)
2. Use `unittest.mock` for simple mocks
3. Use `pytest-mock` for more complex scenarios
4. Mock at the boundary of your code

## Test Coverage Goals

- Minimum coverage: 80%
- Critical paths: 100%
- Models: 100%
- Business logic: 90%+

## Continuous Integration

Tests are automatically run on:
- Pull requests
- Commits to main branch
- Release tags

## Common Test Patterns

### Testing Models
```python
def test_model_creation():
    """Test model instantiation."""
    model = MyModel(field1="value1", field2="value2")
    assert model.field1 == "value1"
    assert model.field2 == "value2"
```

### Testing with Mocks
```python
@patch('src.module.external_function')
def test_with_mock(mock_func):
    """Test with mocked external dependency."""
    mock_func.return_value = "mocked_value"
    result = function_under_test()
    assert result == "expected_value"
    mock_func.assert_called_once()
```

### Testing Async Code
```python
@pytest.mark.asyncio
async def test_async_function():
    """Test async functionality."""
    result = await async_function()
    assert result == expected_value
```

## Troubleshooting

### Import Errors
Ensure you're running tests from the project root:
```bash
cd /path/to/kubitor
pytest tests/
```

### Database Tests Failing
Database tests may require a test database. Check `conftest.py` for database fixtures.

### Async Test Issues
Make sure `pytest-asyncio` is installed:
```bash
pip install pytest-asyncio
```