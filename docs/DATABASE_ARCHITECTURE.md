# Database Architecture

## Clean Layered Architecture

Kubitor follows a clean layered architecture for database operations:

```
┌─────────────────────────────────────────────────┐
│                   CLI/API Layer                  │
│  (src/cli/, external APIs)                      │
└─────────────────────┬───────────────────────────┘
                      │ Uses
┌─────────────────────▼───────────────────────────┐
│              Service Layer                       │
│  (src/api/)                                     │
│  • ScanService                                  │
│  • AnalysisService                              │
│  • HistoryService                              │
└─────────────────────┬───────────────────────────┘
                      │ Uses
┌─────────────────────▼───────────────────────────┐
│            Repository Layer                      │
│  (src/model/repository/)                        │
│  • ScanRepository - Business logic              │
│  • ResourceRepository - Business logic          │
│  • ChangeRepository - Business logic            │
│  • HistoricalRepository - Analytics             │
└─────────────────────┬───────────────────────────┘
                      │ Uses
┌─────────────────────▼───────────────────────────┐
│              DAO Layer                          │
│  (src/model/dao/)                               │
│  • ScanDAO - SQL queries                        │
│  • ResourceDAO - SQL queries                    │
│  • ChangeDAO - SQL queries                      │
│  • Dialect-specific implementations             │
└─────────────────────┬───────────────────────────┘
                      │ Uses
┌─────────────────────▼───────────────────────────┐
│           Connection Layer                       │
│  (src/database/)                                │
│  • DatabaseConnection - Connection management   │
│  • AsyncDatabaseConnection - Async support      │
│  • Transaction management                       │
└──────────────────────────────────────────────────┘
```

## Layer Responsibilities

### 1. Connection Layer (`src/database/`)
- **Purpose**: Low-level database connectivity
- **Responsibilities**:
  - Connection pooling
  - Transaction management
  - Database dialect detection
  - Query execution
- **No business logic**

### 2. DAO Layer (`src/model/dao/`)
- **Purpose**: Data Access Objects - SQL query encapsulation
- **Responsibilities**:
  - CRUD operations
  - SQL query construction
  - Database dialect-specific queries
  - Result set mapping
- **No business logic, only data access**

### 3. Repository Layer (`src/model/repository/`)
- **Purpose**: Business logic for data operations
- **Responsibilities**:
  - Complex queries using DAOs
  - Data aggregation
  - Business rules enforcement
  - Data transformation
- **Uses DAOs, never writes SQL directly**

### 4. Service Layer (`src/api/`)
- **Purpose**: High-level business operations
- **Responsibilities**:
  - Orchestration of repositories
  - Transaction boundaries
  - API contracts
  - External service integration
- **Uses repositories, never DAOs directly**

### 5. CLI/API Layer
- **Purpose**: User interaction
- **Responsibilities**:
  - Command parsing
  - Input validation
  - Output formatting
  - Error handling for users
- **Uses services, never repositories directly**

## Database Dialect Support

The architecture supports multiple database dialects through the DAO layer:

```python
# DAO Registry Pattern
SCAN_DAO_REGISTRY: Dict[DatabaseDialect, Callable] = {
    DatabaseDialect.SQLITE: lambda conn: ScanDAOSQLite(conn),
    DatabaseDialect.POSTGRESQL: lambda conn: ScanDAOPostgreSQL(conn),
}
```

## Transaction Management

Transactions are managed at the service layer:

```python
# Service layer handles transaction boundaries
class ScanService:
    def perform_scan(self, context, namespace):
        # Begin transaction
        scan_id = self.scan_repo.create_scan_record(...)
        self.resource_repo.bulk_create_resources(scan_id, resources)
        self.change_repo.detect_and_store_changes(scan_id)
        # Commit transaction
```

## Example Flow

**Storing a scan:**

1. CLI calls `ScanService.perform_scan()`
2. ScanService orchestrates:
   - Calls `ScanRepository.create_scan_record()`
   - ScanRepository calls `ScanDAO.create_scan()`
   - ScanDAO executes SQL via `DatabaseConnection`
3. Results flow back up through layers

## Benefits

1. **Separation of Concerns**: Each layer has a single responsibility
2. **Testability**: Each layer can be tested with mocks
3. **Flexibility**: Easy to swap implementations (e.g., different databases)
4. **Maintainability**: Changes are localized to specific layers
5. **No Duplication**: Single source of truth for each operation

## Migration from Old Architecture

Previously, there were duplicate implementations:
- `src/database/service.py` had inline DAOs (removed)
- Direct SQL in services (refactored to use repositories)
- Mixed patterns in CLI (standardized to use services)

Now all database operations follow the clean architecture pattern.