# LiminalDB Technical Specification

## Current Technologies

### Programming Languages and Frameworks
- **Go (Golang)**: The primary programming language used for implementing LiminalDB
- **Standard Go Libraries**: Used for core functionality including:
  - `bytes`: For buffer manipulation
  - `encoding/binary`: For binary data serialization
  - `errors`: For error handling
  - `io`: For I/O operations
  - `os`: For file system operations
  - `strings`: For string manipulation
  - `sync`: For mutex-based concurrency control
  - `time`: For timestamp handling

### Database Systems and Technologies
- **Custom File Format**: Binary file format with a magic number and version for identification
- **B-Tree Indexing**: Used for efficient data retrieval
- **In-Memory Processing**: Data is loaded into memory for processing
- **File-Based Storage**: Tables and indexes are stored in separate binary files
- **REPL Interface**: Read-Eval-Print Loop for interactive SQL execution

## Implemented Features

### Data Types
- **Integer64 (INT)**: 64-bit integer values
- **Float64 (FLOAT)**: 64-bit floating-point values
- **String (STRING)**: Variable-length string values with maximum length constraint (my version of varchar)
- **Boolean (BOOL)**: True/false values
- **Timestamp (TIMESTAMP)**: Unix timestamp values

### Schema Management
- **Table Creation**: Create tables with column definitions
- **Table Deletion**: Drop tables
- **Column Definition**: Define columns with name, data type, length (for strings), nullability, and primary key flag
- **Primary Key Constraints**: Define primary key columns
- **Foreign Key Constraints**: Define foreign key relationships between tables
- **Schema Validation**: Validate table metadata (column names, primary keys, etc.)

### Data Manipulation
- **Data Insertion**: Insert rows into tables
- **Data Retrieval**: Query data with field selection and filtering
- **Data Deletion**: Delete rows based on conditions
- **Field Selection**: Select specific columns or all columns (*)
- **Row Filtering**: Filter rows using WHERE clauses with comparison operators

### Indexing
- **B-Tree Indexes**: Create and use B-Tree indexes for efficient data retrieval
- **Primary Key Indexes**: Automatically create indexes for primary keys
- **Unique Indexes**: Create indexes with uniqueness constraint
- **Index Management**: Create, drop, and list indexes
- **Index Selection**: Automatically select the best index for a query

### Query Processing
- **SQL Parsing**: Parse SQL statements into an Abstract Syntax Tree (AST)
- **Query Execution**: Execute SQL queries against the database
- **Expression Evaluation**: Evaluate expressions in WHERE clauses
- **Full Table Scan**: Fall back to full table scan when no suitable index is found

### Stored Procedures
- **Procedure Creation**: Create stored procedures with parameters
- **Procedure Execution**: Execute stored procedures with arguments
- **Procedure Modification**: Alter existing stored procedures

### Error Handling
- **Validation Errors**: Detect and report schema validation errors
- **Constraint Violations**: Detect and report primary key and foreign key violations
- **Type Errors**: Detect and report data type mismatches
- **File Errors**: Handle file system errors
- **Logging**: Log errors and debug information

## Planned Future Features

### A. Foundational Next Steps

- **Foreign Key Constraints Enhancement**:
  - Improve performance of foreign key constraint checking
  - Add support for ON DELETE and ON UPDATE actions (CASCADE, SET NULL, etc.)
  - Implement referential integrity checks during schema modifications

- **Expanded Data Type Support**:
  - **NUMERIC(p,s)**: Fixed-point numbers with precision and scale
  - **DATE**: Date values without time component
  - **JSON**: Native JSON data type with query support
  - **ARRAY**: Support for array data types
  - **ENUM**: Enumerated types with predefined values

- **Basic Transaction Control**:
  - Implement ACID properties for single operations
  - Add support for explicit transaction boundaries (BEGIN, COMMIT, ROLLBACK)
  - Implement basic concurrency control with locks

- **Schema Management Enhancement**:
  - Add support for ALTER TABLE operations (ADD/DROP/MODIFY columns)
  - Implement schema versioning and migration
  - Add support for CHECK constraints

- **Improved Error Handling and Reporting**:
  - More specific and user-friendly error messages
  - Structured error types for programmatic handling
  - Error code system for categorizing errors

- **Query Language Enhancements**:
  - Implement UPDATE statement for modifying existing data
  - Add support for ORDER BY clause for result sorting
  - Implement LIMIT and OFFSET for pagination

### B. Advanced SQL Capabilities

- **Advanced Indexing**:
  - **Hash Indexes**: For equality comparisons
  - **Full-Text Search Indexes**: For text search operations
  - **Spatial Indexes**: For geographic data
  - **Multi-Column Indexes**: Optimize for queries with multiple conditions
  - **Index-Only Scans**: Retrieve data directly from indexes when possible

- **Comprehensive Transaction Control**:
  - Full ACID compliance
  - Multiple isolation levels (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE)
  - Deadlock detection and resolution
  - Two-phase commit for distributed transactions

- **Advanced Querying**:
  - **JOIN Operations**: INNER, LEFT, RIGHT, FULL OUTER joins
  - **Subqueries**: Support for nested queries
  - **Common Table Expressions (CTEs)**: For recursive and complex queries
  - **Window Functions**: For analytical queries
  - **Aggregation**: GROUP BY, HAVING, and aggregate functions
  - **Set Operations**: UNION, INTERSECT, EXCEPT

- **Stored Procedures and Functions**:
  - Enhanced stored procedure language with control structures
  - User-defined functions for complex calculations
  - Return values and output parameters
  - Exception handling in procedures

- **Triggers**:
  - BEFORE/AFTER triggers for INSERT, UPDATE, DELETE
  - Row-level and statement-level triggers
  - Conditional trigger execution

- **Views & Materialized Views**:
  - Virtual tables based on SELECT statements
  - Materialized views for performance optimization
  - Automatic and manual view refreshing

- **Database Security Enhancements**:
  - User authentication and authorization
  - Role-based access control
  - Row-level and column-level security
  - Data encryption at rest and in transit

- **Partitioning**:
  - Range, list, and hash partitioning
  - Partition pruning for query optimization
  - Dynamic partition management

- **Replication and High Availability**:
  - Primary-replica replication
  - Synchronous and asynchronous replication
  - Automatic failover and recovery

## Development Checklist

### Implemented Features
- [x] Basic data types (INT, FLOAT, STRING, BOOL, TIMESTAMP)
- [x] Table creation and deletion
- [x] Primary key constraints
- [x] Basic foreign key constraints
- [x] Data insertion
- [x] Data retrieval with field selection
- [x] Data deletion
- [x] B-Tree indexing
- [x] Unique and primary key indexes
- [x] SQL parsing and execution
- [x] Basic Stored procedures support
- [x] Basic error handling and logging

### Foundational Next Steps
- [ ] Foreign key constraint enhancement (In Progress)
- [ ] Expanded data type support
  - [ ] NUMERIC(p,s)
  - [ ] DATE
  - [ ] JSON
  - [ ] ARRAY
  - [ ] ENUM
  - [ ] INT32
  - [ ] FLOAT32
- [ ] Basic transaction control
  - [ ] ACID properties for single operations
  - [ ] Explicit transaction boundaries
  - [ ] Basic concurrency control
- [ ] Schema management enhancement
  - [ ] ALTER TABLE operations
  - [ ] Schema versioning and migration
  - [ ] CHECK constraints
- [ ] Improved error handling and reporting
- [ ] Query language enhancements
  - [ ] UPDATE statement
  - [ ] ORDER BY clause
  - [ ] LIMIT and OFFSET
- [ ] Database server
  - [ ] **Network Protocol**:
    - Design and implement a custom network protocol for client-server communication
    - Support for TCP/IP connections
  - [ ] **Connection Handling**:
    - Manage multiple client connections concurrently
    - Connection pooling for efficient resource utilization
  - [ ] **Client Libraries**:
    - Develop client libraries for Go and C#
  - [ ] **API Endpoints**:
    - Define API endpoints for database operations (e.g., executing queries, managing schema)


### Advanced SQL Capabilities
- [ ] Triggers
- [ ] Advanced indexing
  - [ ] Hash indexes
  - [ ] Full-text search indexes
  - [ ] Spatial indexes
  - [ ] Multi-column indexes
  - [ ] Index-only scans
- [ ] Advanced querying
  - [ ] JOIN operations
  - [ ] Subqueries
  - [ ] Common Table Expressions (CTEs)
  - [ ] Window functions
  - [ ] Aggregation
  - [ ] Set operations
- [ ] Comprehensive transaction control
  - [ ] Full ACID compliance
  - [ ] Multiple isolation levels
  - [ ] Deadlock detection and resolution
  - [ ] Two-phase commit
- [ ] Enhanced stored procedures and functions
- [ ] Views & materialized views
- [ ] Database security enhancements
- [ ] Partitioning
- [ ] Replication and high availability