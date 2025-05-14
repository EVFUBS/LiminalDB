# LiminalDB Technical Specification

This document outlines the current state, implemented features, and planned future developments for the LiminalDB database system.

## Current Technologies

### Programming Languages and Frameworks
-   **Go (Golang)**: The primary programming language for LiminalDB's implementation.
-   **Standard Go Libraries**: Used for core functionality, including:
  -   `bytes`: Buffer manipulation.
  -   `encoding/binary`: Binary data serialization.
  -   `errors`: Error handling mechanisms.
  -   `io`: Input/Output operations.
  -   `os`: File system interactions.
  -   `strings`: String manipulation utilities.
  -   `sync`: Concurrency control using mutexes.
  -   `time`: Handling timestamps and time-related operations.

### Database Systems and Technologies
-   **Custom File Format**: A proprietary binary file format identified by a magic number and version.
-   **B-Tree Indexing**: Utilized for efficient data storage and retrieval.
-   **In-Memory Processing**: Data is loaded into memory for faster processing during operations.
-   **File-Based Storage**: Tables and their associated indexes are stored persistently in separate binary files on the file system.
-   **REPL Interface**: A Read-Eval-Print Loop providing an interactive environment for SQL execution.

## Implemented Features

### Data Types
LiminalDB currently supports the following basic data types:
-   **Integer64 (INT)**: Signed 64-bit integers.
-   **Float64 (FLOAT)**: 64-bit floating-point numbers.
-   **String (STRING)**: Variable-length strings with a defined maximum length (analogous to `VARCHAR`).
-   **Boolean (BOOL)**: True or false values.
-   **Timestamp (TIMESTAMP)**: Represents points in time using Unix timestamp values.

### Schema Management (DDL)
Functionality for defining and managing the database structure:
-   **Table Creation**: Define and create new tables with specified columns.
-   **Table Deletion**: Drop (remove) existing tables and their data.
-   **Column Definition**: Specify column attributes like name, data type, size (for strings), nullability, and primary key status.
-   **Primary Key Constraints**: Enforce uniqueness and non-nullability for primary key columns.
-   **Foreign Key Constraints**: Define relationships between tables based on foreign keys.
-   **Schema Validation**: Validate table metadata for consistency and correctness (e.g., primary key existence, data type compatibility).

### Data Manipulation Language (DML)
Operations for interacting with data stored in tables:
-   **Data Insertion**: Add new rows of data into a table.
-   **Data Retrieval**: Query data from tables, allowing selection of specific fields and filtering rows.
-   **Data Deletion**: Remove rows from a table based on specified conditions.
-   **Field Selection**: Choose specific columns to include in query results, or select all columns using `*`.
-   **Row Filtering**: Apply conditions using `WHERE` clauses with comparison operators to filter retrieved or deleted rows.

### Indexing
Implementation of indexing mechanisms to improve query performance:
-   **B-Tree Indexes**: Create and utilize B-Tree structures for efficient searching, insertion, and deletion.
-   **Primary Key Indexes**: Automatically create indexes for primary key columns to ensure fast lookups.
-   **Unique Indexes**: Create indexes that enforce uniqueness across the indexed columns.
-   **Index Management**: Commands to create, drop, and list existing indexes.
-   **Index Selection**: Automatic selection of the most suitable index for a given query where applicable.

### Query Processing
The core engine for executing SQL statements:
-   **SQL Parsing**: Transform raw SQL statements into an Abstract Syntax Tree (AST).
-   **Query Execution**: Process the AST to perform the requested database operations.
-   **Expression Evaluation**: Evaluate logical and arithmetic expressions, particularly in `WHERE` clauses.
-   **Full Table Scan**: A fallback mechanism to read the entire table when no suitable index can be used or when the query requires it.

### Stored Procedures
Basic support for stored procedural logic:
-   **Procedure Creation**: Define stored procedures, potentially with parameters.
-   **Procedure Execution**: Run defined stored procedures, passing arguments as needed.
-   **Procedure Modification**: Alter the definition of existing stored procedures.

### Error Handling
Mechanisms for identifying and reporting issues:
-   **Validation Errors**: Report errors related to schema definitions or data constraints during operations.
-   **Constraint Violations**: Detect and report violations of primary key and foreign key constraints.
-   **Type Errors**: Identify and report mismatches or invalid conversions between data types.
-   **File Errors**: Handle issues encountered during file system operations (reading, writing, etc.).
-   **Logging**: Record errors, warnings, and potentially debug information for monitoring and troubleshooting.

## Planned Future Features

This section outlines the features planned for future development, categorized into foundational next steps and more advanced SQL capabilities.

### A. Foundational Next Steps

#### Data Types Expansion
Adding support for more specialized and common data types:
-   **NUMERIC(p,s)**: Fixed-point decimal numbers with configurable precision (`p`) and scale (`s`).
-   **DATE**: Store calendar date values without a time component.
-   **JSON**: Native support for JSON documents with built-in query capabilities.
-   **ARRAY**: Support for columns storing arrays of other data types.
-   **ENUM**: Enumerated types allowing values from a predefined list.
-   **INT32**: Signed 32-bit integers.
-   **FLOAT32**: 32-bit floating-point numbers.

#### Schema Management Enhancement
Improving DDL capabilities:
-   **ALTER TABLE Operations**: Commands to modify existing table structures (add, drop, or modify columns).
-   **Schema Versioning and Migration**: Tools or mechanisms to manage schema changes over time.
-   **CHECK Constraints**: Define constraints that validate data based on a boolean expression.

#### Data Manipulation & Querying Language (DML/DQL) Enhancements
Expanding the SQL language support:
-   **UPDATE Statement**: Modify existing rows based on specified conditions.
-   **ORDER BY Clause**: Sort query results by one or more columns.
-   **LIMIT and OFFSET**: Control the number of rows returned and specify a starting point for pagination.

#### Transaction & Concurrency Control
Introducing mechanisms for managing concurrent operations and ensuring data integrity:
-   **Basic Transaction Control**:
  -   Implement **ACID properties** for single database operations.
  -   Support explicit transaction boundaries (`BEGIN`, `COMMIT`, `ROLLBACK`).
  -   Implement basic **concurrency control** using locks to prevent data corruption from concurrent access.

#### Error Handling Improvement
Making error reporting more robust and informative:
-   More specific and user-friendly error messages.
-   Structured error types for programmatic handling by client applications.
-   An error code system for better categorization and documentation of errors.

#### Networking & Server
Transforming LiminalDB into a client-server database:
-   **Network Protocol**: Design and implement a custom protocol for client-server communication, likely over TCP/IP.
-   **Connection Handling**: Manage multiple simultaneous client connections efficiently.
-   **Client Libraries**: Develop libraries in languages like Go and C# to easily connect and interact with the database server.
-   **API Endpoints**: Define clear API endpoints for executing queries, managing schema, and performing other database operations.

### B. Advanced SQL Capabilities

#### Indexing
Adding more sophisticated indexing options:
-   **Hash Indexes**: Optimized for equality lookups.
-   **Full-Text Search Indexes**: Enable efficient text-based searches.
-   **Spatial Indexes**: Support indexing of geographic or geometric data.
-   **Multi-Column Indexes**: Indexes covering multiple columns to optimize queries with compound conditions.
-   **Index-Only Scans**: Ability to retrieve all necessary data directly from an index without accessing the main table data.

#### Comprehensive Transaction Control
Achieving full transactional integrity and concurrent access management:
-   Full **ACID compliance**.
-   Support for multiple **isolation levels** (e.g., READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE).
-   Mechanisms for **deadlock detection** and resolution.
-   Support for **two-phase commit** for distributed transactions (longer term).

#### Query Language
Implementing advanced query constructs:
-   **JOIN Operations**: Combine data from multiple tables (INNER, LEFT, RIGHT, FULL OUTER joins).
-   **Subqueries**: Allow nested queries within other SQL statements.
-   **Common Table Expressions (CTEs)**: Support for named temporary result sets, useful for complex and recursive queries.
-   **Window Functions**: Perform calculations across a set of table rows related to the current row (e.g., ranking, cumulative sums).
-   **Aggregation**: Use `GROUP BY`, `HAVING`, and aggregate functions (`COUNT`, `SUM`, `AVG`, etc.).
-   **Set Operations**: Combine result sets using `UNION`, `INTERSECT`, and `EXCEPT`.

#### Procedural Objects
Enhancing stored procedures and introducing user-defined functions:
-   An enhanced stored procedure language with control structures (loops, conditionals).
-   User-defined functions (UDFs) for encapsulating complex calculations.
-   Support for return values and output parameters in procedures/functions.
-   Exception handling within procedural code.

#### Views & Materialized Views
Providing abstract or precomputed data representations:
-   **Views**: Create virtual tables based on `SELECT` statements.
-   **Materialized Views**: Store the result of a query as a physical table for faster access, potentially with automatic or manual refreshing.

#### Database Security
Implementing security measures to protect data:
-   User authentication and authorization mechanisms.
-   Role-based access control (RBAC) for managing permissions.
-   Row-level and column-level security policies.
-   Data encryption at rest (storage) and in transit (networking).

#### Scalability & Availability
Features for handling larger datasets and ensuring continuous operation:
-   **Partitioning**: Divide large tables into smaller, more manageable parts (Range, List, Hash partitioning). Support for partition pruning and dynamic management.
-   **Replication and High Availability**: Set up primary-replica configurations for failover and read scaling. Support for synchronous and asynchronous replication. Automatic failover and recovery procedures.

## Development Checklist

### Implemented Features
-   [x] Basic data types (INT, FLOAT, STRING, BOOL, TIMESTAMP)
-   [x] Table creation and deletion (DDL)
-   [x] Primary key constraints
-   [x] Basic foreign key constraints
-   [x] Data insertion (DML)
-   [x] Data retrieval with field selection (DQL)
-   [x] Data deletion (DML)
-   [x] B-Tree indexing
-   [x] Unique and primary key indexes
-   [x] SQL parsing and execution engine
-   [x] Basic Stored procedures support
-   [x] Basic error handling and logging

### Foundational Next Steps
-   [ ] Foreign key constraint enhancement (In Progress)
-   [ ] Data Types Expansion
  -   [ ] NUMERIC(p,s)
  -   [ ] DATE
  -   [ ] JSON
  -   [ ] ARRAY
  -   [ ] ENUM
  -   [ ] INT32
  -   [ ] FLOAT32
-   [ ] Transaction & Concurrency Control
  -   [ ] ACID properties for single operations
  -   [ ] Explicit transaction boundaries (BEGIN, COMMIT, ROLLBACK)
  -   [ ] Basic concurrency control (Locking)
-   [ ] Schema Management Enhancement (ALTER TABLE)
  -   [ ] ALTER TABLE operations (ADD/DROP/MODIFY columns)
  -   [ ] Schema versioning and migration
  -   [ ] CHECK constraints
-   [ ] Improved error handling and reporting
-   [ ] Data Manipulation & Querying Language (DML/DQL) Enhancements
  -   [ ] UPDATE statement
  -   [ ] ORDER BY clause
  -   [ ] LIMIT and OFFSET
-   [ ] Networking & Server
  -   [ ] Network Protocol Design/Implementation
  -   [ ] TCP/IP Connections
  -   [ ] Connection Handling
  -   [ ] Connection Pooling
  -   [ ] Client Libraries (Go, C#)
  -   [ ] API Endpoints Definition

### Advanced SQL Capabilities
-   [ ] Procedural Objects (Enhanced Procedures & UDFs)
-   [ ] Indexing (In Progress)
  -   [ ] Hash indexes
  -   [ ] Full-text search indexes
  -   [ ] Spatial indexes
  -   [ ] Multi-column indexes
  -   [ ] Index-only scans
-   [ ] Query Language
  -   [ ] JOIN operations
  -   [ ] Subqueries
  -   [ ] Common Table Expressions (CTEs)
  -   [ ] Window functions
  -   [ ] Aggregation (GROUP BY, HAVING, etc.)
  -   [ ] Set operations (UNION, INTERSECT, EXCEPT)
-   [ ] Comprehensive Transaction Control
  -   [ ] Full ACID compliance
  -   [ ] Multiple isolation levels
  -   [ ] Deadlock detection and resolution
  -   [ ] Two-phase commit
-   [ ] Views & Materialized Views
-   [ ] Database Security
-   [ ] Scalability & Availability (Partitioning, Replication/HA)