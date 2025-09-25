# LSQL Language Specification

## Introduction

LSQL (Liminal SQL) is a SQL-like language for interacting with the LiminalDb database system. This document outlines the syntax, features, and usage of LSQL.

## Basic Syntax

LSQL statements follow a similar syntax to standard SQL, with some specific features and limitations. Statements are case-insensitive and typically end with a semicolon (though this may be optional in some contexts).

### Comments

LSQL does not currently support comments.

### Identifiers

Identifiers (table names, column names) follow standard naming conventions:
- Must begin with a letter or underscore
- Can contain letters, numbers, and underscores
- Are case-insensitive

### Variables

Variables in LSQL are prefixed with the `@` symbol:
```sql
@id
@name
```

Variables are primarily used in stored procedures.

## Data Types

LSQL supports the following data types:

| Type | Description | Example |
|------|-------------|---------|
| `int` | Integer values | `42` |
| `string(n)` | String values with maximum length n | `'Hello'` |
| `bool` | Boolean values | `true`, `false` |
| `float` | Floating-point values | `3.14` |

### Type Declarations

When creating tables, columns are defined with their types:

```sql
CREATE TABLE users (
    id int primary key,
    name string(100),
    active bool,
    salary float
)
```

## Statements

### Data Definition Language (DDL)

#### CREATE TABLE

Creates a new table with specified columns and constraints.

```sql
CREATE TABLE table_name (
    column1 data_type [constraints],
    column2 data_type [constraints],
    ...
)
```

Example:
```sql
CREATE TABLE users (
    id int primary key,
    name string(100),
    active bool
)
```

### Foreign Keys

Foreign keys establish relationships between tables by referencing the primary key of another table. This ensures referential integrity in the database.

```sql
CREATE TABLE table_name (
    column1 data_type [constraints],
    column2 data_type,
    FOREIGN KEY (column2) REFERENCES referenced_table(referenced_column)
)
```

Example:
```sql
CREATE TABLE customers (
    cid int primary key,
    name string(100)
)

CREATE TABLE orders (
    oid int primary key,
    customer_id int,
    FOREIGN KEY (customer_id) REFERENCES customers(cid)
)
```

When using foreign keys:
- The referenced column must be a primary key in the referenced table
- Inserting a value in the foreign key column that doesn't exist in the referenced table will fail
- The data types of the foreign key and referenced column must match

Example usage:
```sql
-- This will succeed because customer with cid=1 exists
INSERT INTO customers (cid, name) VALUES (1, 'John Doe')
INSERT INTO orders (oid, customer_id) VALUES (1, 1)

-- This will fail because customer with cid=999 doesn't exist
INSERT INTO orders (oid, customer_id) VALUES (2, 999)
```

### Dropping Foreign Keys

To remove a foreign key constraint from a table, use the `DROP FOREIGN KEY` statement:

```sql
ALTER TABLE table_name DROP FOREIGN KEY constraint_name
```

Example:
```sql
-- Remove the foreign key constraint from the orders table
ALTER TABLE orders DROP FOREIGN KEY fk_customer_id
```

Note: When dropping a table that has foreign key constraints referencing it, you must first drop the foreign key constraints or drop the referencing tables.

Constraints:
- `primary key`: Designates a column as the primary key

#### DROP TABLE

Removes a table from the database.

```sql
DROP TABLE table_name
```

Example:
```sql
DROP TABLE users
```

#### CREATE INDEX

Creates an index on a table column.

```sql
CREATE [UNIQUE] INDEX index_name ON table_name (column_name)
```

Example:
```sql
CREATE INDEX idx_user_name ON users (name)
CREATE UNIQUE INDEX idx_user_email ON users (email)
```

#### DROP INDEX

Removes an index.

```sql
DROP INDEX index_name ON table_name
```

Example:
```sql
DROP INDEX idx_user_name ON users
```

#### SHOW INDEXES

Lists all indexes for a table.

```sql
SHOW INDEXES FROM table_name
```

Example:
```sql
SHOW INDEXES FROM users
```

#### DESCRIBE

Shows the structure of a table.

```sql
DESC TABLE table_name
```

Example:
```sql
DESC TABLE users
```

### Data Manipulation Language (DML)

#### SELECT

Retrieves data from a table.

```sql
SELECT column1, column2, ... FROM table_name [WHERE condition]
```

Example:
```sql
SELECT id, name FROM users WHERE active = true
```

To select all columns, use the asterisk:
```sql
SELECT * FROM users
```

#### INSERT

Adds new rows to a table.

```sql
INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...)
```

Example:
```sql
INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)
```

#### DELETE

Removes rows from a table.

```sql
DELETE FROM table_name WHERE condition
```

Example:
```sql
DELETE FROM users WHERE active = false
```

### Stored Procedures

#### CREATE PROCEDURE

Creates a new stored procedure.

```sql
CREATE PROCEDURE procedure_name(@param1 type, @param2 type, ...) AS
BEGIN
    SQL statements;
END
```

Example:
```sql
CREATE PROCEDURE get_user_by_id(@id int) AS
BEGIN
    SELECT name, active FROM users WHERE id = @id;
END
```

#### ALTER PROCEDURE

Modifies an existing stored procedure.

```sql
ALTER PROCEDURE procedure_name(@param1 type, @param2 type, ...) AS
BEGIN
    SQL statements;
END
```

Example:
```sql
ALTER PROCEDURE get_user_by_id(@id int) AS
BEGIN
    SELECT id, name, active FROM users WHERE id = @id;
END
```

#### EXEC

Executes a stored procedure.

```sql
EXEC procedure_name(value1, value2, ...)
```

Example:
```sql
EXEC get_user_by_id(1)
```

## Expressions and Operators

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `id = 1` |
| `<` | Less than | `price < 100` |
| `>` | Greater than | `quantity > 0` |
| `<=` | Less than or equal to | `age <= 18` |
| `>=` | Greater than or equal to | `score >= 90` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Logical AND | `active = true AND age > 18` |
| `OR` | Logical OR | `department = 'HR' OR department = 'Marketing'` |

### Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `price + tax` |
| `-` | Subtraction | `total - discount` |
| `*` | Multiplication | `quantity * price` |
| `/` | Division | `total / count` |
## Future Extensions

This language specification will be extended as new features are added to LSQL. Examples being:

- Joins between tables
- Aggregate functions (COUNT, SUM, AVG, etc.)
- Subqueries
- Transactions
- Views
- More advanced constraints (CHECK, UNIQUE, etc.)
- User-defined functions
- More data types
- Triggers
- Batch operations