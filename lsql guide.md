# LSQL Guide

> How to understand LSQL and design your own flavour of SQL for LiminalDB

This guide is a practical companion to the existing language and technical
specifications. It uses the integration tests (`tests/integration/*.go`) as
examples of **real LSQL in use**, and then explains how you can design your
own SQL _flavour_ on top of LiminalDB.

If you want to:

- Learn the shape of LSQL quickly by example, and/or
- Experiment with a slightly different SQL dialect (custom syntax,
  additional constructs, or restricted subsets)

this document walks you through the concepts and the files in this repo that
you will need to touch.

---

## 1. LSQL in 10 minutes (by example)

This section is grounded in the integration tests:

- `tests/integration/sql_test.go`
- `tests/integration/transaction_test.go`
- `tests/integration/expression_test.go`

They form a **living specification** of what LSQL currently supports.

### 1.1. Basic DDL – creating and dropping tables

From `TestCreateTable`:

```sql
CREATE TABLE users (
    id int primary key,
    name string(100),
    active bool
)
```

Key points:

- `CREATE TABLE <name> (...)` with a comma‑separated column list
- column types: `int`, `string(n)`, `bool`, `float`, `datetime`
- `primary key` is specified inline on a single column

Dropping a table (`TestDropTable`):

```sql
DROP TABLE temp_table
```

After a successful `DROP TABLE`, the on‑disk table file should be gone.

### 1.2. Basic DML – insert, select, update, delete

From `TestInsertRow`:

```sql
CREATE TABLE products (pid int primary key, pname string(50));

INSERT INTO products (pid, pname)
VALUES (1, 'Test Product');
```

From `TestSelectRow`:

```sql
CREATE TABLE customers (
    cid   int primary key,
    email string(100)
);

INSERT INTO customers (cid, email)
VALUES (101, 'test@example.com');

SELECT cid, email
FROM customers
WHERE cid = 101;
```

From `TestUpdateRow`:

```sql
CREATE TABLE items (
    id          int primary key,
    description string(200)
);

INSERT INTO items (id, description)
VALUES (1, 'Old Description');

UPDATE items
SET description = 'Updated Description'
WHERE id = 1;

SELECT description FROM items WHERE id = 1;
```

From `TestDropRow` (deleting rows):

```sql
DELETE FROM orders WHERE oid = 201;
```

Error semantics demonstrated in tests:

- Selecting from a non‑existent table should return an error
  (`TestSelectNonExistentTable`).
- Inserting into, deleting from, or dropping a non‑existent table should
  return an error (`TestInsertIntoNonExistentTable`,
  `TestDeleteFromNonExistentTable`, `TestDropNonExistentTable`).

### 1.3. Foreign keys

From `TestForeignKey`:

```sql
CREATE TABLE customers (
    cid  int primary key,
    name string(100)
);

CREATE TABLE orders (
    oid         int primary key,
    customer_id int,
    FOREIGN KEY (customer_id) REFERENCES customers(cid)
);

INSERT INTO customers (cid, name) VALUES (1, 'John Doe');

-- Valid: customer_id 1 exists
INSERT INTO orders (oid, customer_id) VALUES (1, 1);

-- Invalid: customer_id 999 does not exist, must error
INSERT INTO orders (oid, customer_id) VALUES (2, 999);
```

From `TestDropForeignKey`:

```sql
ALTER TABLE orders DROP CONSTRAINT FK_orders_customer_id;

-- After dropping the FK, inserting an unknown customer_id MUST succeed
INSERT INTO orders (oid, customer_id) VALUES (3, 1);
```

### 1.4. Altering tables and dealing with existing data

LSQL supports `ALTER TABLE ... ADD COLUMN` with various combinations of
nullability and default values. The following patterns are covered in tests.

#### 1.4.1. Add a nullable column to an empty table

From `TestAddColumnNoData`:

```sql
CREATE TABLE products (
    pid   int primary key,
    pname string(50)
);

ALTER TABLE products ADD COLUMN price float;

SELECT pid, pname, price FROM products;
```

- The new column `price` is `float` and nullable.
- Since there are no rows, the result set is empty but the schema contains
  the new column.

#### 1.4.2. Add a column with a default value to a table with data

From `TestAddColumnWithData`:

```sql
CREATE TABLE products (
    pid   int primary key,
    pname string(50)
);

INSERT INTO products (pid, pname)
VALUES (1, 'Product A');

ALTER TABLE products
ADD COLUMN price float DEFAULT 9.99;

SELECT pid, pname, price FROM products;
```

Semantics:

- Existing rows get `price = 9.99`.
- New rows (if any) will use the default when `price` is omitted.

#### 1.4.3. Add a nullable column without default to a table with data

From `TestAddColumnWithExistingData`:

```sql
ALTER TABLE products
ADD COLUMN price float NULL;

SELECT pid, pname, price FROM products;
```

- Existing rows get `price = NULL`.

#### 1.4.4. Adding a non‑nullable column with no default must error

From `TestAddColumnWithExistingDataNoDefault`:

```sql
ALTER TABLE products
ADD COLUMN price float NOT NULL;
```

- This must fail if the table already contains data, because there is no
  value to fill in for existing rows.

### 1.5. Datetime values

From `TestTimestamp`:

```sql
CREATE TABLE events (
    id         int primary key,
    event_time datetime
);

INSERT INTO events (id, event_time)
VALUES (1, '2023-10-01 12:00:00');

SELECT id, event_time FROM events WHERE id = 1;
```

- Datetimes are parsed from the string literal format
  `YYYY-MM-DD HH:MM:SS` in UTC.

### 1.6. Expressions and operator precedence

From `tests/integration/expression_test.go`:

```sql
CREATE TABLE math_test (id int primary key, value int);

INSERT INTO math_test (id, value) VALUES (1, 10);

SELECT id, value
FROM math_test
WHERE value = 2 + 3 * 4;  -- 3*4 happens before +2, so expression = 14
```

The tests also cover:

```sql
SELECT id FROM math_test WHERE value = 10 + 20 / 5;  -- 20/5 = 4 => 14
SELECT id FROM math_test WHERE value = 2 * 3 + 4 * 5; -- 6 + 20 = 26
```

Arithmetic precedence:

1. `*` and `/`
2. `+` and `-`

Logical and comparison operators (see `Language Specification.md` and
`TestComplexQuery`):

- Comparisons: `=`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`

Example from `TestComplexQuery`:

```sql
SELECT name, salary
FROM employees
WHERE department = 'Engineering'
  AND salary > 75000;

SELECT name, department
FROM employees
WHERE department = 'HR' OR department = 'Marketing';
```

### 1.7. Transactions

LSQL supports explicit transactions using `BEGIN TRAN`, `COMMIT` and
`ROLLBACK`. There are two main usages in this codebase:

1. **Client‑side wrapper** used by integration tests
   in `tests/integration/sql_test.go`:

   ```go
   func wrapSqlInCommitTransaction(sql string) string {
       tranSql := "BEGIN TRAN \n" + sql + "\n COMMIT"
       return tranSql
   }
   ```

   This pattern wraps a single statement in a transaction that always commits.

2. **Multi‑statement transactions** (over HTTP) in
   `tests/integration/transaction_test.go`:

   ```sql
   BEGIN TRAN
   CREATE TABLE single_tx (id int primary key, value string(50))
   INSERT INTO single_tx (id, value) VALUES (1, 'test')
   COMMIT
   ```

   ```sql
   BEGIN TRAN
   CREATE TABLE tx_roll (id int primary key, name string(50))
   INSERT INTO tx_roll (id, name) VALUES (1, 'Alice')
   ROLLBACK
   ```

Transaction semantics shown in tests:

- After **commit**, tables and rows become visible and durable.
- After **rollback**, tables created in the transaction do not exist on disk
  and are not queryable.
- Mixed operations (create, insert, update, delete, alter) inside a single
  transaction are atomically committed or rolled back; see
  `TestTransactionMixedOperations`.

### 1.8. Concurrency

The transaction tests also document LSQL’s behaviour under concurrency
(`TestConcurrentInsertsSameTable`, `TestConcurrentInsertsDifferentTables`,
`TestConcurrentReadersDuringWrites`). These are more about the database
engine than the language surface, but they matter when you define a
transactional flavour:

- Concurrent inserts into the same table are expected to succeed without
  corrupting data.
- Concurrent inserts across multiple tables should all be visible and
  consistent.
- Readers concurrent with writers should be able to read without errors.

---

## 2. How LSQL is implemented (high‑level)

Understanding the implementation helps you decide **where** to change things
when designing your own flavour.

At a very high level, an LSQL query moves through these stages:

1. **Lexing** – convert the raw SQL string into tokens.
2. **Parsing** – build an Abstract Syntax Tree (AST) from the token stream.
3. **Evaluation** – interpret the AST into a list of database operations.
4. **Execution** – the engine executes those operations against tables,
   indexes, and transaction logs.

The core files are:

- **Lexing**: `internal/interpreter/lexer/lexer.go`
- **Parsing**: `internal/interpreter/parser/*.go`
  - `parser.go`, `expressions.go`, `statements.go`, `constructor.go` etc.
- **AST definitions**: `internal/ast/*.go`
  - `expressions.go`, `statements.go`
- **Evaluation**: `internal/interpreter/eval/*.go`
  - `eval.go`, `statements.go`, `expressions.go`, `helpers.go`, `filter.go`
- **Database operations**: `internal/database/operations/*.go`
  - `create.go`, `insert.go`, `update.go`, `delete.go`, `alter.go`,
    `drop.go`, `foreignKey.go`, `index.go`, etc.
- **Engine and transactions**:
  - `internal/database/engine/engine.go`
  - `internal/database/transaction/*.go`

When you “create your own flavour” of LSQL, you will typically:

- Reuse the lower layers (operations, engine, transaction system), and
- Adjust or extend the **lexing → AST → evaluation** pipeline.

---

## 3. What is a "flavour" of LSQL?

In this project, a _flavour_ of LSQL means:

> A consistent surface language that ultimately maps down to the same
> underlying database operations, but may offer different syntax, shortcuts,
> conventions, or restricted capabilities.

Examples of potential flavours:

- A **teaching** flavour that only supports `CREATE TABLE`, `INSERT`,
  `SELECT`, and `DELETE`, and bans `ALTER TABLE` and transactions.
- A **strict** flavour that enforces all identifiers to be lowercase or adds
  explicit `BEGIN TRANSACTION` / `END TRANSACTION` keywords.
- A **domain‑specific** flavour that adds pseudo‑statements like
  `UPSERT` that compile down to a combination of `SELECT` + `INSERT`/
  `UPDATE` under the hood.

In practice, your flavour is defined by three things:

1. The set of statements you accept.
2. The exact syntax of those statements (keywords, optional parts, etc.).
3. How those statements are interpreted into operations (semantics).

---

## 4. Designing your own LSQL flavour: step‑by‑step

This section is about **how** to go from “I want my own flavour” to actual
changes in this repository.

### 4.1. Clarify the flavour’s goals

Before touching any code, write down:

- Which features of core LSQL you want to **keep**.
- Which you want to **remove** or **forbid**.
- What you want to **add** (new statements, new expressions, new types).

Good questions to ask yourself:

- Do I need transactions, or is a single‑statement model enough?
- Do I care about foreign keys and indexes?
- Do I need stored procedures, or can I ignore them for now?
- Is my flavour meant for humans or for generated SQL from another tool?

Once you know this, you can map goals to code changes.

### 4.2. Decide on the surface syntax

Sketch the syntax of your flavour as if you were writing documentation for
its users. For each statement, define:

- The **canonical form**: required keywords and order.
- Any **optional clauses** (e.g. optional `WHERE`, optional `DEFAULT`).
- What combinations are **illegal**.

Use the integration tests as templates. For example, the existing LSQL
syntax for an `ALTER TABLE ... ADD COLUMN` statement looks like:

```sql
ALTER TABLE products ADD COLUMN price float DEFAULT 9.99;
ALTER TABLE products ADD COLUMN price float NULL;
ALTER TABLE products ADD COLUMN price float NOT NULL;
```

You can choose to:

- Keep this exactly as‑is, or
- Introduce alternative spellings like `ALTER TABLE ... ADD price FLOAT` and
  translate them to the internal AST form.

### 4.3. Update or extend the lexer

Location: `internal/interpreter/lexer/lexer.go`.

The lexer is responsible for turning raw text into tokens (identifiers,
keywords, numbers, strings, operators, punctuation). When you change or add
syntax, ask:

- Do I need **new keywords** (e.g. `UPSERT`, `RETURNING`)?
- Do I need **new operators** (e.g. `||` for string concatenation)?
- Do I need to support **different literal forms** (e.g. JSON literals)?

Actions:

1. Add new token types for any new keywords or operators.
2. Update the keyword map so the lexer recognizes them as keywords instead of
   generic identifiers.
3. Ensure the lexer can emit tokens in the order your new grammar expects.

Tip: Keep keyword additions backwards‑compatible when possible, so existing
tests continue to pass.

### 4.4. Extend the AST (if needed)

Location: `internal/ast/expressions.go`, `internal/ast/statements.go`.

The AST is the bridge between “text” and “meaning”. Each kind of
statement/expression has a Go struct representation here.

If your flavour introduces a new concept that is **not** expressible by
recombining existing nodes, you will:

1. Add a new AST type (e.g. `UpsertStatement`).
2. Add any supporting expression node types.
3. Ensure the parser constructs these nodes when it sees your new syntax.

If you are only **restricting** or **rephrasing** syntax (e.g. removing some
optional keywords), you might not need new AST types; you might only adjust
how existing nodes are built.

### 4.5. Update the parser grammar

Location: `internal/interpreter/parser/*.go`.

Relevant files:

- `parser.go` – entry points, common helpers.
- `statements.go` – statement‑level grammar
  (`CREATE TABLE`, `INSERT`, `SELECT`, `ALTER TABLE`, etc.).
- `expressions.go` – expression grammar and precedence rules.

For each statement you want to add or reshape:

1. Find the existing parse function (e.g. `parseInsert`, `parseSelect`).
2. Adjust the grammar to accept your new flavour’s syntax.
3. Construct the appropriate AST nodes.

For example, if you add an `UPSERT` statement, you might:

- Add `UPSERT` as a keyword in the lexer.
- Add a `parseUpsert` function in `statements.go`.
- Wire it into the main dispatcher (the place where the parser decides which
  kind of statement it is seeing based on the first keyword).

Expression changes (precedence rules, new operators) live in
`parser/expressions.go` and must remain consistent with the
`internal/ast/expressions.go` definitions.

### 4.6. Implement semantics in the evaluator

Location: `internal/interpreter/eval/*.go`.

Once the parser builds an AST, the evaluator turns it into
`operations.Operation` values that the engine understands.

Key files:

- `eval.go` – evaluation entry points.
- `statements.go` – evaluation of high‑level statements.
- `expressions.go` – evaluation of expression AST nodes.
- `helpers.go`, `filter.go`, `operations.go` – helper utilities.

For each new AST node or statement kind, you will:

1. Add a branch in the evaluator that recognizes the node.
2. Translate it into the appropriate sequence of low‑level operations
   (`createTableOp`, `insertOp`, `updateOp`, etc.), often defined under
   `internal/database/operations`.
3. Respect existing semantics from the tests where applicable
   (nullability rules, foreign key behaviour, expression precedence, etc.).

If your flavour intentionally changes semantics (for example, a different
default for `NULL` handling), document this clearly and—ideally—add new
tests that capture your intended behaviour.

### 4.7. Wire semantics to database operations

Location: `internal/database/operations/*.go`.

In most cases, you **reuse** these operations as‑is. You only need to touch
them when your flavour introduces behaviour that the current operations do
not support—for example:

- A new constraint type.
- A new DDL operation that changes on‑disk layout.

Use the existing operations as a model. For example:

- `alter.go` shows how `ALTER TABLE` is implemented.
- `foreignKey.go` shows how foreign key constraints are stored and enforced.
- `update.go`, `insert.go`, `delete.go` show how row‑level changes are
  applied.

### 4.8. Transactions and isolation

Location: `internal/database/transaction/*.go`, `internal/database/wal/wal.go`,
`internal/database/engine/engine.go`.

If your flavour changes how transactions are expressed in SQL
(for example, you add `BEGIN` / `END` or `SAVEPOINT`), you will need to:

1. Add the new statements at the parsing/evaluation layers.
2. Map them to the existing transaction API exposed by the engine.

The **behaviour** of transactions (commit, rollback, isolation guarantees)
is shared across flavours and lives mostly in these lower layers.

### 4.9. Add tests for your flavour

The existing integration tests are an excellent template. To test your
flavour:

1. Add new tests under `tests/integration/`.
2. Use helpers similar to `execute` (local engine) or `execRemote`
   (HTTP server) to send your new SQL statements.
3. Assert on both **data** (rows returned) and **errors** (when operations
   should fail).

Some ideas:

- A dedicated `*_flavour_test.go` file that covers only your flavour’s
  extensions.
- Tests that mirror the existing ones but use your new syntax.

---

## 5. Example: sketching a simple custom flavour

This is an illustrative example of how you might define a minimal teaching
flavour, `LSQL-Teach`, that:

- Allows only `CREATE TABLE`, `INSERT`, `SELECT`, and `DELETE`.
- Automatically wraps each statement in its own transaction.
- Bans `ALTER TABLE`, foreign keys, and indexes.

### 5.1. Define the rules

Document for users:

- Supported statements:
  - `CREATE TABLE ...`
  - `INSERT INTO ... VALUES ...`
  - `SELECT ... FROM ... [WHERE ...]`
  - `DELETE FROM ... WHERE ...`
- Unsupported statements:
  - Any `ALTER`, `DROP`, `CREATE INDEX`, `SHOW INDEXES`, stored procedures,
    explicit `BEGIN TRAN` / `COMMIT` / `ROLLBACK`.

### 5.2. Parser changes

- In the main statement dispatcher, return an error for unsupported
  first keywords (`ALTER`, `DROP`, `CREATE INDEX`, etc.).
- Ensure the grammar for supported statements is as simple as possible.

### 5.3. Evaluation and execution

- In the evaluator entry point, implicitly wrap each parsed statement in a
  transaction (similar to how `wrapSqlInCommitTransaction` works in
  `sql_test.go`).
- Reuse existing operations beneath.

### 5.4. Tests

- Add `tests/integration/teach_flavour_test.go`.
- Copy a subset of existing tests (`TestCreateTable`, `TestInsertRow`,
  `TestSelectRow`, `TestDropRow`) but:
  - Verify that attempting to use `ALTER TABLE` or `BEGIN TRAN` returns an
    error.

This pattern extends to more ambitious flavours—start by declaring what is
**in** and **out**, then let those decisions drive lexer/parser/evaluator
changes.

---

## 6. Checklist for building a new LSQL flavour

Use this checklist as you iterate:

1. **Specification**
   - [ ] Write down your flavour’s supported statements and syntax.
   - [ ] Decide which existing LSQL behaviours you rely on (e.g. foreign key
         semantics, transaction behaviour).

2. **Lexer** (`internal/interpreter/lexer/lexer.go`)
   - [ ] Add any new keywords or operators.
   - [ ] Ensure literals you rely on (strings, numbers, datetimes) are
         supported.

3. **AST** (`internal/ast/*.go`)
   - [ ] Add new node types only if necessary.
   - [ ] Reuse existing AST forms when possible.

4. **Parser** (`internal/interpreter/parser/*.go`)
   - [ ] Extend or restrict the grammar according to your spec.
   - [ ] Maintain coherent expression precedence rules.

5. **Evaluator** (`internal/interpreter/eval/*.go`)
   - [ ] Map each statement/AST node to concrete database operations.
   - [ ] Preserve or intentionally change semantics (and document it).

6. **Operations & Engine** (`internal/database/operations/*.go`,
   `internal/database/engine/engine.go`)
   - [ ] Reuse as much as possible.
   - [ ] Extend only when your flavour requires new behaviour.

7. **Transactions** (`internal/database/transaction/*.go`)
   - [ ] Decide how your flavour exposes transactions in SQL.
   - [ ] Map your syntax to the existing transaction API.

8. **Tests** (`tests/integration/*.go`)
   - [ ] Add positive tests for valid statements.
   - [ ] Add negative tests for forbidden/invalid statements.
   - [ ] Reuse existing helpers (`execute`, `execRemote`) where possible.

By following this guide, you can confidently evolve LSQL, define targeted
flavours for different use cases, and keep behaviour well‑specified and
tested via concrete examples.
