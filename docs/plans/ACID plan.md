ACID Implementation Plan for LiminalDB
1. Atomicity
Transaction Boundaries:
Introduce explicit transaction commands (BEGIN, COMMIT, ROLLBACK) in the SQL parser and AST (internal/ast/statements.go).
Operation Grouping:
Buffer changes in memory until COMMIT is called.
On ROLLBACK, discard all uncommitted changes.
Error Handling:
Ensure that partial failures revert all changes in a transaction (internal/database/operations).
2. Consistency
Constraint Enforcement:
Validate all constraints (primary key, foreign key, unique, type checks) before committing changes (internal/database/operations/create.go, internal/database/operations/update.go).
Schema Validation:
Ensure schema changes (ALTER, CREATE) do not violate existing data integrity.
3. Isolation
Locking Mechanism:
Implement row/table-level locks to prevent concurrent conflicting operations (internal/database).
Transaction Isolation Levels:
Start with a simple isolation level (e.g., READ COMMITTED), then expand to others (e.g., SERIALIZABLE).
Concurrent Access Control:
Queue or block conflicting operations until the current transaction completes.
4. Durability
Write-Ahead Logging (WAL):
Before applying changes, write them to a log file (logs/).
On crash/restart, replay the log to restore committed transactions.
Safe File Writes:
Use atomic file operations when writing tables (internal/database/operations).
Periodic Checkpoints:
Regularly flush in-memory changes to disk to minimize recovery time.



Steps to take: 
Design Transaction Manager:
Add a transaction manager module (e.g., internal/database/transaction/).

Update SQL Parser & AST:
Support transaction statements in AST (internal/ast/statements.go).

Implement WAL:
Create log writing and recovery logic (logs/).

Add Locking:
Implement basic locking in table/row operations.

Integrate with Existing Operations:
Refactor CRUD operations (internal/database/operations/update.go, internal/database/operations/create.go) to respect transaction boundaries and locks.

Test & Validate:
Add unit/integration tests (tests/) for transaction scenarios and crash recovery.