-- bsqldb_transactions.sql: Transaction handling via FreeTDS bsqldb.
-- Tests BEGIN TRAN / COMMIT / ROLLBACK behavior through a real TDS client.

DROP TABLE IF EXISTS e2e_tx_test
go

CREATE TABLE e2e_tx_test (
  id INT NOT NULL,
  label VARCHAR(100)
)
go

-- Transaction 1: INSERT + COMMIT.
BEGIN TRAN
go
INSERT INTO e2e_tx_test (id, label) VALUES (1, 'committed_row')
go
COMMIT
go

-- Verify committed data is visible.
SELECT id, label FROM e2e_tx_test WHERE label = 'committed_row'
go

-- Transaction 2: INSERT + ROLLBACK.
BEGIN TRAN
go
INSERT INTO e2e_tx_test (id, label) VALUES (2, 'rolled_back_row')
go
ROLLBACK
go

-- Marker for the Go test to find the boundary of the final SELECT.
SELECT 'final_select_marker' AS marker
go

-- The rolled-back row should NOT appear.
SELECT id, label FROM e2e_tx_test ORDER BY id
go

-- Transaction 3: Multiple statements in one transaction.
BEGIN TRAN
go
INSERT INTO e2e_tx_test (id, label) VALUES (10, 'batch_a')
go
INSERT INTO e2e_tx_test (id, label) VALUES (11, 'batch_b')
go
COMMIT
go

-- Both rows should be visible.
SELECT id, label FROM e2e_tx_test WHERE id >= 10 ORDER BY id
go

-- Cleanup.
DROP TABLE e2e_tx_test
go
