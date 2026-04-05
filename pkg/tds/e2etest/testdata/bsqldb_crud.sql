-- bsqldb_crud.sql: Basic CRUD operations via FreeTDS bsqldb.
-- Tests CREATE TABLE, INSERT, SELECT, UPDATE, DELETE against CockroachDB's
-- TDS frontend to verify end-to-end SQL execution through a real TDS client.

DROP TABLE IF EXISTS e2e_crud_test
go

CREATE TABLE e2e_crud_test (
  id INT NOT NULL,
  name VARCHAR(100),
  age INT
)
go

-- Insert multiple rows.
INSERT INTO e2e_crud_test (id, name, age) VALUES (1, 'Alice', 30)
go
INSERT INTO e2e_crud_test (id, name, age) VALUES (2, 'Bob', 25)
go
INSERT INTO e2e_crud_test (id, name, age) VALUES (3, 'Charlie', 35)
go

-- Select all rows ordered.
SELECT id, name, age FROM e2e_crud_test ORDER BY id
go

-- Update a row.
UPDATE e2e_crud_test SET age = 31 WHERE id = 1
go

-- Verify update.
SELECT name, age FROM e2e_crud_test WHERE id = 1
go

-- Delete a row.
DELETE FROM e2e_crud_test WHERE id = 3
go

-- Verify delete — should show 2 rows.
SELECT id, name FROM e2e_crud_test ORDER BY id
go

-- Multi-column SELECT with expressions.
SELECT id, name, age, age + 10 AS age_plus_10 FROM e2e_crud_test ORDER BY id
go

-- Cleanup.
DROP TABLE e2e_crud_test
go
