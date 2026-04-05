-- bsqldb_system.sql: System queries and metadata via FreeTDS bsqldb.
-- Tests system variables, catalog queries, and SET commands against
-- CockroachDB's TDS frontend.

-- System variables.
SELECT @@VERSION
go

SELECT @@SERVERNAME AS server_name
go

SELECT @@SPID AS session_id
go

-- SET commands (common TDS client initialization).
SET ANSI_NULLS ON
go
SET QUOTED_IDENTIFIER ON
go
SET ANSI_PADDING ON
go
SET CONCAT_NULL_YIELDS_NULL ON
go
SET TEXTSIZE 2147483647
go

-- Verify queries still work after SET commands.
SELECT 1 AS after_set
go

-- Create a table for metadata queries.
DROP TABLE IF EXISTS e2e_meta_test
go

CREATE TABLE e2e_meta_test (
  id INT NOT NULL,
  name VARCHAR(100),
  created_at DATETIME
)
go

-- INFORMATION_SCHEMA queries.
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'e2e_meta_test'
go

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'e2e_meta_test'
ORDER BY ORDINAL_POSITION
go

-- Cleanup.
DROP TABLE e2e_meta_test
go
