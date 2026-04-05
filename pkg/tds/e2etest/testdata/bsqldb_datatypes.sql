-- bsqldb_datatypes.sql: Data type coverage via FreeTDS bsqldb.
-- Tests that CockroachDB's TDS frontend correctly handles various SQL Server
-- and Sybase data types when queried through a real FreeTDS client.

DROP TABLE IF EXISTS e2e_types_test
go

CREATE TABLE e2e_types_test (
  id INT NOT NULL,
  col_varchar VARCHAR(200),
  col_int INT,
  col_bigint BIGINT,
  col_smallint SMALLINT,
  col_tinyint TINYINT,
  col_bit BIT,
  col_float FLOAT,
  col_real REAL,
  col_decimal DECIMAL(10, 2)
)
go

-- Insert a row with various types.
INSERT INTO e2e_types_test (
  id, col_varchar, col_int, col_bigint, col_smallint,
  col_tinyint, col_bit, col_float, col_real, col_decimal
) VALUES (
  1, 'hello world', 42, 9999999999, 100,
  200, 1, 3.14159, 2.718, 12345.67
)
go

-- Insert a row with NULL values.
INSERT INTO e2e_types_test (
  id, col_varchar, col_int, col_bigint, col_smallint,
  col_tinyint, col_bit, col_float, col_real, col_decimal
) VALUES (
  2, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL
)
go

-- Select all data back.
SELECT id, col_varchar, col_int, col_bigint, col_smallint,
       col_tinyint, col_bit, col_float, col_real, col_decimal
FROM e2e_types_test
ORDER BY id
go

-- Test string functions.
SELECT LEN('hello world') AS str_len
go

SELECT UPPER('hello') AS upper_result
go

SELECT SUBSTRING('hello world', 1, 5) AS sub_result
go

-- Test numeric expressions.
SELECT CAST(42 AS BIGINT) AS cast_bigint
go

SELECT CONVERT(VARCHAR(10), 12345) AS convert_result
go

-- Cleanup.
DROP TABLE e2e_types_test
go
