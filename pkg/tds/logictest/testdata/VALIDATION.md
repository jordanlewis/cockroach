# TDS Logictest Validation Report

Generated: 2026-04-03

## Summary

| Metric | Count |
|--------|-------|
| Total test files | 34 |
| Total test directives (exec/query) | 729 |
| Passing directives | 663 (91%) |
| Error directives (expected failures) | 66 (9%) |
| Known behavioral divergences | 2 |
| Stale test comments | 1 |

## Error Categorization (66 total)

Errors are categorized by root cause and implementation effort. "Expected
errors" (bad syntax, missing tables) are separated from feature gaps.

### Expected Errors (correct behavior) — 8

These tests verify that the TDS frontend correctly returns errors for
invalid or impossible operations. They are NOT bugs.

| File | Error | What's Tested |
|------|-------|---------------|
| errors | parse error | Bad syntax produces parse error |
| errors | does not exist | Missing table produces error |
| errors | could not parse | Type mismatch in CONVERT |
| drop | does not exist | SELECT from dropped table errors |
| ground_truth_functions | invalid integer | Int64 min overflow in parser |
| ground_truth_types | invalid integer | Int64 min overflow in parser |
| ground_truth_system | custom error | RAISERROR sends error to client |
| ground_truth_system | error 18001 | RAISERROR with error number |

Note: The 2 "invalid integer" errors are a parser limitation (unary minus
on int64 max+1 overflows). Fixing requires special-casing the literal
`-9223372036854775808` in the lexer. Low priority.

### Intentional Feature Errors (parsed but unsupported) — 4

These features parse correctly but are intentionally blocked from execution.

| File | Error | Feature |
|------|-------|---------|
| ddl_extended | unsupported | CREATE PROCEDURE |
| ddl_extended | unsupported | CREATE FUNCTION |
| ddl_extended | unsupported | CREATE TRIGGER |
| tsql_control_flow | Test error | RAISERROR (Sybase syntax, working correctly) |

### Parser Feature Gaps — 43

These fail because the T-SQL parser cannot parse the syntax yet.
Grouped by feature area for convoy planning.

#### Subqueries and Nested SELECTs (18 errors)

The parser does not support parenthesized SELECT as an expression or
in FROM/WHERE clauses. This is the single largest gap.

| File | Count | Specific Features |
|------|-------|-------------------|
| select_extended | 7 | Subquery in WHERE (IN), scalar subquery, EXISTS |
| subqueries | 9 | Scalar, IN, correlated, EXISTS, NOT EXISTS, derived table, comparison, ANY/SOME, ALL |
| functions_extended | 1 | STRING_AGG with UNION ALL subquery in FROM |
| select_extended | 1 | Common Table Expression (WITH ... AS) |

**Implementation note:** Requires recursive descent into nested SELECT
statements. The parser currently stops at the first `(` that contains
SELECT. CTE support (WITH) is a separate parse path.

#### Set Operations (4 errors)

| File | Count | Features |
|------|-------|----------|
| select_extended | 4 | UNION, UNION ALL, INTERSECT, EXCEPT |

**Implementation note:** Requires recognizing set operation keywords after
a complete SELECT statement and parsing the second SELECT.

#### Window Functions and OVER clause (3 errors)

| File | Count | Features |
|------|-------|----------|
| select_extended | 3 | ROW_NUMBER() OVER, RANK() OVER, SUM() OVER with PARTITION BY |

**Implementation note:** Requires parsing OVER (...) clauses after
function calls, including PARTITION BY and ORDER BY within the window spec.

#### DML Extensions (7 errors)

| File | Count | Features |
|------|-------|----------|
| dml_extended | 1 | INSERT ... SELECT |
| dml_extended | 1 | MERGE statement |
| dml_extended | 3 | OUTPUT clause (INSERT, DELETE, UPDATE) |
| dml_extended | 1 | UPDATE ... FROM ... JOIN |
| dml_extended | 1 | DELETE ... JOIN |

**Implementation note:** INSERT...SELECT requires parsing SELECT after
VALUES position. MERGE is a complex new statement type. OUTPUT is a
clause modifier. UPDATE/DELETE with FROM/JOIN are T-SQL extensions to
standard DML.

#### CAST / TRY_CAST Syntax (2 errors)

| File | Count | Features |
|------|-------|----------|
| functions_extended | 1 | CAST(expr AS type) |
| functions_extended | 1 | TRY_CAST(expr AS type) |

**Implementation note:** The parser treats CAST as a function call, but
AS inside parentheses is a keyword and breaks expression parsing. Needs
special-case handling like CONVERT already has.

#### EXEC Statement and Stored Procedures (5 errors)

| File | Count | Features |
|------|-------|----------|
| system_metadata | 3 | EXEC sp_tables, sp_columns, sp_helptext |
| system_metadata | 1 | EXEC sp_executesql (dynamic SQL) |
| tsql_control_flow | 1 | EXEC with named parameters (@id = 1) |

**Implementation note:** EXEC without sp_help/sp_helpdb is not handled
by the catalog layer and falls through to the parser, which doesn't
support the EXEC statement. Catalog needs more sp_ entries or parser
needs general EXEC support.

#### APPLY Operators (2 errors)

| File | Count | Features |
|------|-------|----------|
| select_extended | 2 | CROSS APPLY, OUTER APPLY |

**Implementation note:** APPLY is a T-SQL-specific lateral join variant.
Translates to LATERAL in standard SQL.

#### PIVOT / UNPIVOT (2 errors)

| File | Count | Features |
|------|-------|----------|
| select_extended | 2 | PIVOT, UNPIVOT |

**Implementation note:** Complex T-SQL-specific table operators. Lower
priority — can often be rewritten with GROUP BY + CASE.

#### OFFSET-FETCH (1 error)

| File | Count | Features |
|------|-------|----------|
| select_extended | 1 | ORDER BY ... OFFSET n ROWS FETCH NEXT m ROWS ONLY |

**Implementation note:** SQL Server 2012+ pagination syntax. Translates
to LIMIT/OFFSET.

#### Control Flow Gaps (4 errors)

| File | Count | Features |
|------|-------|----------|
| tsql_control_flow | 1 | BEGIN TRY / BEGIN CATCH |
| tsql_control_flow | 1 | GOTO |
| tsql_control_flow | 1 | RETURN statement |
| tsql_control_flow | 1 | WAITFOR DELAY |

**Implementation note:** TRY/CATCH would require error handling in the
T-SQL executor. GOTO requires label tracking. RETURN is for stored
procedures. WAITFOR is scheduling.

#### THROW Statement (1 error)

| File | Count | Features |
|------|-------|----------|
| tsql_control_flow | 1 | THROW errnum, msg, state |

**Implementation note:** SQL Server 2012+ replacement for RAISERROR.
Simple parse + translate to error.

#### Table Variables and SELECT INTO (2 errors)

| File | Count | Features |
|------|-------|----------|
| temp_tables | 1 | DECLARE @t TABLE (...) |
| temp_tables | 1 | SELECT ... INTO #table FROM ... |

**Implementation note:** Table variables are T-SQL local table types.
SELECT INTO creates a table from a query result.

### Translation/Runtime Gaps — 11

These parse correctly but fail at execution due to missing translations
or CockroachDB limitations.

| File | Error | Root Cause |
|------|-------|------------|
| system_metadata | unrecognized configuration parameter | @@ROWCOUNT → current_setting() not supported in CRDB |
| system_metadata | lastval | @@IDENTITY → lastval() fails without prior nextval() |
| system_metadata | unknown function | SCOPE_IDENTITY() not translated |
| system_metadata | does not exist | sysobjects ORDER BY name — catalog doesn't translate ORDER BY refs |
| functions_extended | unknown signature | FORMAT(value, fmt) → to_char() signature mismatch |
| ground_truth_system | unexpected token | EXEC bad — not a real sp_, parser rejects |
| ground_truth_system | sysusers | sysusers table not in catalog layer |

## Known Behavioral Divergences (not errors, but incorrect results)

These tests pass but produce incorrect results compared to real
Sybase/SQL Server behavior:

1. **Transaction rollback doesn't undo DML** (ground_truth_transactions:118)
   - ROLLBACK after INSERT does not discard the inserted row
   - Test documents expected Sybase behavior in comments but asserts
     the current (wrong) CockroachDB behavior
   - Root cause: TDS frontend's ROLLBACK is syntactic only

2. **PATINDEX lossy translation** (functions_extended:43)
   - PATINDEX('%world%', str) → strpos(str, '%world%') — searches for
     literal '%' characters instead of treating them as wildcards
   - Returns 0 instead of expected match position
   - Test passes because it asserts the current (wrong) result

## Stale Comments

1. **functions:42-44** — Comment says "TDS frontend does not yet translate
   LEN→length" with a TODO. But LEN translation IS implemented and works
   (tested successfully in functions_extended:27). The commented-out test
   should be uncommented or removed.

## Recommended Convoy Prioritization (next wave)

Based on error count, driver compatibility impact, and implementation
complexity:

### Convoy A: Subqueries + Set Operations (HIGH priority, 22 errors)

Subqueries (18) and set operations (4) are the largest gap and block
the most real-world queries. All require the parser to handle nested
SELECT statements — implementing one enables the others.

**Estimated scope:** Parser change to support parenthesized SELECT
expressions, plus UNION/INTERSECT/EXCEPT as binary operators between
SELECT statements.

### Convoy B: CAST/TRY_CAST + OFFSET-FETCH (HIGH priority, 3 errors)

CAST is fundamental SQL that every driver uses. OFFSET-FETCH is common
pagination. Both are small, high-value parser changes.

**Estimated scope:** Special-case CAST(expr AS type) in expression
parser. Add OFFSET-FETCH to ORDER BY clause handling.

### Convoy C: DML Extensions (MEDIUM priority, 7 errors)

INSERT...SELECT, OUTPUT, UPDATE...FROM, DELETE...JOIN, MERGE.
Important for ETL and data migration workloads.

**Estimated scope:** INSERT...SELECT is small. OUTPUT and MERGE are
medium complexity. UPDATE/DELETE with FROM/JOIN need JOIN support in
DML statements.

### Convoy D: Window Functions (MEDIUM priority, 3 errors)

ROW_NUMBER, RANK, SUM OVER. Common in analytics queries and pagination
patterns.

**Estimated scope:** Parse OVER() clause after function calls. Translate
PARTITION BY and ORDER BY window specs.

### Convoy E: EXEC + System Procedures (LOW priority, 5 errors)

More sp_ procedures in catalog layer. General EXEC statement support.

**Estimated scope:** Add sp_tables, sp_columns, sp_helptext to catalog
regex. Consider general EXEC parsing.

### Convoy F: Remaining Parser Gaps (LOW priority, 9 errors)

APPLY (2), PIVOT/UNPIVOT (2), control flow (4), THROW (1).
Lower real-world impact.

### Runtime Fixes (standalone, 2 items)

1. @@ROWCOUNT implementation (track affected row count per statement)
2. Transaction rollback semantics (wire ROLLBACK to actual CRDB rollback)
