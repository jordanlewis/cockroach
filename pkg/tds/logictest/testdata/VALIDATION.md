# TDS Logictest Validation Report

Generated: 2026-04-03 (updated: window functions, subqueries, OFFSET-FETCH now pass)

## Summary

| Metric | Count |
|--------|-------|
| Total test files | 34 |
| Total test directives (exec/query) | 736 |
| Passing directives | 686 (93%) |
| Error directives (expected failures) | 45 (6%) |
| Empty-result directives (parse OK, TDS wire gap) | 5 |
| Known behavioral divergences | 2 |
| Stale test comments | 1 |

## Error Categorization (45 total)

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

### Parser Feature Gaps — 26

These fail because the T-SQL parser cannot parse the syntax yet.
Grouped by feature area for convoy planning. (Down from 43: window
functions, subqueries in WHERE/SELECT/EXISTS, set operations,
OFFSET-FETCH, and CTE now parse correctly.)

#### Subqueries and Nested SELECTs (10 errors, down from 18)

Subqueries in WHERE (IN), scalar subqueries, and EXISTS now work in
select_extended. CTEs (WITH) parse correctly but return empty results
through the TDS wire protocol (see "TDS wire-protocol gaps" below).

| File | Count | Specific Features |
|------|-------|-------------------|
| subqueries | 9 | Scalar, IN, correlated, EXISTS, NOT EXISTS, derived table, comparison, ANY/SOME, ALL |
| functions_extended | 1 | STRING_AGG with UNION ALL subquery in FROM |

**Implementation note:** The parser now handles parenthesized SELECT in
WHERE, scalar subquery in SELECT list, and EXISTS. Remaining gaps are
in the `subqueries` test file (9 tests) and one in functions_extended.

#### Set Operations (0 errors, TDS wire gap)

UNION, UNION ALL, INTERSECT, and EXCEPT now parse and translate correctly.
However, compound SELECT results are not returned through the TDS wire
protocol (no ColMetaData in response). See "TDS wire-protocol gaps" below.

#### Window Functions and OVER clause (0 errors, FIXED)

ROW_NUMBER(), RANK(), and SUM() OVER with PARTITION BY now parse, translate,
and execute correctly through the TDS wire protocol. All 3 tests pass with
correct results in select_extended.

#### DML Extensions (0 errors — IMPLEMENTED)

All 7 DML extension features are now fully supported:
- INSERT...SELECT, MERGE (→ INSERT ON CONFLICT), OUTPUT clause
  (→ RETURNING) on INSERT/UPDATE/DELETE, UPDATE...FROM...JOIN,
  DELETE...JOIN (→ DELETE USING). See dml_extended tests.

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

#### OFFSET-FETCH (0 errors, FIXED)

OFFSET-FETCH now parses, translates, and executes correctly. The test in
select_extended passes with correct results.


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

### TDS Wire-Protocol Gaps — 5

These queries parse and translate correctly, but the TDS server does not
return ColMetaData in the response, producing empty result sets. The tests
are marked as `query` with empty expected output.

| File | Count | Features |
|------|-------|----------|
| select_extended | 4 | UNION, UNION ALL, INTERSECT, EXCEPT |
| select_extended | 1 | Common Table Expression (WITH ... AS) |

**Root cause:** The TDS server likely sends only a DONE token without
column metadata for compound SELECT and CTE statements. Needs investigation
in the TDS server execution path.

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

### Convoy A: Remaining Subqueries (HIGH priority, 10 errors)

Subqueries in `subqueries` test file (9) and one in functions_extended.
The parser now handles subqueries in select_extended, but the dedicated
subqueries test file has more complex patterns.

**Estimated scope:** Extend existing subquery support to cover derived
tables, comparison subqueries, ANY/SOME/ALL operators.

### Convoy B: CAST/TRY_CAST + OFFSET-FETCH (COMPLETE — 0 errors remaining)

CAST, TRY_CAST, and OFFSET-FETCH are now fully supported.

### Convoy C: TDS Wire-Protocol Gaps (HIGH priority, 5 empty-result tests)

UNION, INTERSECT, EXCEPT, and CTE parse and translate correctly but
return empty results through TDS. Fixing the wire protocol would
recover 5 tests immediately.

**Estimated scope:** Investigate why compound SELECT and CTE statements
don't return ColMetaData in TDS responses.

### Convoy D: DML Extensions (COMPLETE — 0 errors remaining)

All DML extension features (INSERT...SELECT, OUTPUT, UPDATE...FROM,
DELETE...JOIN, MERGE) are now implemented.

### Window Functions (COMPLETE — 0 errors remaining)

ROW_NUMBER(), RANK(), and SUM() OVER with PARTITION BY now parse,
translate, and execute correctly through the TDS wire protocol.

### Convoy E: EXEC + System Procedures (LOW priority, 5 errors)

More sp_ procedures in catalog layer. General EXEC statement support.

### Convoy F: Remaining Parser Gaps (LOW priority, 9 errors)

APPLY (2), PIVOT/UNPIVOT (2), control flow (4), THROW (1).
Lower real-world impact.

### DONE: Window Functions, OFFSET-FETCH, Subqueries in WHERE/SELECT

Window functions (ROW_NUMBER, RANK, SUM OVER with PARTITION BY),
OFFSET-FETCH pagination, subquery in WHERE (IN), scalar subquery,
and EXISTS now fully pass end-to-end through TDS.

### Runtime Fixes (standalone, 2 items)

1. @@ROWCOUNT implementation (track affected row count per statement)
2. Transaction rollback semantics (wire ROLLBACK to actual CRDB rollback)
