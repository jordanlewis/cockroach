# TDS Logictest Validation Report

Generated: 2026-04-04 (validation pass #2)

## Summary

| Metric | Count |
|--------|-------|
| Total test files | 34 |
| Total test directives (exec/query) | 735 |
| Passing directives | 703 (96%) |
| Error directives (expected failures) | 32 (4%) |
| Known behavioral divergences | 1 |

**Changes from pass #1 (2026-04-03):**
- TDS wire-protocol gap FIXED: UNION, UNION ALL, INTERSECT, EXCEPT, and
  CTE now return full result sets through TDS (5 tests recovered)
- IDENTITY column translation FIXED: removed conflicting NULL declaration
- Computed column translation gap FOUND: missing data type in AS clause
- TRY_CAST gap FOUND: parser/translator doesn't support TRY_CAST
- VALIDATION.md moved out of testdata/ (was being parsed as a test file)
- compute_by test ordering stabilized (added secondary sort to query)

## Error Categorization (32 total)

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
on int64 max+1 overflows). Low priority.

### Intentional Feature Errors (parsed but unsupported) — 8

These features parse correctly but are intentionally blocked or test
error-handling paths.

| File | Error | Feature |
|------|-------|---------|
| ddl_extended | unsupported | CREATE PROCEDURE |
| ddl_extended | unsupported | CREATE FUNCTION |
| ddl_extended | unsupported | CREATE TRIGGER |
| tsql_control_flow | Test error | RAISERROR (Sybase syntax, working correctly) |
| tsql_control_flow | Custom error | THROW (SQL Server 2012+, working correctly) |
| tsql_control_flow | division by zero | BEGIN TRY/CATCH (parsed, best-effort semantics) |
| select_extended | unsupported: PIVOT | PIVOT (parsed, translation not yet implemented) |
| select_extended | unsupported: UNPIVOT | UNPIVOT (parsed, translation not yet implemented) |

### System Metadata / EXEC Gaps — 10

System stored procedures, system variables, and catalog tables that are
not yet implemented.

| File | Error | Root Cause |
|------|-------|------------|
| system_metadata | lastval | @@IDENTITY → lastval() fails without prior nextval() |
| system_metadata | unknown function | SCOPE_IDENTITY() not translated |
| system_metadata | does not exist | sysobjects ORDER BY name — catalog gap |
| system_metadata | unsupported | EXEC sp_tables |
| system_metadata | unsupported | EXEC sp_columns |
| system_metadata | unsupported | EXEC sp_helptext |
| system_metadata | unsupported | EXEC sp_executesql (dynamic SQL) |
| tsql_control_flow | unsupported | EXEC with named parameters (@id = 1) |
| ground_truth_system | unsupported | EXEC bad (non-existent procedure) |
| ground_truth_system | sysusers | sysusers system table not in catalog |

### Parser / Translation Gaps — 6

These fail because the T-SQL parser cannot parse the syntax or the
translator produces invalid CockroachDB SQL.

| File | Error | Feature |
|------|-------|---------|
| temp_tables | parse error | DECLARE @t TABLE (...) — table variables |
| temp_tables | parse error | SELECT ... INTO #table — SELECT INTO |
| tsql_control_flow | parse error | GOTO label — label definitions |
| functions_extended | syntax error | TRY_CONVERT(type, value) |
| functions_extended | syntax error | TRY_CAST(value AS type) |
| types_extended | syntax error | Computed columns — missing data type in translation |

## Completed Features (previously errors, now passing)

### Set Operations and CTE (FIXED in pass #2 — was 5 wire-protocol gaps)

UNION, UNION ALL, INTERSECT, EXCEPT, and CTE (WITH ... AS) now return
full result sets through the TDS wire protocol. Previously these parsed
and translated correctly but the TDS server didn't return ColMetaData.

### IDENTITY Columns (FIXED in pass #2)

`INT IDENTITY(1,1)` translation now correctly omits the conflicting
NULL declaration. IDENTITY columns are implicitly NOT NULL in both
T-SQL and CockroachDB.

### Window Functions, OFFSET-FETCH, APPLY (previously fixed)

ROW_NUMBER(), RANK(), SUM() OVER, OFFSET-FETCH, CROSS APPLY, and
OUTER APPLY all pass end-to-end.

### DML Extensions (previously fixed)

INSERT...SELECT, MERGE, OUTPUT, UPDATE...FROM, DELETE...JOIN.

### Runtime Fixes (previously fixed)

@@ROWCOUNT, transaction rollback semantics.

## Known Behavioral Divergences

1. **PATINDEX lossy translation** (functions_extended:43)
   - PATINDEX('%world%', str) → strpos(str, '%world%') — searches for
     literal '%' characters instead of treating them as wildcards
   - Returns 0 instead of expected match position
   - Test passes because it asserts the current (wrong) result

## Actionable Gap Categories (for fix beads)

Prioritized by error count:

1. **System procedures EXEC** (5 errors) — sp_tables, sp_columns,
   sp_helptext, sp_executesql, EXEC with named params
2. **System variables and catalog** (4 errors) — @@IDENTITY, SCOPE_IDENTITY(),
   sysobjects, sysusers
3. **Parser gaps** (3 errors) — table variables, SELECT INTO, GOTO labels
4. **Translation gaps** (3 errors) — TRY_CAST, TRY_CONVERT, computed columns
