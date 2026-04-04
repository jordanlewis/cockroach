# TNS/Oracle Validation Pass #3 — Fixed Point Confirmed

**Date**: 2026-04-04
**Suite**: `pkg/tns/logictest/testdata/` (19 test files)
**Result**: 19 PASS, 0 FAIL — fixed point reached

## What Changed

Pass #2 corrected expected outputs and showed 8/15 failing on 3 bugs
(co-0w4, co-4ov, co-p6v). All three bugs were fixed in commit dfe0cfd8b99.
Pass #3 adds 4 new test files from jade (hq-8036) and confirms the full
suite at fixed point.

## New Test Files (from hq-8036)

| Test File | Coverage |
|-----------|----------|
| insert_select | INSERT ... SELECT with WHERE, column lists, expressions |
| derived_tables | Subquery in FROM, multi-table FROM, nested derived tables |
| pagination | FETCH FIRST n ROWS ONLY (Oracle 12c+), ROWNUM variants |
| format_models | TO_CHAR/TO_DATE with Oracle format model translation |

## Current Test Status

| Test File | Status | Notes |
|-----------|--------|-------|
| aggregates | PASS | co-4ov fixed |
| basic | PASS | — |
| case_expr | PASS | — |
| ddl | PASS | co-0w4 fixed |
| derived_tables | PASS | new (hq-8036) |
| dml | PASS | — |
| dual | PASS | — |
| expressions | PASS | co-0w4, co-4ov fixed |
| format_models | PASS | new (hq-8036) |
| functions | PASS | co-p6v fixed |
| insert_select | PASS | new (hq-8036) |
| joins | PASS | — |
| null_handling | PASS | co-0w4, co-p6v fixed |
| pagination | PASS | new (hq-8036) |
| select | PASS | — |
| sequences | PASS | — |
| string_ops | PASS | co-0w4, co-p6v fixed |
| subqueries | PASS | co-4ov fixed |
| types | PASS | co-0w4 fixed |

## Resolved Bugs

| Bead | Fix | Commit |
|------|-----|--------|
| co-0w4 | FmtExport for unquoted string output | dfe0cfd8b99 |
| co-4ov | apd.Decimal.Reduce + Text('f') for trailing zeros | dfe0cfd8b99 |
| co-p6v | Oracle column names from AST override CRDB names | dfe0cfd8b99 |

## Fixed Point Criteria

All 19 test files PASS. The test-fix loop has reached fixed point.
