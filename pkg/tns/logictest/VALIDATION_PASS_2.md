# TNS/Oracle Validation Pass #2 — Test-Fix Loop Baseline

**Date**: 2026-04-04
**Suite**: `pkg/tns/logictest/testdata/` (15 test files)
**Result**: 8 FAIL, 7 PASS — tests now encode correct Oracle behavior

## What Changed

Pass #1 documented bugs but encoded buggy behavior in expected outputs
(all tests PASS with wrong results). Pass #2 corrects the expected outputs
to reflect proper Oracle behavior, making tests FAIL until bugs are fixed.

## Current Test Status

| Test File | Status | Bug(s) Triggered |
|-----------|--------|------------------|
| basic | PASS | — |
| case_expr | PASS | — |
| dml | PASS | — |
| dual | PASS | — |
| joins | PASS | — |
| select | PASS | — |
| sequences | PASS | — |
| aggregates | **FAIL** | co-4ov (DECIMAL precision on AVG) |
| ddl | **FAIL** | co-0w4 (string quoting x2) |
| expressions | **FAIL** | co-4ov (division precision), co-0w4 (concatenation) |
| functions | **FAIL** | co-p6v (DECODE column name) |
| null_handling | **FAIL** | co-0w4 (x2), co-p6v (NVL, NVL2 column names) |
| string_ops | **FAIL** | co-0w4 (concatenation), co-p6v (TRIM column name) |
| subqueries | **FAIL** | co-4ov (AVG precision in FROM subquery) |
| types | **FAIL** | co-0w4 (VARCHAR2/CLOB quoting x2) |

## Active Bugs (3 distinct)

| Bead | Priority | Description | Instances | Fix Location |
|------|----------|-------------|-----------|--------------|
| co-0w4 | P1 | Multi-word string quoting | 8 | `executor.go:338` |
| co-4ov | P3 | DECIMAL precision overflow | 3 | `executor.go` / type mapping |
| co-p6v | P3 | Column naming leaks CRDB names | 5 | `executor.go` / column mapping |

## Resolved / Not-A-Bug

| Original Bead | Disposition |
|---------------|-------------|
| co-an5 | Not a bug — test data has employees in all depts, NOT IN correctly empty |
| co-7xi | Bug exists but current test data coincidentally produces correct result |
| co-118 | Confirmed (sequences) — not tested red since it would crash test runner |
| co-trr | Confirmed (function fallback) — not tested red since it affects parsing |

## Fixed Point Criteria

When all 15 test files PASS, the test-fix loop has reached fixed point.
Currently: **8 of 15 failing**.
