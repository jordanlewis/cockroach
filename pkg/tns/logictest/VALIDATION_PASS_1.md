# TNS/Oracle Validation Pass #1 — Error Report

**Date**: 2026-04-04
**Suite**: `pkg/tns/logictest/testdata/` (15 test files)
**Result**: All tests PASS (expected outputs encode current behavior, including bugs)

## Summary

| Metric | Count |
|--------|-------|
| Test files audited | 15 |
| Test files clean (no bugs) | 5 (basic, case_expr, dml, joins, dual) |
| Test files with bugs | 10 |
| Distinct bug categories | 7 |
| Total error instances | ~25 |

## Bug Categories

### 1. Multi-word string quoting — `co-0w4` (P1, ~10 instances)

**Root cause**: `executor.go:338` — `tree.FmtBareStrings` adds single quotes
around strings containing spaces in wire-encoded results.

| File | Line | Example |
|------|------|---------|
| types | 89 | `'world with spaces'` |
| types | 111 | `'a long description'` |
| ddl | 23 | `'a description'` |
| ddl | 110 | `'unicode description'` |
| expressions | 40 | `'hello world'` (concatenation) |
| null_handling | 37 | `'also present'` |
| null_handling | 46 | `'also present'` (via NVL) |
| null_handling | 78 | `'has value'` (via NVL2) |
| string_ops | 48 | `'John Doe'` (concatenation) |

### 2. DECIMAL precision overflow — `co-4ov` (P3, ~3 instances)

**Root cause**: `NUMBER` maps to bare `DECIMAL`, producing CockroachDB's full
precision instead of Oracle-style `NUMBER` precision.

| File | Line | Example |
|------|------|---------|
| aggregates | 60 | `AVG(amount)` → `174.00000000000000000` |
| expressions | 26 | `10 / 2` → `5.0000000000000000000` |
| subqueries | 80 | `AVG(salary)` → `87500.000000000000000` |

### 3. Oracle function fallback gap — `co-trr` (P2, systemic)

**Root cause**: `executor.go:106-108` — when the Oracle parser fails, fallback
path (`translateDDLTypes`) only substitutes types, not function names.

Affected functions: `INSTR`, `LENGTHB`, `SYSDATE`/`SYSTIMESTAMP` in complex
expressions. Documented in comments in `functions` and `string_ops` test files.

### 4. Sequence operations broken — `co-118` (P1, all seq ops)

**Root cause**: `translate.go:596` emits `nextval('name')` but the internal
executor requires `nextval(regclass)`.

Only `CREATE SEQUENCE` / `DROP SEQUENCE` work (via DDL passthrough). All
`NEXTVAL`/`CURRVAL` operations fail. Tested in `sequences`.

### 5. NOT IN with subquery — `co-an5` (P2, 1 instance)

**Root cause**: Standard SQL NULL semantics — `NOT IN (SELECT ...)` returns
empty when the subquery could contain NULL values.

| File | Line | Detail |
|------|------|--------|
| subqueries | 62 | `NOT IN (SELECT DISTINCT dept_id ...)` → empty |

### 6. ROWNUM + AND compound WHERE — `co-7xi` (P2, 1 instance)

**Root cause**: `translate.go:245-256` — `extractRownumLimit` only handles
top-level AND, not nested AND chains.

| File | Line | Detail |
|------|------|--------|
| select | 144 | `WHERE age > 25 AND ROWNUM <= 2` → wrong rows |

### 7. Column naming / aliasing (P3, ~6 instances, NEW)

**Root cause**: Translated functions expose CockroachDB internal names instead
of Oracle-expected column names.

| File | Line | Got | Expected |
|------|------|----|----------|
| null_handling | 50 | `COALESCE` | `NVL` or `VAL` |
| null_handling | 71 | `COALESCE` | `NVL` or `SCORE` |
| null_handling | 82 | `CASE` | `NVL2` or expr-based |
| functions | 54 | `CASE` | `DECODE` |
| string_ops | 90 | `BTRIM` | `TRIM` |
| dual / expressions | various | `?COLUMN?` | expression text |

## Fix Convoy Priority

| Priority | Bead | Description | Impact |
|----------|------|-------------|--------|
| P1 | co-0w4 | Multi-word string quoting | All string data with spaces broken |
| P1 | co-118 | Sequence operations | All sequence ops unusable |
| P2 | co-trr | Function mapping fallback | Several Oracle functions silently fail |
| P2 | co-an5 | NOT IN with subquery | Standard Oracle pattern broken |
| P2 | co-7xi | ROWNUM + AND compound WHERE | Pagination with filters broken |
| P3 | co-4ov | DECIMAL precision | Cosmetic but noticeable |
| P3 | NEW | Column naming/aliasing | Cosmetic, affects tooling |

## Clean Test Files

These test files have no known bugs:

- **basic** — simple SELECT/INSERT/UPDATE/DELETE
- **case_expr** — CASE WHEN expressions (minor column naming, but acceptable)
- **dml** — INSERT, UPDATE, DELETE operations
- **joins** — INNER, LEFT, RIGHT, FULL, CROSS JOIN
- **dual** — DUAL pseudo-table (minor `?COLUMN?` naming)
