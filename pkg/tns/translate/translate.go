// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package translate converts Oracle SQL ASTs into CockroachDB-compatible SQL
// strings. It handles Oracle-specific constructs that have no direct
// CockroachDB counterpart:
//
//   - ROWNUM pseudo-column → LIMIT clause
//   - FROM DUAL → omitted FROM clause
//   - SYSDATE → now()::DATE
//   - SYSTIMESTAMP → now()
//   - NVL(a, b) → COALESCE(a, b)
//   - NVL2(a, b, c) → CASE WHEN a IS NOT NULL THEN b ELSE c END
//   - DECODE(expr, s1, r1, ..., def) → CASE expr WHEN s1 THEN r1 ... ELSE def END
//   - TO_CHAR(expr, fmt) with Oracle format model → to_char with PG format
//   - seq.NEXTVAL → nextval('seq')
//   - seq.CURRVAL → currval('seq')
//   - NUMBER → DECIMAL, VARCHAR2 → VARCHAR (via types package)
//   - :bind variables → $N positional parameters
package translate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/tns/parser"
	"github.com/cockroachdb/errors"
)

// Result holds the translated SQL string and any bind variable mapping.
type Result struct {
	// SQL is the CockroachDB-compatible SQL string.
	SQL string
	// Params maps positional parameter indices (1-based) to the original Oracle
	// bind variable names. For example, if the Oracle SQL had :emp_id and :dept,
	// Params might be {1: "emp_id", 2: "dept"}.
	Params map[int]string
	// ColumnNames contains the Oracle-friendly column names for SELECT columns,
	// derived from the original Oracle AST. For columns with explicit aliases, the
	// alias is used. For function calls like NVL, DECODE, TRIM, the Oracle function
	// name is used. This allows the TNS executor to override CockroachDB's column
	// names (e.g. COALESCE→NVL, CASE→DECODE, BTRIM→TRIM) in query results.
	ColumnNames []string
}

// Translate parses an Oracle SQL string and returns the equivalent
// CockroachDB SQL along with bind variable mappings.
func Translate(oracleSQL string) (Result, error) {
	stmt, err := parser.Parse(oracleSQL)
	if err != nil {
		return Result{}, errors.Wrap(err, "parsing Oracle SQL")
	}
	t := &translator{
		bindMap: make(map[string]int),
		params:  make(map[int]string),
	}
	sql, err := t.translateStmt(stmt)
	if err != nil {
		return Result{}, err
	}
	return Result{SQL: sql, Params: t.params, ColumnNames: t.columnNames}, nil
}

// translator walks an Oracle AST and emits CockroachDB SQL.
type translator struct {
	// bindMap maps Oracle bind variable names to their assigned positional
	// parameter index ($1, $2, ...). A bind variable that appears multiple
	// times reuses the same index.
	bindMap map[string]int
	// params is the reverse of bindMap: positional index → Oracle name.
	params map[int]string
	// nextParam is the next positional parameter index to assign.
	nextParam int
	// columnNames holds Oracle-friendly column names for the top-level SELECT.
	columnNames []string
}

// assignBind returns the positional parameter index for the given Oracle bind
// variable name, allocating a new index on first occurrence.
func (t *translator) assignBind(name string) int {
	if idx, ok := t.bindMap[name]; ok {
		return idx
	}
	t.nextParam++
	t.bindMap[name] = t.nextParam
	t.params[t.nextParam] = name
	return t.nextParam
}

// translateStmt dispatches translation for each statement type.
func (t *translator) translateStmt(stmt parser.Statement) (string, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return t.translateSelect(s)
	case *parser.InsertStmt:
		return t.translateInsert(s)
	case *parser.UpdateStmt:
		return t.translateUpdate(s)
	case *parser.DeleteStmt:
		return t.translateDelete(s)
	case *parser.CreateSequenceStmt:
		return t.translateCreateSequence(s)
	default:
		return "", errors.Newf("unsupported statement type: %T", stmt)
	}
}

// translateSelect translates a SELECT statement. It handles ROWNUM-based
// limits by extracting the limit value from WHERE clauses of the form
// "ROWNUM <= N" and emitting a LIMIT clause instead.
func (t *translator) translateSelect(s *parser.SelectStmt) (string, error) {
	var b strings.Builder
	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
	}

	// Extract Oracle-friendly column names for the top-level SELECT.
	if t.columnNames == nil {
		t.columnNames = make([]string, len(s.Columns))
		for i, c := range s.Columns {
			t.columnNames[i] = oracleColumnName(c)
		}
	}

	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		expr, err := t.translateExpr(c.Expr)
		if err != nil {
			return "", err
		}
		b.WriteString(expr)
		if c.Alias != "" {
			b.WriteString(" AS ")
			b.WriteString(strings.ToLower(c.Alias))
		}
	}

	// Emit FROM clause, omitting DUAL.
	nonDualTables := t.filterDual(s.From)
	if len(nonDualTables) > 0 {
		b.WriteString(" FROM ")
		for i, tbl := range nonDualTables {
			if i > 0 {
				b.WriteString(", ")
			}
			te, err := t.translateTableExpr(tbl)
			if err != nil {
				return "", err
			}
			b.WriteString(te)
		}
	}

	// Extract ROWNUM-based limit from WHERE clause.
	where, rownumLimit := t.extractRownumLimit(s.Where)
	if where != nil {
		expr, err := t.translateExpr(where)
		if err != nil {
			return "", err
		}
		b.WriteString(" WHERE ")
		b.WriteString(expr)
	}

	if len(s.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, g := range s.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			expr, err := t.translateExpr(g)
			if err != nil {
				return "", err
			}
			b.WriteString(expr)
		}
	}
	if s.Having != nil {
		expr, err := t.translateExpr(s.Having)
		if err != nil {
			return "", err
		}
		b.WriteString(" HAVING ")
		b.WriteString(expr)
	}
	if len(s.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range s.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			expr, err := t.translateExpr(o.Expr)
			if err != nil {
				return "", err
			}
			b.WriteString(expr)
			if o.Desc {
				b.WriteString(" DESC")
			}
		}
	}

	// Emit LIMIT: prefer explicit AST limit, fall back to ROWNUM-extracted limit.
	limit := s.Limit
	if limit == nil && rownumLimit != nil {
		limit = rownumLimit
	}
	if limit != nil {
		expr, err := t.translateExpr(limit)
		if err != nil {
			return "", err
		}
		b.WriteString(" LIMIT ")
		b.WriteString(expr)
	}
	if s.ForUpdate {
		b.WriteString(" FOR UPDATE")
	}
	return b.String(), nil
}

// filterDual returns table expressions with DualTableRef entries removed.
func (t *translator) filterDual(tables []parser.TableExpr) []parser.TableExpr {
	var result []parser.TableExpr
	for _, tbl := range tables {
		if _, ok := tbl.(*parser.DualTableRef); !ok {
			result = append(result, tbl)
		}
	}
	return result
}

// extractRownumLimit looks for a top-level "ROWNUM <= N" or "ROWNUM < N"
// condition in the WHERE clause. If found, it returns the remaining WHERE
// conditions (with the ROWNUM predicate removed) and the limit expression.
//
// For ROWNUM < N, the limit is N-1 (since ROWNUM is 1-based and "< 10" means
// rows 1..9, so LIMIT 9).
//
// This handles the common Oracle pattern:
//
//	SELECT * FROM t WHERE ROWNUM <= 10
//	→ SELECT * FROM t LIMIT 10
func (t *translator) extractRownumLimit(
	where parser.Expr,
) (remaining parser.Expr, limit parser.Expr) {
	if where == nil {
		return nil, nil
	}

	// Check if the entire WHERE is a ROWNUM comparison.
	if lim, ok := t.isRownumComparison(where); ok {
		return nil, lim
	}

	// Check top-level AND: ROWNUM predicate may be one operand.
	bin, ok := where.(*parser.BinaryExpr)
	if !ok || bin.Op != parser.OpAnd {
		return where, nil
	}
	if lim, ok := t.isRownumComparison(bin.Left); ok {
		return bin.Right, lim
	}
	if lim, ok := t.isRownumComparison(bin.Right); ok {
		return bin.Left, lim
	}
	return where, nil
}

// isRownumComparison checks whether expr is "ROWNUM <= N" or "ROWNUM < N"
// and returns the corresponding limit expression.
func (t *translator) isRownumComparison(expr parser.Expr) (parser.Expr, bool) {
	bin, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return nil, false
	}
	if _, isRownum := bin.Left.(*parser.RowNumExpr); !isRownum {
		return nil, false
	}
	switch bin.Op {
	case parser.OpLte:
		return bin.Right, true
	case parser.OpLt:
		// ROWNUM < N → LIMIT N-1
		if num, ok := bin.Right.(*parser.NumberLit); ok {
			var v int
			n, err := fmt.Sscanf(num.Value, "%d", &v)
			if err == nil && n == 1 {
				return &parser.NumberLit{Value: fmt.Sprintf("%d", v-1)}, true
			}
		}
		// If not a simple integer, fall through and keep the predicate.
		return nil, false
	default:
		return nil, false
	}
}

func (t *translator) translateInsert(s *parser.InsertStmt) (string, error) {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(t.translateTableRef(s.Table))
	if len(s.Columns) > 0 {
		b.WriteString(" (")
		b.WriteString(strings.Join(lowercaseSlice(s.Columns), ", "))
		b.WriteString(")")
	}
	if s.Select != nil {
		sel, err := t.translateSelect(s.Select)
		if err != nil {
			return "", err
		}
		b.WriteString(" ")
		b.WriteString(sel)
	} else {
		b.WriteString(" VALUES ")
		for i, row := range s.Values {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("(")
			for j, v := range row {
				if j > 0 {
					b.WriteString(", ")
				}
				expr, err := t.translateExpr(v)
				if err != nil {
					return "", err
				}
				b.WriteString(expr)
			}
			b.WriteString(")")
		}
	}
	return b.String(), nil
}

func (t *translator) translateUpdate(s *parser.UpdateStmt) (string, error) {
	var b strings.Builder
	b.WriteString("UPDATE ")
	b.WriteString(t.translateTableRef(s.Table))
	b.WriteString(" SET ")
	for i, a := range s.Assignments {
		if i > 0 {
			b.WriteString(", ")
		}
		expr, err := t.translateExpr(a.Value)
		if err != nil {
			return "", err
		}
		b.WriteString(strings.ToLower(a.Column))
		b.WriteString(" = ")
		b.WriteString(expr)
	}
	if s.Where != nil {
		expr, err := t.translateExpr(s.Where)
		if err != nil {
			return "", err
		}
		b.WriteString(" WHERE ")
		b.WriteString(expr)
	}
	return b.String(), nil
}

func (t *translator) translateDelete(s *parser.DeleteStmt) (string, error) {
	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(t.translateTableRef(s.Table))
	if s.Where != nil {
		expr, err := t.translateExpr(s.Where)
		if err != nil {
			return "", err
		}
		b.WriteString(" WHERE ")
		b.WriteString(expr)
	}
	return b.String(), nil
}

func (t *translator) translateCreateSequence(s *parser.CreateSequenceStmt) (string, error) {
	var b strings.Builder
	b.WriteString("CREATE SEQUENCE ")
	b.WriteString(strings.ToLower(s.Name))
	if s.StartWith != nil {
		fmt.Fprintf(&b, " START WITH %d", *s.StartWith)
	}
	if s.Increment != nil {
		fmt.Fprintf(&b, " INCREMENT BY %d", *s.Increment)
	}
	if s.MinValue != nil {
		fmt.Fprintf(&b, " MINVALUE %d", *s.MinValue)
	}
	if s.MaxValue != nil {
		fmt.Fprintf(&b, " MAXVALUE %d", *s.MaxValue)
	}
	if s.Cache != nil {
		fmt.Fprintf(&b, " CACHE %d", *s.Cache)
	}
	if s.Cycle {
		b.WriteString(" CYCLE")
	}
	return b.String(), nil
}

func (t *translator) translateTableExpr(te parser.TableExpr) (string, error) {
	switch tbl := te.(type) {
	case *parser.DualTableRef:
		// Should have been filtered out; emit nothing.
		return "", nil
	case *parser.TableRef:
		return t.translateTableRef(tbl), nil
	case *parser.SubqueryTableExpr:
		sub, err := t.translateSelect(tbl.Query)
		if err != nil {
			return "", err
		}
		result := "(" + sub + ")"
		if tbl.Alias != "" {
			result += " AS " + strings.ToLower(tbl.Alias)
		}
		return result, nil
	case *parser.JoinTableExpr:
		left, err := t.translateTableExpr(tbl.Left)
		if err != nil {
			return "", err
		}
		right, err := t.translateTableExpr(tbl.Right)
		if err != nil {
			return "", err
		}
		var b strings.Builder
		b.WriteString(left)
		b.WriteString(" ")
		b.WriteString(string(tbl.Type))
		b.WriteString(" ")
		b.WriteString(right)
		if tbl.Cond != nil {
			cond, err := t.translateExpr(tbl.Cond)
			if err != nil {
				return "", err
			}
			b.WriteString(" ON ")
			b.WriteString(cond)
		}
		return b.String(), nil
	default:
		return "", errors.Newf("unsupported table expression type: %T", te)
	}
}

func (t *translator) translateTableRef(ref *parser.TableRef) string {
	var b strings.Builder
	if ref.Schema != "" {
		b.WriteString(strings.ToLower(ref.Schema))
		b.WriteString(".")
	}
	b.WriteString(strings.ToLower(ref.Name))
	if ref.Alias != "" {
		b.WriteString(" ")
		b.WriteString(strings.ToLower(ref.Alias))
	}
	return b.String()
}

// translateExpr translates an Oracle expression to CockroachDB SQL.
func (t *translator) translateExpr(expr parser.Expr) (string, error) {
	switch e := expr.(type) {
	case *parser.ColumnRefExpr:
		if e.Table != "" {
			return strings.ToLower(e.Table) + "." + strings.ToLower(e.Column), nil
		}
		return strings.ToLower(e.Column), nil

	case *parser.NumberLit:
		return e.Value, nil

	case *parser.StringLit:
		escaped := strings.ReplaceAll(e.Value, "'", "''")
		return "'" + escaped + "'", nil

	case *parser.NullLit:
		return "NULL", nil

	case *parser.StarExpr:
		if e.Table != "" {
			return strings.ToLower(e.Table) + ".*", nil
		}
		return "*", nil

	case *parser.BindExpr:
		idx := t.assignBind(e.Name)
		return fmt.Sprintf("$%d", idx), nil

	case *parser.BinaryExpr:
		left, err := t.translateExpr(e.Left)
		if err != nil {
			return "", err
		}
		right, err := t.translateExpr(e.Right)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s %s %s)", left, e.Op, right), nil

	case *parser.UnaryExpr:
		inner, err := t.translateExpr(e.Expr)
		if err != nil {
			return "", err
		}
		if e.Op == parser.OpNot {
			return fmt.Sprintf("(NOT %s)", inner), nil
		}
		return fmt.Sprintf("(%s%s)", e.Op, inner), nil

	case *parser.IsNullExpr:
		inner, err := t.translateExpr(e.Expr)
		if err != nil {
			return "", err
		}
		if e.Not {
			return fmt.Sprintf("(%s IS NOT NULL)", inner), nil
		}
		return fmt.Sprintf("(%s IS NULL)", inner), nil

	case *parser.InExpr:
		inner, err := t.translateExpr(e.Expr)
		if err != nil {
			return "", err
		}
		not := ""
		if e.Not {
			not = " NOT"
		}
		if e.Subquery != nil {
			sub, err := t.translateSelect(e.Subquery)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("(%s%s IN (%s))", inner, not, sub), nil
		}
		vals := make([]string, len(e.Values))
		for i, v := range e.Values {
			s, err := t.translateExpr(v)
			if err != nil {
				return "", err
			}
			vals[i] = s
		}
		return fmt.Sprintf("(%s%s IN (%s))", inner, not, strings.Join(vals, ", ")), nil

	case *parser.BetweenExpr:
		inner, err := t.translateExpr(e.Expr)
		if err != nil {
			return "", err
		}
		low, err := t.translateExpr(e.Low)
		if err != nil {
			return "", err
		}
		high, err := t.translateExpr(e.High)
		if err != nil {
			return "", err
		}
		not := ""
		if e.Not {
			not = " NOT"
		}
		return fmt.Sprintf("(%s%s BETWEEN %s AND %s)", inner, not, low, high), nil

	case *parser.ExistsExpr:
		sub, err := t.translateSelect(e.Query)
		if err != nil {
			return "", err
		}
		return "EXISTS (" + sub + ")", nil

	case *parser.SubqueryExpr:
		sub, err := t.translateSelect(e.Query)
		if err != nil {
			return "", err
		}
		return "(" + sub + ")", nil

	case *parser.ParenExpr:
		inner, err := t.translateExpr(e.Expr)
		if err != nil {
			return "", err
		}
		return "(" + inner + ")", nil

	case *parser.CaseExpr:
		return t.translateCase(e)

	case *parser.FuncCallExpr:
		return t.translateFuncCall(e)

	// Oracle-specific expressions.
	case *parser.RowNumExpr:
		// Standalone ROWNUM reference outside of a WHERE comparison. This is
		// unusual but valid (e.g. "SELECT ROWNUM, ..."). Translate to a
		// row_number() window function as an approximation.
		return "row_number() OVER ()", nil

	case *parser.SysDateExpr:
		return "now()::DATE", nil

	case *parser.SysTimestampExpr:
		return "now()", nil

	case *parser.SequenceExpr:
		seqName := strings.ToLower(e.Sequence)
		switch e.Op {
		case parser.SeqNextVal:
			return fmt.Sprintf("nextval('%s')", seqName), nil
		case parser.SeqCurrVal:
			return fmt.Sprintf("currval('%s')", seqName), nil
		default:
			return "", errors.Newf("unknown sequence operation: %s", e.Op)
		}

	case *parser.NVLExpr:
		// NVL(a, b) → COALESCE(a, b)
		a, err := t.translateExpr(e.Expr)
		if err != nil {
			return "", err
		}
		b, err := t.translateExpr(e.Default)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("COALESCE(%s, %s)", a, b), nil

	case *parser.NVL2Expr:
		// NVL2(a, b, c) → CASE WHEN a IS NOT NULL THEN b ELSE c END
		a, err := t.translateExpr(e.Expr)
		if err != nil {
			return "", err
		}
		bVal, err := t.translateExpr(e.NotNullVal)
		if err != nil {
			return "", err
		}
		c, err := t.translateExpr(e.NullVal)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("CASE WHEN %s IS NOT NULL THEN %s ELSE %s END", a, bVal, c), nil

	case *parser.DecodeExpr:
		return t.translateDecode(e)

	default:
		return "", errors.Newf("unsupported expression type: %T", expr)
	}
}

// translateDecode translates Oracle DECODE into a CASE expression:
//
//	DECODE(expr, s1, r1, s2, r2, default)
//	→ CASE expr WHEN s1 THEN r1 WHEN s2 THEN r2 ELSE default END
func (t *translator) translateDecode(d *parser.DecodeExpr) (string, error) {
	var b strings.Builder
	operand, err := t.translateExpr(d.Expr)
	if err != nil {
		return "", err
	}
	b.WriteString("CASE ")
	b.WriteString(operand)
	for _, p := range d.Pairs {
		search, err := t.translateExpr(p.Search)
		if err != nil {
			return "", err
		}
		result, err := t.translateExpr(p.Result)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, " WHEN %s THEN %s", search, result)
	}
	if d.Default != nil {
		def, err := t.translateExpr(d.Default)
		if err != nil {
			return "", err
		}
		b.WriteString(" ELSE ")
		b.WriteString(def)
	}
	b.WriteString(" END")
	return b.String(), nil
}

func (t *translator) translateCase(c *parser.CaseExpr) (string, error) {
	var b strings.Builder
	b.WriteString("CASE")
	if c.Operand != nil {
		op, err := t.translateExpr(c.Operand)
		if err != nil {
			return "", err
		}
		b.WriteString(" ")
		b.WriteString(op)
	}
	for _, w := range c.Whens {
		cond, err := t.translateExpr(w.Cond)
		if err != nil {
			return "", err
		}
		result, err := t.translateExpr(w.Result)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, " WHEN %s THEN %s", cond, result)
	}
	if c.Else != nil {
		els, err := t.translateExpr(c.Else)
		if err != nil {
			return "", err
		}
		b.WriteString(" ELSE ")
		b.WriteString(els)
	}
	b.WriteString(" END")
	return b.String(), nil
}

// translateFuncCall translates a function call, applying Oracle→CRDB function
// name mappings and format model translations where applicable.
func (t *translator) translateFuncCall(f *parser.FuncCallExpr) (string, error) {
	name := strings.ToUpper(f.Name)

	// Handle TO_CHAR with format model translation.
	if name == "TO_CHAR" && len(f.Args) == 2 {
		return t.translateToChar(f.Args[0], f.Args[1])
	}
	if name == "TO_DATE" && len(f.Args) == 2 {
		return t.translateToDate(f.Args[0], f.Args[1])
	}

	// Map Oracle function names to CRDB equivalents.
	crdbName := mapFuncName(name)

	args := make([]string, len(f.Args))
	for i, a := range f.Args {
		s, err := t.translateExpr(a)
		if err != nil {
			return "", err
		}
		args[i] = s
	}
	return crdbName + "(" + strings.Join(args, ", ") + ")", nil
}

// mapFuncName maps Oracle built-in function names to CockroachDB equivalents.
func mapFuncName(oracleName string) string {
	switch oracleName {
	case "NVL":
		return "COALESCE"
	case "LENGTHB":
		return "octet_length"
	case "SUBSTR":
		return "substring"
	case "INSTR":
		return "strpos"
	default:
		return strings.ToLower(oracleName)
	}
}

// translateToChar handles TO_CHAR(expr, fmt) by translating Oracle date format
// models to PostgreSQL/CockroachDB format models.
//
// Oracle and PostgreSQL share many format elements but have key differences:
//
//	Oracle        → PostgreSQL
//	YYYY            YYYY          (same)
//	MM              MM            (same)
//	DD              DD            (same)
//	HH24            HH24          (same)
//	HH / HH12      HH12          (same)
//	MI              MI            (same)
//	SS              SS            (same)
//	MON             Mon           (case-sensitive in PG)
//	MONTH           Month
//	DY              Dy
//	DAY             Day
//	AM / PM         AM
//	FF[1-9]         US            (fractional seconds → microseconds)
//	RR              YY            (Oracle 2-digit year pivot)
//	RRRR            YYYY          (Oracle 4-digit with RR pivot semantics)
func (t *translator) translateToChar(expr parser.Expr, fmtExpr parser.Expr) (string, error) {
	inner, err := t.translateExpr(expr)
	if err != nil {
		return "", err
	}

	// If the format is a string literal, translate the format model inline.
	if lit, ok := fmtExpr.(*parser.StringLit); ok {
		pgFmt := translateOracleDateFormat(lit.Value)
		return fmt.Sprintf("to_char(%s, '%s')", inner, pgFmt), nil
	}

	// Non-literal format: translate at runtime (pass through).
	fmtStr, err := t.translateExpr(fmtExpr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("to_char(%s, %s)", inner, fmtStr), nil
}

// translateToDate handles TO_DATE(str, fmt).
func (t *translator) translateToDate(expr parser.Expr, fmtExpr parser.Expr) (string, error) {
	inner, err := t.translateExpr(expr)
	if err != nil {
		return "", err
	}
	if lit, ok := fmtExpr.(*parser.StringLit); ok {
		pgFmt := translateOracleDateFormat(lit.Value)
		return fmt.Sprintf("to_timestamp(%s, '%s')::DATE", inner, pgFmt), nil
	}
	fmtStr, err := t.translateExpr(fmtExpr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("to_timestamp(%s, %s)::DATE", inner, fmtStr), nil
}

// oracleColumnName derives the Oracle-style column name for a SELECT column.
// If the column has an explicit alias, that alias is used. Otherwise the name
// is derived from the expression: function calls use the Oracle function name
// (e.g. NVL, DECODE, TRIM), column references use the column name, etc.
func oracleColumnName(col parser.SelectColumn) string {
	if col.Alias != "" {
		return strings.ToUpper(col.Alias)
	}
	return oracleExprName(col.Expr)
}

// oracleExprName returns a short Oracle-style name for the given expression.
// This is used for column naming when no explicit alias is provided.
func oracleExprName(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.ColumnRefExpr:
		return strings.ToUpper(e.Column)
	case *parser.FuncCallExpr:
		return strings.ToUpper(e.Name)
	case *parser.NVLExpr:
		return "NVL"
	case *parser.NVL2Expr:
		return "NVL2"
	case *parser.DecodeExpr:
		return "DECODE"
	default:
		// For StarExpr, CaseExpr, literals, and other expressions, return ""
		// so the CRDB-derived column name is used instead. CRDB often infers
		// a more useful name (e.g. a column name from the ELSE branch of CASE).
		return ""
	}
}

// lowercaseSlice returns a new slice with all strings lowercased.
func lowercaseSlice(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strings.ToLower(s)
	}
	return out
}
