// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package translate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslate(t *testing.T) {
	tests := []struct {
		name   string
		oracle string
		crdb   string
		params map[int]string // expected bind variable mapping (nil if none)
	}{
		// DUAL → omit.
		{
			name:   "select from dual",
			oracle: "SELECT 1 FROM DUAL",
			crdb:   "SELECT 1",
		},
		{
			name:   "sysdate from dual",
			oracle: "SELECT SYSDATE FROM DUAL",
			crdb:   "SELECT now()::DATE",
		},
		{
			name:   "systimestamp from dual",
			oracle: "SELECT SYSTIMESTAMP FROM DUAL",
			crdb:   "SELECT now()",
		},

		// NVL → COALESCE.
		{
			name:   "nvl",
			oracle: "SELECT NVL(name, 'unknown') FROM employees",
			crdb:   "SELECT COALESCE(name, 'unknown') FROM employees",
		},

		// NVL2 → CASE WHEN.
		{
			name:   "nvl2",
			oracle: "SELECT NVL2(commission, salary + commission, salary) FROM employees",
			crdb:   "SELECT CASE WHEN commission IS NOT NULL THEN (salary + commission) ELSE salary END FROM employees",
		},

		// DECODE → CASE.
		{
			name:   "decode",
			oracle: "SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM users",
			crdb:   "SELECT CASE status WHEN 'A' THEN 'Active' WHEN 'I' THEN 'Inactive' ELSE 'Unknown' END FROM users",
		},
		{
			name:   "decode without default",
			oracle: "SELECT DECODE(status, 'A', 'Active') FROM users",
			crdb:   "SELECT CASE status WHEN 'A' THEN 'Active' END FROM users",
		},

		// ROWNUM → LIMIT.
		{
			name:   "rownum lte",
			oracle: "SELECT * FROM employees WHERE ROWNUM <= 10",
			crdb:   "SELECT * FROM employees LIMIT 10",
		},
		{
			name:   "rownum lt",
			oracle: "SELECT * FROM employees WHERE ROWNUM < 11",
			crdb:   "SELECT * FROM employees LIMIT 10",
		},
		{
			name:   "rownum with other conditions",
			oracle: "SELECT * FROM employees WHERE dept_id = 10 AND ROWNUM <= 5",
			crdb:   "SELECT * FROM employees WHERE (dept_id = 10) LIMIT 5",
		},
		{
			name:   "rownum left side of and",
			oracle: "SELECT * FROM employees WHERE ROWNUM <= 5 AND dept_id = 10",
			crdb:   "SELECT * FROM employees WHERE (dept_id = 10) LIMIT 5",
		},

		// Sequence operations.
		{
			name:   "nextval",
			oracle: "SELECT emp_seq.NEXTVAL FROM DUAL",
			crdb:   "SELECT nextval('emp_seq')",
		},
		{
			name:   "currval",
			oracle: "SELECT emp_seq.CURRVAL FROM DUAL",
			crdb:   "SELECT currval('emp_seq')",
		},
		{
			name:   "insert with nextval",
			oracle: "INSERT INTO employees (id, name) VALUES (emp_seq.NEXTVAL, 'John')",
			crdb:   "INSERT INTO employees (id, name) VALUES (nextval('emp_seq'), 'John')",
		},

		// TO_CHAR format translation.
		{
			name:   "to_char date",
			oracle: "SELECT TO_CHAR(hire_date, 'YYYY-MM-DD') FROM employees",
			crdb:   "SELECT to_char(hire_date, 'YYYY-MM-DD') FROM employees",
		},
		{
			name:   "to_char with oracle formats",
			oracle: "SELECT TO_CHAR(hire_date, 'DD-MON-RRRR') FROM employees",
			crdb:   "SELECT to_char(hire_date, 'DD-Mon-YYYY') FROM employees",
		},
		{
			name:   "to_char with rr",
			oracle: "SELECT TO_CHAR(created_at, 'DD/MM/RR HH24:MI:SS') FROM logs",
			crdb:   "SELECT to_char(created_at, 'DD/MM/YY HH24:MI:SS') FROM logs",
		},

		// TO_DATE.
		{
			name:   "to_date",
			oracle: "SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD') FROM DUAL",
			crdb:   "SELECT to_timestamp('2024-01-15', 'YYYY-MM-DD')::DATE",
		},

		// Bind variables → positional parameters.
		{
			name:   "bind variables",
			oracle: "SELECT * FROM employees WHERE dept_id = :dept AND salary > :min_sal",
			crdb:   "SELECT * FROM employees WHERE ((dept_id = $1) AND (salary > $2))",
			params: map[int]string{1: "dept", 2: "min_sal"},
		},
		{
			name:   "repeated bind variable",
			oracle: "SELECT * FROM employees WHERE dept_id = :dept OR manager_dept = :dept",
			crdb:   "SELECT * FROM employees WHERE ((dept_id = $1) OR (manager_dept = $1))",
			params: map[int]string{1: "dept"},
		},

		// Identifier lowercasing (Oracle uses uppercase unquoted identifiers).
		{
			name:   "uppercase identifiers",
			oracle: "SELECT EMPLOYEE_ID, FIRST_NAME FROM HR.EMPLOYEES WHERE DEPARTMENT_ID = 10",
			crdb:   "SELECT employee_id, first_name FROM hr.employees WHERE (department_id = 10)",
		},

		// INSERT.
		{
			name:   "insert",
			oracle: "INSERT INTO employees (id, name, salary) VALUES (1, 'John', 50000)",
			crdb:   "INSERT INTO employees (id, name, salary) VALUES (1, 'John', 50000)",
		},

		// UPDATE.
		{
			name:   "update",
			oracle: "UPDATE employees SET salary = 60000 WHERE id = 1",
			crdb:   "UPDATE employees SET salary = 60000 WHERE (id = 1)",
		},

		// DELETE.
		{
			name:   "delete",
			oracle: "DELETE FROM employees WHERE id = 1",
			crdb:   "DELETE FROM employees WHERE (id = 1)",
		},

		// CREATE SEQUENCE.
		{
			name:   "create sequence",
			oracle: "CREATE SEQUENCE emp_seq START WITH 1 INCREMENT BY 1",
			crdb:   "CREATE SEQUENCE emp_seq START WITH 1 INCREMENT BY 1",
		},

		// ROWNUM in SELECT list (rare but valid).
		{
			name:   "rownum in select list",
			oracle: "SELECT ROWNUM, name FROM employees",
			crdb:   "SELECT row_number() OVER (), name FROM employees",
		},

		// Complex query combining multiple Oracle features.
		{
			name: "complex query",
			oracle: "SELECT NVL(e.FIRST_NAME, 'N/A'), " +
				"DECODE(e.STATUS, 'A', 'Active', 'I', 'Inactive', 'Unknown'), " +
				"e.HIRE_DATE " +
				"FROM HR.EMPLOYEES e " +
				"WHERE e.DEPARTMENT_ID = :dept_id AND ROWNUM <= 20 " +
				"ORDER BY e.HIRE_DATE DESC",
			crdb: "SELECT COALESCE(e.first_name, 'N/A'), " +
				"CASE e.status WHEN 'A' THEN 'Active' WHEN 'I' THEN 'Inactive' ELSE 'Unknown' END, " +
				"e.hire_date " +
				"FROM hr.employees e " +
				"WHERE (e.department_id = $1) " +
				"ORDER BY e.hire_date DESC " +
				"LIMIT 20",
			params: map[int]string{1: "dept_id"},
		},

		// Subquery.
		{
			name: "subquery in where",
			oracle: "SELECT * FROM employees WHERE dept_id IN " +
				"(SELECT dept_id FROM departments WHERE location = 'NYC')",
			crdb: "SELECT * FROM employees WHERE (dept_id IN " +
				"(SELECT dept_id FROM departments WHERE (location = 'NYC')))",
		},

		// FOR UPDATE.
		{
			name:   "select for update",
			oracle: "SELECT * FROM employees WHERE id = 1 FOR UPDATE",
			crdb:   "SELECT * FROM employees WHERE (id = 1) FOR UPDATE",
		},

		// CASE expression (passthrough).
		{
			name:   "case expression",
			oracle: "SELECT CASE WHEN salary > 50000 THEN 'high' ELSE 'low' END FROM employees",
			crdb:   "SELECT CASE WHEN (salary > 50000) THEN 'high' ELSE 'low' END FROM employees",
		},

		// String concatenation.
		{
			name:   "string concat",
			oracle: "SELECT first_name || ' ' || last_name FROM employees",
			crdb:   "SELECT ((first_name || ' ') || last_name) FROM employees",
		},

		// IS NULL / IS NOT NULL.
		{
			name:   "is null",
			oracle: "SELECT * FROM employees WHERE manager_id IS NULL",
			crdb:   "SELECT * FROM employees WHERE (manager_id IS NULL)",
		},
		{
			name:   "is not null",
			oracle: "SELECT * FROM employees WHERE manager_id IS NOT NULL",
			crdb:   "SELECT * FROM employees WHERE (manager_id IS NOT NULL)",
		},

		// BETWEEN.
		{
			name:   "between",
			oracle: "SELECT * FROM employees WHERE salary BETWEEN 40000 AND 60000",
			crdb:   "SELECT * FROM employees WHERE (salary BETWEEN 40000 AND 60000)",
		},

		// EXISTS.
		{
			name: "exists",
			oracle: "SELECT * FROM departments d WHERE EXISTS " +
				"(SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
			crdb: "SELECT * FROM departments d WHERE EXISTS " +
				"(SELECT 1 FROM employees e WHERE (e.dept_id = d.id))",
		},

		// JOIN.
		{
			name: "join",
			oracle: "SELECT e.name, d.name FROM employees e " +
				"JOIN departments d ON e.dept_id = d.id",
			crdb: "SELECT e.name, d.name FROM employees e " +
				"JOIN departments d ON (e.dept_id = d.id)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Translate(tt.oracle)
			require.NoError(t, err)
			require.Equal(t, tt.crdb, result.SQL)
			if tt.params != nil {
				require.Equal(t, tt.params, result.Params)
			}
		})
	}
}

func TestTranslateColumnNames(t *testing.T) {
	tests := []struct {
		name     string
		oracle   string
		expected []string
	}{
		{
			name:     "trim function",
			oracle:   "SELECT TRIM('  hello  ') FROM DUAL",
			expected: []string{"TRIM"},
		},
		{
			name:     "nvl function",
			oracle:   "SELECT NVL(val, 0) FROM t",
			expected: []string{"NVL"},
		},
		{
			name:     "decode function",
			oracle:   "SELECT DECODE(status, 'A', 'Active') FROM t",
			expected: []string{"DECODE"},
		},
		{
			name:     "column ref",
			oracle:   "SELECT id, name FROM t",
			expected: []string{"ID", "NAME"},
		},
		{
			name:     "explicit alias",
			oracle:   "SELECT id AS employee_id FROM t",
			expected: []string{"EMPLOYEE_ID"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Translate(tt.oracle)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result.ColumnNames)
		})
	}
}

func TestTranslateOracleDateFormat(t *testing.T) {
	tests := []struct {
		oracle string
		pg     string
	}{
		{"YYYY-MM-DD", "YYYY-MM-DD"},
		{"DD-MON-YYYY", "DD-Mon-YYYY"},
		{"DD-MON-RRRR", "DD-Mon-YYYY"},
		{"DD/MM/RR", "DD/MM/YY"},
		{"YYYY-MM-DD HH24:MI:SS", "YYYY-MM-DD HH24:MI:SS"},
		{"DD-MONTH-YYYY", "DD-Month-YYYY"},
		{"DY, DD MON YYYY", "Dy, DD Mon YYYY"},
		{"DAY", "Day"},
		{"HH24:MI:SS.FF6", "HH24:MI:SS.US"},
		{"HH24:MI:SS.FF3", "HH24:MI:SS.MS"},
		{"HH12:MI:SS A.M.", "HH12:MI:SS AM"},
		// Case insensitive input.
		{"dd-mon-rrrr", "dd-Mon-YYYY"},
	}
	for _, tt := range tests {
		t.Run(tt.oracle, func(t *testing.T) {
			got := translateOracleDateFormat(tt.oracle)
			require.Equal(t, tt.pg, got)
		})
	}
}

func TestTranslateError(t *testing.T) {
	// Invalid SQL should produce a parse error.
	_, err := Translate("SELECTFROM")
	require.Error(t, err)
}
