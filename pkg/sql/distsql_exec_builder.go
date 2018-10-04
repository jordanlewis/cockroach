// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type distExecBuilder struct{}

func (distExecBuilder) ConstructValues(rows [][]tree.TypedExpr, cols sqlbase.ResultColumns) (exec.Node, error) {
	panic("implement me")
}

func getScanOpToTableOrdinalMap(n *scanNode) []int {
	if n.colCfg.wantedColumns == nil {
		return nil
	}
	if n.colCfg.addUnwantedAsHidden {
		panic("addUnwantedAsHidden not supported")
	}
	res := make([]int, len(n.cols))
	for i := range res {
		res[i] = tableOrdinal(n.desc, n.cols[i].ID, n.colCfg.visibility)
	}
	return res
}

func (distExecBuilder) ConstructScan(
	table opt.Table,
	index opt.Index,
	needed exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	reverse bool,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	panic("implement me")
}

func (distExecBuilder) ConstructVirtualScan(table opt.Table) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructFilter(n exec.Node, filter tree.TypedExpr) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructSimpleProject(n exec.Node, cols []exec.ColumnOrdinal, colNames []string) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructRender(n exec.Node, exprs tree.TypedExprs, colNames []string) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructHashJoin(joinType sqlbase.JoinType, left, right exec.Node, onCond tree.TypedExpr) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.ColumnOrdinal,
	orderedGroupCols exec.ColumnOrdinalSet,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructScalarGroupBy(input exec.Node, aggregations []exec.AggInfo) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructDistinct(input exec.Node, distinctCols, orderedCols exec.ColumnOrdinalSet) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructSetOp(typ tree.UnionType, all bool, left, right exec.Node) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructSort(input exec.Node, ordering sqlbase.ColumnOrdering) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructIndexJoin(
	input exec.Node, table opt.Table, cols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input exec.Node,
	table opt.Table,
	index opt.Index,
	keyCols []exec.ColumnOrdinal,
	lookupCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructLimit(input exec.Node, limit, offset tree.TypedExpr) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) RenameColumns(input exec.Node, colNames []string) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructPlan(root exec.Node, subqueries []exec.Subquery) (exec.Plan, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructExplain(options *tree.ExplainOptions, plan exec.Plan) (exec.Node, error) {
	panic("implement me")
}

func (distExecBuilder) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	panic("implement me")
}
