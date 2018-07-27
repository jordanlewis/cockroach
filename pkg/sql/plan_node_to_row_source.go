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
	"context"

	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type planNodeToRowSource struct {
	started bool
	running bool

	fastPath bool

	node        planNode
	params      runParams
	outputTypes []sqlbase.ColumnType

	out distsqlrun.ProcOutputHelper

	// run time state machine values
	ctx context.Context
	row sqlbase.EncDatumRow
}

func makePlanNodeToRowSource(source planNode, params runParams, fastPath bool) (*planNodeToRowSource, error) {
	nodeColumns := planColumns(source)

	types := make([]sqlbase.ColumnType, len(nodeColumns))
	for i := range nodeColumns {
		colTyp, err := sqlbase.DatumTypeToColumnType(nodeColumns[i].Typ)
		if err != nil {
			return nil, err
		}
		types[i] = colTyp
	}
	row := make(sqlbase.EncDatumRow, len(nodeColumns))

	return &planNodeToRowSource{
		node:        source,
		params:      params,
		outputTypes: types,
		row:         row,
		running:     true,
		fastPath:    fastPath,
	}, nil
}

var _ distsqlrun.InitableProcessor = &planNodeToRowSource{}

func (p *planNodeToRowSource) InitWithOutput(post *distsqlrun.PostProcessSpec, output distsqlrun.RowReceiver) {
	p.out.Init(post, p.outputTypes, p.params.EvalContext(), output)
}

func (p *planNodeToRowSource) OutputTypes() []sqlbase.ColumnType {
	return p.out.OutputTypes()
}

func (p *planNodeToRowSource) Start(ctx context.Context) context.Context {
	p.ctx = ctx
	p.params.ctx = ctx
	return ctx
}

func (p *planNodeToRowSource) internalClose() {
	if p.running {
		p.node.Close(p.ctx)
		p.running = false
	}
}

func (p *planNodeToRowSource) Next() (sqlbase.EncDatumRow, *distsqlrun.ProducerMetadata) {
	if !p.running {
		return nil, nil
	}
	if !p.started {
		p.started = true
		// This starts all of the nodes below this node.
		// It doesn't handle subqueries, which is a problem. We'll need to deal with
		// that somehow.
		if err := startExec(p.params, p.node); err != nil {
			return nil, &distsqlrun.ProducerMetadata{Err: err}
		}

		if p.fastPath {
			var count int
			// If our node is a "fast path node", it means that we're set up to just
			// return a row count. So trigger the fast path and return the row count as
			// a row with a single column.
			if fastPath, ok := p.node.(planNodeFastPath); ok {
				count, ok = fastPath.FastPathResults()
				if !ok {
					p.internalClose()
					return nil, nil
				}
			} else {
				next, err := p.node.Next(p.params)
				for ; next; next, err = p.node.Next(p.params) {
					// If we're tracking memory, clear the previous row's memory account.
					if p.params.extendedEvalCtx.ActiveMemAcc != nil {
						p.params.extendedEvalCtx.ActiveMemAcc.Clear(p.ctx)
					}
					count++
				}
				if err != nil {
					return nil, &distsqlrun.ProducerMetadata{Err: err}
				}
			}
			p.internalClose()
			return sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(count))}}, nil
		}
	}

	for p.running {
		valid, err := p.node.Next(p.params)
		if err != nil {
			p.internalClose()
			return nil, &distsqlrun.ProducerMetadata{Err: err}
		}
		if !valid {
			p.internalClose()
			return nil, nil
		}

		for i, datum := range p.node.Values() {
			p.row[i] = sqlbase.DatumToEncDatum(p.outputTypes[i], datum)
		}
		outRow, ok, err := p.out.ProcessRow(p.ctx, p.row)
		if err != nil {
			p.internalClose()
			return nil, &distsqlrun.ProducerMetadata{Err: err}
		}
		if !ok {
			p.internalClose()
		}
		if outRow != nil {
			return outRow, nil
		}
	}
	return nil, nil
}

func (p *planNodeToRowSource) ConsumerDone() {
	p.internalClose()
}

func (p *planNodeToRowSource) ConsumerClosed() {
	p.internalClose()
}

func (p *planNodeToRowSource) Run(ctx context.Context, wg *sync.WaitGroup) {
	if p.out.Output() == nil {
		panic("processor output not initialized for emitting rows")
	}
	ctx = p.Start(ctx)
	distsqlrun.Run(ctx, p, p.out.Output())
	p.internalClose()
	if wg != nil {
		wg.Done()
	}
}
