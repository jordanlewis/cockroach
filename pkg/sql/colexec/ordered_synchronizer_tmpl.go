// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for ordered_synchronizer.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"container/heap"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// {{/*
// Declarations to make the template compile properly.

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

type syncInputInfo struct {
	input        SynchronizerInput
	batch        coldata.Batch
	currentIndex int
}

type syncOrderingColInfo struct {
	col        sqlbase.ColumnOrderInfo
	comparator vecComparator
}

type syncOutputInfo struct {
	// outColIdx contains the position of the corresponding vector in the
	// slice for the same type. For example, if we have an output batch with
	// types = [Int64, Int64, Bool, Bytes, Bool, Int64], then the outColIdx for
	// each outputInfo will be:
	//                      [0, 1, 0, 0, 1, 2]
	//                       ^  ^  ^  ^  ^  ^
	//                       |  |  |  |  |  |
	//                       |  |  |  |  |  3rd among all Int64's
	//                       |  |  |  |  2nd among all Bool's
	//                       |  |  |  1st among all Bytes's
	//                       |  |  1st among all Bool's
	//                       |  2nd among all Int64's
	//                       1st among all Int64's
	outColIdx int
	nulls     *coldata.Nulls
	setter    orderedSyncSetter
}

// OrderedSynchronizer receives rows from multiple inputs and produces a single
// stream of rows, ordered according to a set of columns. The rows in each input
// stream are assumed to be ordered according to the same set of columns.
type OrderedSynchronizer struct {
	allocator             *colmem.Allocator
	typs                  []*types.T
	canonicalTypeFamilies []types.Family

	// inputInfo stores information for each input.
	inputInfo []syncInputInfo

	// orderingInfo stores information for each ordering column.
	orderingInfo []syncOrderingColInfo

	// heap is a min heap which stores indices into inputInfos. The "current
	// value" of ith input batch is the tuple at inputInfos.currentIndex[i] position of
	// inputInfos[i].batch. If an input is fully exhausted, it will be removed
	// from heap.
	heap   []int
	output coldata.Batch

	// In order to reduce the number of interface conversions, we will get access
	// to the underlying slice for the output vectors and will use them directly.
	// {{range .}}
	// {{range .WidthOverloads}}
	out_TYPECols []_GOTYPESLICE
	// {{end}}
	// {{end}}

	// inputInfo stores information for each outputColumn.
	outputInfo []syncOutputInfo
}

var _ colexecbase.Operator = &OrderedSynchronizer{}

// ChildCount implements the execinfrapb.OpNode interface.
func (o *OrderedSynchronizer) ChildCount(verbose bool) int {
	return len(o.inputInfo)
}

// Child implements the execinfrapb.OpNode interface.
func (o *OrderedSynchronizer) Child(nth int, verbose bool) execinfra.OpNode {
	return o.inputInfo[nth].input.Op
}

// NewOrderedSynchronizer creates a new OrderedSynchronizer.
func NewOrderedSynchronizer(
	allocator *colmem.Allocator,
	inputs []SynchronizerInput,
	typs []*types.T,
	ordering sqlbase.ColumnOrdering,
) (*OrderedSynchronizer, error) {
	orderingInfo := make([]syncOrderingColInfo, len(ordering))
	for i, col := range ordering {
		orderingInfo[i].col = col
	}
	inputInfo := make([]syncInputInfo, len(inputs))
	for i, input := range inputs {
		inputInfo[i].input = input
	}
	return &OrderedSynchronizer{
		allocator:             allocator,
		inputInfo:             inputInfo,
		orderingInfo:          orderingInfo,
		typs:                  typs,
		canonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(typs),
	}, nil
}

type orderedSyncSetter func(o *OrderedSynchronizer, vec coldata.Vec, colIdx int, srcRowIdx, outputIdx int)

// {{range .}}
// {{range .WidthOverloads}}
func set_TYPE(o *OrderedSynchronizer, vec coldata.Vec, colIdx int, srcRowIdx, outputIdx int) {
	srcCol := vec._TYPE()
	outCol := o.out_TYPECols[o.outputInfo[colIdx].outColIdx]
	v := execgen.UNSAFEGET(srcCol, srcRowIdx)
	execgen.SET(outCol, outputIdx, v)
}

// {{end}}
// {{end}}

// Next is part of the Operator interface.
func (o *OrderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	if o.heap == nil {
		o.heap = make([]int, 0, len(o.inputInfo))
		for i := range o.inputInfo {
			batch := o.inputInfo[i].input.Op.Next(ctx)
			o.inputInfo[i].batch = batch
			o.updateComparators(i)
			if batch.Length() > 0 {
				o.heap = append(o.heap, i)
			}
		}
		heap.Init(o)
	}
	o.output.ResetInternalBatch()
	outputIdx := 0
	o.allocator.PerformOperation(o.output.ColVecs(), func() {
		for outputIdx < coldata.BatchSize() {
			if o.Len() == 0 {
				// All inputs exhausted.
				break
			}

			minBatch := o.heap[0]
			// Copy the min row into the output.
			info := &o.inputInfo[minBatch]
			batch := info.batch
			srcRowIdx := info.currentIndex
			if sel := batch.Selection(); sel != nil {
				srcRowIdx = sel[srcRowIdx]
			}
			for i, info := range o.outputInfo {
				vec := batch.ColVec(i)
				if vec.Nulls().NullAt(srcRowIdx) {
					info.nulls.SetNull(outputIdx)
				} else {
					info.setter(o, vec, i, srcRowIdx, outputIdx)
				}
			}

			// Advance the input batch, fetching a new batch if necessary.
			if info.currentIndex+1 < o.inputInfo[minBatch].batch.Length() {
				info.currentIndex++
			} else {
				info.batch = o.inputInfo[minBatch].input.Op.Next(ctx)
				info.currentIndex = 0
				o.updateComparators(minBatch)
			}
			if info.batch.Length() == 0 {
				heap.Remove(o, 0)
			} else {
				heap.Fix(o, 0)
			}

			outputIdx++
		}
	})

	o.output.SetLength(outputIdx)
	return o.output
}

// Init is part of the Operator interface.
func (o *OrderedSynchronizer) Init() {
	o.output = o.allocator.NewMemBatch(o.typs)
	o.outputInfo = make([]syncOutputInfo, len(o.typs))
	for i, outVec := range o.output.ColVecs() {
		o.outputInfo[i].nulls = outVec.Nulls()
		switch typeconv.TypeFamilyToCanonicalTypeFamily(o.typs[i].Family()) {
		// {{range .}}
		case _CANONICAL_TYPE_FAMILY:
			switch o.typs[i].Width() {
			// {{range .WidthOverloads}}
			case _TYPE_WIDTH:
				o.outputInfo[i].outColIdx = len(o.out_TYPECols)
				o.out_TYPECols = append(o.out_TYPECols, outVec._TYPE())
				o.outputInfo[i].setter = set_TYPE
				// {{end}}
			}
		// {{end}}
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled type %s", o.typs[i]))
		}
	}
	for i := range o.inputInfo {
		o.inputInfo[i].input.Op.Init()
	}
	for i := range o.orderingInfo {
		typ := o.typs[o.orderingInfo[i].col.ColIdx]
		o.orderingInfo[i].comparator = GetVecComparator(typ, len(o.inputInfo))
	}
}

func (o *OrderedSynchronizer) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	var bufferedMeta []execinfrapb.ProducerMetadata
	for i := range o.inputInfo {
		bufferedMeta = append(bufferedMeta, o.inputInfo[i].input.MetadataSources.DrainMeta(ctx)...)
	}
	return bufferedMeta
}

func (o *OrderedSynchronizer) compareRow(batchIdx1 int, batchIdx2 int) int {
	inputInfo1 := o.inputInfo[batchIdx1]
	inputInfo2 := o.inputInfo[batchIdx2]
	valIdx1 := inputInfo1.currentIndex
	valIdx2 := inputInfo2.currentIndex
	if sel := inputInfo1.batch.Selection(); sel != nil {
		valIdx1 = sel[valIdx1]
	}
	if sel := inputInfo2.batch.Selection(); sel != nil {
		valIdx2 = sel[valIdx2]
	}
	for i := range o.orderingInfo {
		info := &o.orderingInfo[i]
		res := info.comparator.compare(batchIdx1, batchIdx2, valIdx1, valIdx2)
		if res != 0 {
			switch d := info.col.Direction; d {
			case encoding.Ascending:
				return res
			case encoding.Descending:
				return -res
			default:
				colexecerror.InternalError(fmt.Sprintf("unexpected direction value %d", d))
			}
		}
	}
	return 0
}

// updateComparators should be run whenever a new batch is fetched. It updates
// all the relevant vectors in o.comparators.
func (o *OrderedSynchronizer) updateComparators(batchIdx int) {
	batch := o.inputInfo[batchIdx].batch
	if batch.Length() == 0 {
		return
	}
	for _, info := range o.orderingInfo {
		vec := batch.ColVec(info.col.ColIdx)
		info.comparator.setVec(batchIdx, vec)
	}
}

// Len is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Len() int {
	return len(o.heap)
}

// Less is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Less(i, j int) bool {
	return o.compareRow(o.heap[i], o.heap[j]) < 0
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Swap(i, j int) {
	o.heap[i], o.heap[j] = o.heap[j], o.heap[i]
}

// Push is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Push(x interface{}) {
	o.heap = append(o.heap, x.(int))
}

// Pop is part of heap.Interface and is only meant to be used internally.
func (o *OrderedSynchronizer) Pop() interface{} {
	x := o.heap[len(o.heap)-1]
	o.heap = o.heap[:len(o.heap)-1]
	return x
}
