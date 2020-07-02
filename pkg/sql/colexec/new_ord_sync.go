// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type ordSync struct {
	inputs         []SynchronizerInput
	spooledBatches []coldata.Batch
	typs           []*types.T

	tuplesNeeded   []int
	curIdxPerBatch []int

	sorter *sortOp
	spool  *simpleSpooler

	sortBatch   coldata.Batch
	tmpBatch    coldata.Batch
	outputBatch *projectingBatch
	allocator   *colmem.Allocator
	streamIdCol int
}

func NewOrdSync(
	allocator *colmem.Allocator,
	inputs []SynchronizerInput,
	typs []*types.T,
	ordering []execinfrapb.Ordering_Column,
) (*ordSync, error) {
	var err error
	projection := make([]uint32, len(typs))
	for i := range projection {
		projection[i] = uint32(i)
	}

	spool := &simpleSpooler{}
	// We add an extra int2 column to keep track of which stream each tuple came
	// from.
	typs = append(typs, types.Int2)
	sorter, err := newSorter(allocator, spool, typs, ordering)
	if err != nil {
		return nil, err
	}
	// Now, project the extra int2 column back away.
	outputBatch := newProjectionBatch(projection)
	return &ordSync{allocator: allocator, inputs: inputs, spool: spool, sorter: sorter, typs: typs,
			outputBatch: outputBatch},
		nil
}

func (o *ordSync) Init() {
	o.tuplesNeeded = make([]int, len(o.inputs))
	o.curIdxPerBatch = make([]int, len(o.inputs))
	for _, op := range o.inputs {
		op.Op.Init()
	}
	o.sortBatch = o.allocator.NewMemBatchWithSize(o.typs, len(o.inputs)*coldata.BatchSize())
	o.tmpBatch = o.allocator.NewMemBatchWithSize(o.typs, len(o.inputs)*coldata.BatchSize())
}

func (o *ordSync) Next(ctx context.Context) coldata.Batch {
	if o.spooledBatches == nil {
		// First call: spool a batch from all inputs and ask for tuples from each
		// batch.
		o.spooledBatches = make([]coldata.Batch, len(o.inputs))
		for i, input := range o.inputs {
			batch := input.Op.Next(ctx)
			idxCol := coldata.NewMemColumn(types.Int2, coldata.BatchSize(), coldata.StandardColumnFactory)
			ints := idxCol.Int16()
			for j := range ints {
				ints[j] = int16(i)
			}
			batch.AppendCol(idxCol)
			streamIdCol := batch.Width() - 1
			if o.streamIdCol == 0 {
				o.streamIdCol = streamIdCol
			} else {
				if o.streamIdCol != streamIdCol {
					panic("not equal batch widths? not good")
				}
			}
			o.spooledBatches[i] = batch
			o.tuplesNeeded[i] = coldata.BatchSize()
		}
	}

	// Fill up batch to sort with the needed amount of tuples from each batch.
	sortBatchSize := o.sortBatch.Length()
	for i, tuplesToFill := range o.tuplesNeeded {
		filledTuples := 0
		for filledTuples < tuplesToFill {
			batchLen := o.spooledBatches[i].Length()
			curIdx := o.curIdxPerBatch[i]
			if batchLen-curIdx <= 0 {
				// The ith stream's current batch is out of tuples and we still haven't
				// refilled all of the tuples we need, so fetch the next batch from the
				// ith stream.
				o.spooledBatches[i] = o.inputs[i].Op.Next(ctx)
				o.curIdxPerBatch[i] = 0
				curIdx = 0
				batchLen = o.spooledBatches[i].Length()
				if batchLen == 0 {
					// The ith stream ran out of tuples, so we'll move onto the next stream.
					break
				}
			}
			sel := o.spooledBatches[i].Selection()
			endIdx := curIdx + tuplesToFill - filledTuples
			if endIdx > batchLen {
				endIdx = batchLen
			}
			for j := range o.typs {
				o.sortBatch.ColVec(j).Append(coldata.SliceArgs{
					Src:         o.spooledBatches[i].ColVec(j),
					Sel:         sel,
					DestIdx:     sortBatchSize + filledTuples,
					SrcStartIdx: curIdx,
					SrcEndIdx:   endIdx,
				})
			}
			filledTuples += endIdx - curIdx
			o.curIdxPerBatch[i] = endIdx
		}
		sortBatchSize += filledTuples
	}

	if sortBatchSize == 0 {
		return coldata.ZeroBatch
	}

	o.sortBatch.SetLength(sortBatchSize)

	// Set the batch to sort.
	o.spool.batch = o.sortBatch

	// Sort the big batch.
	batch := o.sorter.Next(ctx)

	// Project away the extra column.
	o.outputBatch.Batch = batch

	// Count up the tuples from each stream that were emitted, and request that
	// number from each stream for the next iteration.
	col := batch.ColVec(o.streamIdCol).Int16()
	col = col[:batch.Length()]
	for i := range o.tuplesNeeded {
		o.tuplesNeeded[i] = 0
	}
	for i := 0; i < batch.Length(); i++ {
		v := col.Get(i) //gcassert:inline
		o.tuplesNeeded[v]++
	}
	// Move the tuples we're not going to emit back into the sorter.
	o.tmpBatch.SetLength(0)
	batch = o.sorter.Next(ctx)
	for batch.Length() > 0 {
		for i, v := range batch.ColVecs() {
			o.tmpBatch.ColVec(i).Append(coldata.SliceArgs{
				Src:     v,
				DestIdx: o.tmpBatch.Length(),
			})
		}
		o.tmpBatch.SetLength(o.tmpBatch.Length() + batch.Length())
		batch = o.sorter.Next(ctx)
	}
	// Swap our sort and temp batches.
	o.sortBatch, o.tmpBatch = o.tmpBatch, o.sortBatch

	// Finally, reset the sorter.
	o.sorter.reset(ctx)
	return o.outputBatch
}

func (o ordSync) ChildCount(verbose bool) int                  { return len(o.inputs) }
func (o ordSync) Child(nth int, verbose bool) execinfra.OpNode { return o.inputs[nth].Op }
func (o ordSync) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	var bufferedMeta []execinfrapb.ProducerMetadata
	for _, input := range o.inputs {
		bufferedMeta = append(bufferedMeta, input.MetadataSources.DrainMeta(ctx)...)
	}
	return bufferedMeta
}

type simpleSpooler struct {
	batch coldata.Batch
}

func (s *simpleSpooler) ChildCount(verbose bool) int {
	return 0
}

func (s *simpleSpooler) Child(nth int, verbose bool) execinfra.OpNode {
	panic("")
}

func (s *simpleSpooler) init() {}

func (s *simpleSpooler) spool(_ context.Context) {}

func (s *simpleSpooler) getValues(i int) coldata.Vec {
	return s.batch.ColVec(i)
}

func (s *simpleSpooler) getNumTuples() int {
	return s.batch.Length()
}

func (s *simpleSpooler) getPartitionsCol() []bool {
	return nil
}

func (s *simpleSpooler) getWindowedBatch(startIdx, endIdx int) coldata.Batch {
	panic("implement me")
}
