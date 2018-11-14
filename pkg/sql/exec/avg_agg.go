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

package exec

type avgFloatAgg struct {
	done bool

	groups  []bool
	scratch struct {
		curIdx int
		// groupSums[i] keeps track of the sum of elements belonging to the ith
		// group.
		groupSums []float64
		// groupCounts[i] keeps track of the number of elements that we've seen
		// belonging to the ith group.
		groupCounts []int64
		// vec points to the output vector.
		vec []float64
	}
}

var _ aggregateFunc = &avgFloatAgg{}

func (a *avgFloatAgg) Init(groups []bool, v ColVec) {
	a.groups = groups
	a.scratch.vec = v.Float64()
	a.scratch.groupSums = make([]float64, len(a.scratch.vec))
	a.scratch.groupCounts = make([]int64, len(a.scratch.vec))
	a.Reset()
}

func (a *avgFloatAgg) Reset() {
	copy(a.scratch.groupSums, zeroFloat64Batch)
	copy(a.scratch.groupCounts, zeroInt64Batch)
	copy(a.scratch.vec, zeroFloat64Batch)
	a.scratch.curIdx = -1
}

func (a *avgFloatAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *avgFloatAgg) SetOutputIndex(idx int) {
	if a.scratch.curIdx != -1 {
		a.scratch.curIdx = idx
		copy(a.scratch.groupSums[idx+1:], zeroFloat64Batch)
		copy(a.scratch.groupCounts[idx+1:], zeroInt64Batch)
		// TODO(asubiotto): We might not have to zero a.scratch.vec since we
		// overwrite with an independent value.
		copy(a.scratch.vec[idx+1:], zeroFloat64Batch)
	}
}

func (a *avgFloatAgg) Compute(b ColBatch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value.
		if a.scratch.curIdx >= 0 {
			// TODO(asubiotto): Wonder how best to template this part (and below).
			// We'd like to do something similar to AssignFunc, where we have a
			// separate method call on DDecimal per type.
			a.scratch.vec[a.scratch.curIdx] = a.scratch.groupSums[a.scratch.curIdx] / a.scratch.vec[a.scratch.curIdx]
		}
		a.scratch.curIdx++
		a.done = true
		return
	}
	col, sel := b.ColVec(int(inputIdxs[0])).Float64(), b.Selection()
	if sel != nil {
		sel = sel[:inputLen]
		for _, i := range sel {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.scratch.curIdx += x
			a.scratch.vec[i] =
			a.scratch.groupSums[a.scratch.curIdx] = a.scratch.groupSums[a.scratch.curIdx] + col[i]
			a.scratch.groupCounts[a.scratch.curIdx]++
		}
	} else {
		col = col[:inputLen]
		for i := range col {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.scratch.curIdx += x
			a.scratch.groupSums[a.scratch.curIdx] = a.scratch.groupSums[a.scratch.curIdx] + col[i]
			a.scratch.groupCounts[a.scratch.curIdx]++
		}
	}

	for i := 0; i < a.scratch.curIdx; i++ {
		a.scratch.vec[i] = a.scratch.groupSums[i] / a.scratch.vec[i]
	}
}
