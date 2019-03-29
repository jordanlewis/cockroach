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

package coldata

import (
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// column is an interface that represents a raw array of a Go native type.
type column interface{}

// Vec is an interface that represents a column vector that's accessible by
// Go native types.
type Vec interface {
	Nulls

	// TODO(jordan): is a bitmap or slice of bools better?
	// Bool returns a bool list.
	Bool() []bool
	// Int8 returns an int8 slice.
	Int8() []int8
	// Int16 returns an int16 slice.
	Int16() []int16
	// Int32 returns an int32 slice.
	Int32() []int32
	// Int64 returns an int64 slice.
	Int64() []int64
	// Float32 returns a float32 slice.
	Float32() []float32
	// Float64 returns a float64 slice.
	Float64() []float64
	// Bytes returns a []byte slice.
	Bytes() [][]byte
	// TODO(jordan): should this be [][]byte?
	// Decimal returns an apd.Decimal slice.
	Decimal() []apd.Decimal

	// Col returns the raw, typeless backing storage for this Vec.
	Col() interface{}

	// SetCol sets the member column (in the case of mutable columns).
	SetCol(interface{})

	// TemplateType returns an []interface{} and is used for operator templates.
	// Do not call this from normal code - it'll always panic.
	_TemplateType() []interface{}

	// Append appends fromLength elements of the given Vec to toLength
	// elements of this Vec, assuming that both Vecs are of type colType.
	Append(vec Vec, colType types.T, toLength int, fromLength int)

	// AppendSlice appends vec[srcStartIdx:srcEndIdx] elements to
	// this Vec starting at destStartIdx.
	AppendSlice(vec Vec, colType types.T, destStartIdx int, srcStartIdx int, srcEndIdx int)

	// AppendWithSel appends into itself another column vector from a Batch with
	// maximum size of BatchSize, filtered by the given selection vector.
	AppendWithSel(vec Vec, sel []int, batchSize int, colType types.T, toLength int)

	// AppendSliceWithSel appends srcEndIdx - srcStartIdx elements to this Vec starting
	// at destStartIdx. These elements come from vec, filtered by the selection
	// vector sel.
	AppendSliceWithSel(vec Vec, colType types.T, destStartIdx int, srcStartIdx int, srcEndIdx int, sel []int)

	// Copy copies src[srcStartIdx:srcEndIdx] into this Vec.
	Copy(src Vec, srcStartIdx, srcEndIdx int, typ types.T)

	// CopyWithSelInt64 copies vec, filtered by sel, into this Vec. It replaces
	// the contents of this Vec.
	CopyWithSelInt64(vec Vec, sel []int, nSel int, colType types.T)

	// CopyWithSelInt16 copies vec, filtered by sel, into this Vec. It replaces
	// the contents of this Vec.
	CopyWithSelInt16(vec Vec, sel []int, nSel int, colType types.T)

	// CopyWithSelAndNilsInt64 copies vec, filtered by sel, unless nils is set,
	// into Vec. It replaces the contents of this Vec.
	CopyWithSelAndNilsInt64(vec Vec, sel []int, nSel int, nils []bool, colType types.T)

	// Slice returns a new Vec representing a slice of the current Vec from
	// [start, end).
	Slice(colType types.T, start int, end int) Vec

	// PrettyValueAt returns a "pretty"value for the idx'th value in this Vec.
	// It uses the reflect package and is not suitable for calling in hot paths.
	PrettyValueAt(idx int, colType types.T) string

	// ExtendNulls extends the null member of a Vec to accommodate toAppend tuples
	// and sets the right indexes to null, needed when the length of the underlying column changes.
	ExtendNulls(vec Vec, destStartIdx int, srcStartIdx int, toAppend int)

	// ExtendNullsWithSel extends the null member of a Vec to accommodate toAppend tuples
	// and sets the right indexes to null with the selection vector in mind, needed when the
	// length of the underlying column changes.
	ExtendNullsWithSel(vec Vec, destStartIdx int, srcStartIdx int, toAppend int, sel []int)
}

// Nulls represents a list of potentially nullable values.
type Nulls interface {
	// HasNulls returns true if the column has any null values.
	HasNulls() bool

	// NullAt takes in a int and returns true if the ith value of the column is
	// null.
	NullAt(i int) bool
	// SetNull takes in a int and sets the ith value of the column to null.
	SetNull(i int)

	// UnsetNulls sets the column to have 0 null values.
	UnsetNulls()
	// SetNulls sets the column to have only null values.
	SetNulls()
}

var _ Vec = &memColumn{}

// zeroedNulls is a zeroed out slice representing a bitmap of size BatchSize.
// This is copied to efficiently clear a nulls slice.
var zeroedNulls [(BatchSize-1)>>6 + 1]int64

// filledNulls is a slice representing a bitmap of size BatchSize with every
// single bit set.
var filledNulls [(BatchSize-1)>>6 + 1]int64

func init() {
	// Initializes filledNulls to the desired slice.
	for i := range filledNulls {
		filledNulls[i] = ^0
	}
}

// memColumn is a simple pass-through implementation of Vec that just casts
// a generic interface{} to the proper type when requested.
type memColumn struct {
	col column

	nulls []int64
	// hasNulls represents whether or not the memColumn has any null values set.
	hasNulls bool
}

// NewMemColumn returns a new memColumn, initialized with a length.
func NewMemColumn(t types.T, n int) Vec {
	var nulls []int64
	if n > 0 {
		nulls = make([]int64, (n-1)>>6+1)
	} else {
		nulls = make([]int64, 0)
	}

	switch t {
	case types.Bool:
		return &memColumn{col: make([]bool, n), nulls: nulls}
	case types.Bytes:
		return &memColumn{col: make([][]byte, n), nulls: nulls}
	case types.Int8:
		return &memColumn{col: make([]int8, n), nulls: nulls}
	case types.Int16:
		return &memColumn{col: make([]int16, n), nulls: nulls}
	case types.Int32:
		return &memColumn{col: make([]int32, n), nulls: nulls}
	case types.Int64:
		return &memColumn{col: make([]int64, n), nulls: nulls}
	case types.Float32:
		return &memColumn{col: make([]float32, n), nulls: nulls}
	case types.Float64:
		return &memColumn{col: make([]float64, n), nulls: nulls}
	case types.Decimal:
		return &memColumn{col: make([]apd.Decimal, n), nulls: nulls}
	default:
		panic(fmt.Sprintf("unhandled type %s", t))
	}
}

func (m *memColumn) HasNulls() bool {
	return m.hasNulls
}

func (m *memColumn) NullAt(i int) bool {
	intIdx := i >> 6
	return ((m.nulls[intIdx] >> (uint(i) % 64)) & 1) == 1
}

func (m *memColumn) SetNull(i int) {
	m.hasNulls = true
	intIdx := i >> 6
	m.nulls[intIdx] |= 1 << (uint(i) % 64)
}

func (m *memColumn) SetCol(col interface{}) {
	m.col = col
}

func (m *memColumn) UnsetNulls() {
	m.hasNulls = false

	startIdx := 0
	for startIdx < len(m.nulls) {
		startIdx += copy(m.nulls[startIdx:], zeroedNulls[:])
	}
}

func (m *memColumn) SetNulls() {
	m.hasNulls = true

	startIdx := 0
	for startIdx < len(m.nulls) {
		startIdx += copy(m.nulls[startIdx:], filledNulls[:])
	}
}

func (m *memColumn) Bool() []bool {
	return m.col.([]bool)
}

func (m *memColumn) Int8() []int8 {
	return m.col.([]int8)
}

func (m *memColumn) Int16() []int16 {
	return m.col.([]int16)
}

func (m *memColumn) Int32() []int32 {
	return m.col.([]int32)
}

func (m *memColumn) Int64() []int64 {
	return m.col.([]int64)
}

func (m *memColumn) Float32() []float32 {
	return m.col.([]float32)
}

func (m *memColumn) Float64() []float64 {
	return m.col.([]float64)
}

func (m *memColumn) Bytes() [][]byte {
	return m.col.([][]byte)
}

func (m *memColumn) Decimal() []apd.Decimal {
	return m.col.([]apd.Decimal)
}

func (m *memColumn) Col() interface{} {
	return m.col
}

func (m *memColumn) _TemplateType() []interface{} {
	panic("don't call this from non template code")
}
