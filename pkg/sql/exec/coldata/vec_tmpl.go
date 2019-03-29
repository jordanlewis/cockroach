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

// {{/*
// +build execgen_template
//
// This file is the execgen template for colvec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package coldata

import (
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// {{/*

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// */}}

func (m *memColumn) Append(vec Vec, colType types.T, toLength int, fromLength int) {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		m.col = append(m._TemplateType()[:toLength], vec._TemplateType()[:fromLength]...)
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}

	if fromLength > 0 {
		m.nulls = append(m.nulls, make([]int64, (fromLength-1)>>6+1)...)

		if vec.HasNulls() {
			for i := 0; i < fromLength; i++ {
				if vec.NullAt(i) {
					m.SetNull(toLength + i)
				}
			}
		}
	}
}

func (m *memColumn) AppendSlice(
	vec Vec, colType types.T, destStartIdx int, srcStartIdx int, srcEndIdx int,
) {
	batchSize := srcEndIdx - srcStartIdx
	outputLen := destStartIdx + batchSize

	switch colType {
	// {{range .}}
	case _TYPES_T:
		if outputLen > len(m._TemplateType()) {
			m.col = append(m._TemplateType()[:destStartIdx], vec._TemplateType()[srcStartIdx:srcEndIdx]...)
		} else {
			copy(m._TemplateType()[destStartIdx:], vec._TemplateType()[srcStartIdx:srcEndIdx])
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}

	m.ExtendNulls(vec, destStartIdx, srcStartIdx, batchSize)
}

func (m *memColumn) AppendWithSel(
	vec Vec, sel []int, batchSize int, colType types.T, toLength int,
) {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := append(m._TemplateType()[:toLength], make([]_GOTYPE, batchSize)...)
		fromCol := vec._TemplateType()

		for i := 0; i < batchSize; i++ {
			toCol[i+toLength] = fromCol[sel[i]]
		}

		m.col = toCol
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}

	if batchSize > 0 {
		m.nulls = append(m.nulls, make([]int64, (batchSize-1)>>6+1)...)
		for i := 0; i < batchSize; i++ {
			if vec.NullAt(sel[i]) {
				m.SetNull(toLength + i)
			}
		}
	}
}

func (m *memColumn) AppendSliceWithSel(
	vec Vec, colType types.T, destStartIdx int, srcStartIdx int, srcEndIdx int, sel []int,
) {
	batchSize := srcEndIdx - srcStartIdx
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := append(m._TemplateType()[:destStartIdx], make([]_GOTYPE, batchSize)...)
		fromCol := vec._TemplateType()

		for i := 0; i < int(batchSize); i++ {
			toCol[i+destStartIdx] = fromCol[sel[i+int(srcStartIdx)]]
		}

		m.col = toCol
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}

	m.ExtendNullsWithSel(vec, destStartIdx, srcStartIdx, batchSize, sel)
}

func (m *memColumn) Copy(src Vec, srcStartIdx, srcEndIdx int, typ types.T) {
	m.CopyAt(src, 0, srcStartIdx, srcEndIdx, typ)
}

func (m *memColumn) CopyAt(src Vec, destStartIdx, srcStartIdx, srcEndIdx int, typ types.T) {
	switch typ {
	// {{range .}}
	case _TYPES_T:
		copy(m._TemplateType()[destStartIdx:], src._TemplateType()[srcStartIdx:srcEndIdx])
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", typ))
	}
}

func (m *memColumn) CopyWithSelInt64(vec Vec, sel []int, nSel int, colType types.T) {
	m.UnsetNulls()

	// todo (changangela): handle the case when nSel > BatchSize
	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			for i := 0; i < nSel; i++ {
				if vec.NullAt(sel[i]) {
					m.SetNull(i)
				} else {
					toCol[i] = fromCol[sel[i]]
				}
			}
		} else {
			for i := 0; i < nSel; i++ {
				toCol[i] = fromCol[sel[i]]
			}
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) CopyWithSelInt16(vec Vec, sel []int, nSel int, colType types.T) {
	m.UnsetNulls()

	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			for i := 0; i < nSel; i++ {
				if vec.NullAt(sel[i]) {
					m.SetNull(i)
				} else {
					toCol[i] = fromCol[sel[i]]
				}
			}
		} else {
			for i := 0; i < nSel; i++ {
				toCol[i] = fromCol[sel[i]]
			}
		}
		// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) CopyWithSelAndNilsInt64(
	vec Vec, sel []int, nSel int, nils []bool, colType types.T,
) {
	m.UnsetNulls()

	switch colType {
	// {{range .}}
	case _TYPES_T:
		toCol := m._TemplateType()
		fromCol := vec._TemplateType()

		if vec.HasNulls() {
			// TODO(jordan): copy the null arrays in batch.
			for i := 0; i < nSel; i++ {
				if nils[i] {
					m.SetNull(i)
				} else {
					if vec.NullAt(sel[i]) {
						m.SetNull(i)
					} else {
						toCol[i] = fromCol[sel[i]]
					}
				}
			}
		} else {
			for i := 0; i < nSel; i++ {
				if nils[i] {
					m.SetNull(i)
				} else {
					toCol[i] = fromCol[sel[i]]
				}
			}
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) Slice(colType types.T, start int, end int) Vec {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		col := m._TemplateType()
		return &memColumn{
			col: col[start:end],
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) PrettyValueAt(colIdx int, colType types.T) string {
	if m.NullAt(colIdx) {
		return "NULL"
	}
	switch colType {
	// {{range .}}
	case _TYPES_T:
		return fmt.Sprintf("%v", m._TemplateType()[colIdx])
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

func (m *memColumn) ExtendNulls(vec Vec, destStartIdx int, srcStartIdx int, toAppend int) {
	outputLen := destStartIdx + toAppend
	if cap(m.nulls) < outputLen/64 {
		// (batchSize-1)>>6+1 is the number of Int64s needed to encode the additional elements/nulls in the Vec.
		// This is equivalent to ceil(batchSize/64).
		m.nulls = append(m.nulls, make([]int64, (toAppend-1)>>6+1)...)
	}
	if vec.HasNulls() {
		for i := 0; i < toAppend; i++ {
			// TODO(yuzefovich): this can be done more efficiently with a bitwise OR:
			// like m.nulls[i] |= vec.nulls[i].
			if vec.NullAt(srcStartIdx + i) {
				m.SetNull(destStartIdx + i)
			}
		}
	}
}

func (m *memColumn) ExtendNullsWithSel(
	vec Vec, destStartIdx int, srcStartIdx int, toAppend int, sel []int,
) {
	outputLen := destStartIdx + toAppend
	if cap(m.nulls) < outputLen/64 {
		// (batchSize-1)>>6+1 is the number of Int64s needed to encode the additional elements/nulls in the Vec.
		// This is equivalent to ceil(batchSize/64).
		m.nulls = append(m.nulls, make([]int64, (toAppend-1)>>6+1)...)
	}
	for i := 0; i < toAppend; i++ {
		// TODO(yuzefovich): this can be done more efficiently with a bitwise OR:
		// like m.nulls[i] |= vec.nulls[i].
		if vec.NullAt(sel[srcStartIdx+i]) {
			m.SetNull(destStartIdx + i)
		}
	}
}
