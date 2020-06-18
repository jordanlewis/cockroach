// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

// Bools is a slice of bool.
type Bools []bool

// Int16s is a slice of int16.
type Int16s []int16

// Int32s is a slice of int32.
type Int32s []int32

// Int64s is a slice of int64.
type Int64s []int64

// Float64s is a slice of float64.
type Float64s []float64

// Decimals is a slice of apd.Decimal.
type Decimals []apd.Decimal

// Times is a slice of time.Time.
type Times []time.Time

// Durations is a slice of duration.Duration.
type Durations []duration.Duration

// Get returns the element at index idx of the vector.
func (c Bools) Get(idx int) bool { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Bools) Set(idx int, v bool) { c[idx] = v }

func (c Bools) Len() int { return len(c) }

// Get returns the element at index idx of the vector.
func (c Int16s) Get(idx int) int16 { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Int16s) Set(idx int, v int16) { c[idx] = v }
func (c Int16s) Len() int             { return len(c) }

// Get returns the element at index idx of the vector.
func (c Int32s) Get(idx int) int32 { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Int32s) Set(idx int, v int32) { c[idx] = v }
func (c Int32s) Len() int             { return len(c) }

// Get returns the element at index idx of the vector.
func (c Int64s) Get(idx int) int64 { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Int64s) Set(idx int, v int64) { c[idx] = v }
func (c Int64s) Len() int             { return len(c) }

// Get returns the element at index idx of the vector.
func (c Float64s) Get(idx int) float64 { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Float64s) Set(idx int, v float64) { c[idx] = v }
func (c Float64s) Len() int               { return len(c) }

// Get returns the element at index idx of the vector.
func (c Decimals) Get(idx int) apd.Decimal { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Decimals) Set(idx int, v apd.Decimal) { c[idx].Set(&v) }
func (c Decimals) Len() int                   { return len(c) }

// Get returns the element at index idx of the vector.
func (c Times) Get(idx int) time.Time { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Times) Set(idx int, v time.Time) { c[idx] = v }
func (c Times) Len() int                 { return len(c) }

// Get returns the element at index idx of the vector.
func (c Durations) Get(idx int) duration.Duration { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Durations) Set(idx int, v duration.Duration) { c[idx] = v }
func (c Durations) Len() int                         { return len(c) }

func (c *Bools) AppendVal(v bool) { *c = append(*c, v) }
func (c *Decimals) AppendVal(v apd.Decimal) {
	*c = append(*c, apd.Decimal{})
	(*c)[len(*c)-1].Set(&v)
}
func (c *Int16s) AppendVal(v int16)                { *c = append(*c, v) }
func (c *Int32s) AppendVal(v int32)                { *c = append(*c, v) }
func (c *Int64s) AppendVal(v int64)                { *c = append(*c, v) }
func (c *Float64s) AppendVal(v float64)            { *c = append(*c, v) }
func (c *Times) AppendVal(v time.Time)             { *c = append(*c, v) }
func (c *Durations) AppendVal(v duration.Duration) { *c = append(*c, v) }

func (Bools) CopyVal(dst, src *bool)                  { *dst = *src }
func (Decimals) CopyVal(dst, src *apd.Decimal)        { dst.Set(src) }
func (Int16s) CopyVal(dst, src *int16)                { *dst = *src }
func (Int32s) CopyVal(dst, src *int32)                { *dst = *src }
func (Int64s) CopyVal(dst, src *int64)                { *dst = *src }
func (Float64s) CopyVal(dst, src *float64)            { *dst = *src }
func (Times) CopyVal(dst, src *time.Time)             { *dst = *src }
func (Durations) CopyVal(dst, src *duration.Duration) { *dst = *src }

func (c Bools) CopySlice(src Bools, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}
func (c Decimals) CopySlice(src Decimals, destIdx, srcStartIdx, srcEndIdx int) {
	tgt := c[destIdx:]
	src = src[srcStartIdx:srcEndIdx]
	_ = tgt[len(src)-1]
	for i := range src {
		tgt[i].Set(&src[i])
	}
}

func (c Int16s) CopySlice(src Int16s, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}
func (c Int32s) CopySlice(src Int32s, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}
func (c Int64s) CopySlice(src Int64s, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}
func (c Float64s) CopySlice(src Float64s, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}
func (c Times) CopySlice(src Times, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}
func (c Durations) CopySlice(src Durations, destIdx, srcStartIdx, srcEndIdx int) {
	copy(c[destIdx:], src[srcStartIdx:srcEndIdx])
}

func (c *Bools) AppendSlice(src Bools, destIdx, srcStartIdx, srcEndIdx int) {
	*c = append((*c)[:destIdx], src[srcStartIdx:srcEndIdx]...)
}
func (c *Decimals) AppendSlice(src Decimals, destIdx, srcStartIdx, srcEndIdx int) {
	desiredCap := destIdx + srcEndIdx - srcStartIdx
	targetCap := cap(*c)
	if targetCap >= desiredCap {
		*c = (*c)[:desiredCap]
	} else {
		capToAllocate := desiredCap
		if capToAllocate < 2*targetCap {
			capToAllocate = 2 * targetCap
		}
		newSlice := make(Decimals, desiredCap, capToAllocate)
		copy(newSlice, (*c)[:destIdx])
		*c = newSlice
	}
	c.CopySlice(src, destIdx, srcStartIdx, srcEndIdx)
}

func (c *Int16s) AppendSlice(src Int16s, destIdx, srcStartIdx, srcEndIdx int) {
	*c = append((*c)[:destIdx], src[srcStartIdx:srcEndIdx]...)
}
func (c *Int32s) AppendSlice(src Int32s, destIdx, srcStartIdx, srcEndIdx int) {
	*c = append((*c)[:destIdx], src[srcStartIdx:srcEndIdx]...)
}
func (c *Int64s) AppendSlice(src Int64s, destIdx, srcStartIdx, srcEndIdx int) {
	*c = append((*c)[:destIdx], src[srcStartIdx:srcEndIdx]...)
}
func (c *Float64s) AppendSlice(src Float64s, destIdx, srcStartIdx, srcEndIdx int) {
	*c = append((*c)[:destIdx], src[srcStartIdx:srcEndIdx]...)
}
func (c *Times) AppendSlice(src Times, destIdx, srcStartIdx, srcEndIdx int) {
	*c = append((*c)[:destIdx], src[srcStartIdx:srcEndIdx]...)
}
func (c *Durations) AppendSlice(src Durations, destIdx, srcStartIdx, srcEndIdx int) {
	*c = append((*c)[:destIdx], src[srcStartIdx:srcEndIdx]...)
}

func (c Bools) Slice(start, end int) Bools         { return c[start:end] }
func (c Decimals) Slice(start, end int) Decimals   { return c[start:end] }
func (c Int16s) Slice(start, end int) Int16s       { return c[start:end] }
func (c Int32s) Slice(start, end int) Int32s       { return c[start:end] }
func (c Int64s) Slice(start, end int) Int64s       { return c[start:end] }
func (c Float64s) Slice(start, end int) Float64s   { return c[start:end] }
func (c Times) Slice(start, end int) Times         { return c[start:end] }
func (c Durations) Slice(start, end int) Durations { return c[start:end] }
