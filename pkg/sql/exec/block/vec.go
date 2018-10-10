package block

import "unsafe"

// Column is an interface that represents a column vector that's accessible by
// Go native types.
type Column interface {
	Bool() Bitmap
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
	// Float64 returns an float64 slice.
	Float64() []float64
	// Bytes returns a Bytes object, allowing retrieval of multiple byte slices.
	Bytes() Bytes
}

// Vec holds data for a single column. Vec provides accessors for the native
// data such as Int32() to access []int32 data.
type Vec struct {
	N    int32      // the number of elements in the bitmap
	Type ColumnType // the type of vector elements
	NullBitmap
	start unsafe.Pointer // pointer to start of the column data
	end   unsafe.Pointer // pointer to the end of column data
}

// Bool returns the vec data as a boolean bitmap. The bitmap should not be
// mutated.
func (v Vec) Bool() Bitmap {
	if v.Type != ColumnTypeBool {
		panic("vec does not hold bool data")
	}
	n := (v.count(int(v.N)) + 7) / 8
	return Bitmap((*[1 << 31]byte)(v.start)[:n:n])
}

// Int8 returns the vec data as []int8. The slice should not be mutated.
func (v Vec) Int8() []int8 {
	if v.Type != ColumnTypeInt8 {
		panic("vec does not hold int8 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]int8)(v.start)[:n:n]
}

// Int16 returns the vec data as []int16. The slice should not be mutated.
func (v Vec) Int16() []int16 {
	if v.Type != ColumnTypeInt16 {
		panic("vec does not hold int16 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]int16)(v.start)[:n:n]
}

// Int32 returns the vec data as []int32. The slice should not be mutated.
func (v Vec) Int32() []int32 {
	if v.Type != ColumnTypeInt32 {
		panic("vec does not hold int32 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]int32)(v.start)[:n:n]
}

// Int64 returns the vec data as []int64. The slice should not be mutated.
func (v Vec) Int64() []int64 {
	if v.Type != ColumnTypeInt64 {
		panic("vec does not hold int64 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]int64)(v.start)[:n:n]
}

// Float32 returns the vec data as []float32. The slice should not be mutated.
func (v Vec) Float32() []float32 {
	if v.Type != ColumnTypeFloat32 {
		panic("vec does not hold float32 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]float32)(v.start)[:n:n]
}

// Float64 returns the vec data as []float64. The slice should not be mutated.
func (v Vec) Float64() []float64 {
	if v.Type != ColumnTypeFloat64 {
		panic("vec does not hold float64 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]float64)(v.start)[:n:n]
}

// Bytes returns the vec data as Bytes. The underlying data should not be
// mutated.
func (v Vec) Bytes() Bytes {
	if v.Type != ColumnTypeBytes {
		panic("vec does not hold bytes data")
	}
	if uintptr(v.end)%4 != 0 {
		panic("expected offsets data to be 4-byte aligned")
	}
	n := v.N
	return Bytes{
		count:   int(n),
		data:    v.start,
		offsets: unsafe.Pointer(uintptr(v.end) - uintptr(n*4)),
	}
}
