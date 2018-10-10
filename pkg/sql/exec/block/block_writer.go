package block

import (
	"encoding/binary"
	"math"
	"unsafe"
)

type columnWriter struct {
	ctype     ColumnType
	data      []byte
	offsets   []int32
	nulls     nullBitmapBuilder
	count     int32
	nullCount int32
}

func (w *columnWriter) reset() {
	w.data = w.data[:0]
	w.offsets = w.offsets[:0]
	w.nulls = w.nulls[:0]
	w.count = 0
	w.nullCount = 0
}

func (w *columnWriter) grow(n int) []byte {
	i := len(w.data)
	if cap(w.data)-i < n {
		newSize := 2 * cap(w.data)
		if newSize == 0 {
			newSize = 256
		}
		newData := make([]byte, i, newSize)
		copy(newData, w.data)
		w.data = newData
	}
	w.data = w.data[:i+n]
	return w.data[i:]
}

func (w *columnWriter) putBool(v bool) {
	if w.ctype != ColumnTypeBool {
		panic("bool column value expected")
	}
	w.data = (Bitmap)(w.data).set(int(w.count), v)
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putInt8(v int8) {
	if w.ctype != ColumnTypeInt8 {
		panic("int8 column value expected")
	}
	w.data = append(w.data, byte(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putInt16(v int16) {
	if w.ctype != ColumnTypeInt16 {
		panic("int16 column value expected")
	}
	binary.LittleEndian.PutUint16(w.grow(2), uint16(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putInt32(v int32) {
	if w.ctype != ColumnTypeInt32 {
		panic("int32 column value expected")
	}
	binary.LittleEndian.PutUint32(w.grow(4), uint32(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putInt64(v int64) {
	if w.ctype != ColumnTypeInt64 {
		panic("int64 column value expected")
	}
	binary.LittleEndian.PutUint64(w.grow(8), uint64(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putFloat32(v float32) {
	if w.ctype != ColumnTypeFloat32 {
		panic("float32 column value expected")
	}
	binary.LittleEndian.PutUint32(w.grow(4), math.Float32bits(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putFloat64(v float64) {
	if w.ctype != ColumnTypeFloat64 {
		panic("float64 column value expected")
	}
	binary.LittleEndian.PutUint64(w.grow(8), math.Float64bits(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putBytes(v []byte) {
	if w.ctype != ColumnTypeBytes {
		panic("bytes column value expected")
	}
	w.data = append(w.data, v...)
	w.offsets = append(w.offsets, int32(len(w.data)))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putNull() {
	w.nulls = w.nulls.set(int(w.count), true)
	if w.ctype.Width() <= 0 {
		w.offsets = append(w.offsets, int32(len(w.data)))
	}
	w.count++
	w.nullCount++
}

func align(offset, val int32) int32 {
	return (offset + val - 1) & ^(val - 1)
}

func (w *columnWriter) encode(offset int32, buf []byte) int32 {
	// The column type.
	buf[offset] = byte(w.ctype)
	offset++
	// The NULL-bitmap.
	if w.nullCount == 0 {
		buf[offset] = 0 // no NULL-bitmap
		offset++
	} else {
		buf[offset] = 1 // NULL-bitmap exists
		offset++
		offset = align(offset, 4)
		w.nulls.verify()
		for i := 0; i < len(w.nulls); i++ {
			binary.LittleEndian.PutUint32(buf[offset:], w.nulls[i])
			offset += 4
		}
	}
	// The column values.
	offset = align(offset, w.ctype.Alignment())
	offset += int32(copy(buf[offset:], w.data))
	// The offsets for variable width data.
	if w.ctype.Width() <= 0 {
		offset = align(offset, 4)
		dest := (*[1 << 31]int32)(unsafe.Pointer(&buf[offset]))[:w.count:w.count]
		copy(dest, w.offsets)
		offset += int32(len(w.offsets) * 4)
	}
	return offset
}

func (w *columnWriter) size(offset int32) int32 {
	startOffset := offset
	// The column type.
	offset++
	// The NULL-bitmap.
	offset++
	if w.nullCount > 0 {
		offset = align(offset, 4)
		offset += 4 * int32(len(w.nulls))
	}
	// The column values.
	offset = align(offset, w.ctype.Alignment())
	offset += int32(len(w.data))
	// The offsets for variable width data.
	if w.ctype.Width() <= 0 {
		offset = align(offset, 4)
		offset += int32(len(w.offsets) * 4)
	}
	return offset - startOffset
}

func blockHeaderSize(n int) int32 {
	return int32(8 + n*4)
}

func pageOffsetPos(i int) int32 {
	return int32(8 + i*4)
}

type blockWriter struct {
	cols []columnWriter
	buf  []byte
}

func (w *blockWriter) init(s []ColumnType) {
	w.cols = make([]columnWriter, len(s))
	for i := range w.cols {
		w.cols[i].ctype = s[i]
	}
}

func (w *blockWriter) reset() {
	for i := range w.cols {
		w.cols[i].reset()
	}
}

func (w *blockWriter) Finish() []byte {
	size := w.Size()
	if int32(cap(w.buf)) < size {
		w.buf = make([]byte, size)
	}
	w.buf = w.buf[:size]
	n := len(w.cols)
	binary.LittleEndian.PutUint32(w.buf[0:], uint32(n))
	binary.LittleEndian.PutUint32(w.buf[4:], uint32(w.cols[0].count))
	pageOffset := blockHeaderSize(n)
	for i := range w.cols {
		col := &w.cols[i]
		binary.LittleEndian.PutUint32(w.buf[pageOffsetPos(i):], uint32(pageOffset))
		pageOffset = col.encode(pageOffset, w.buf)
	}
	return w.buf
}

func (w *blockWriter) Size() int32 {
	size := blockHeaderSize(len(w.cols))
	for i := range w.cols {
		size += w.cols[i].size(size)
	}
	return size
}

func (w *blockWriter) PutRow(row RowReader) {
	for i := range w.cols {
		col := &w.cols[i]
		if row.Null(i) {
			col.putNull()
			continue
		}
		switch w.cols[i].ctype {
		case ColumnTypeBool:
			col.putBool(row.Bool(i))
		case ColumnTypeInt8:
			col.putInt8(row.Int8(i))
		case ColumnTypeInt16:
			col.putInt16(row.Int16(i))
		case ColumnTypeInt32:
			col.putInt32(row.Int32(i))
		case ColumnTypeInt64:
			col.putInt64(row.Int64(i))
		case ColumnTypeFloat32:
			col.putFloat32(row.Float32(i))
		case ColumnTypeFloat64:
			col.putFloat64(row.Float64(i))
		case ColumnTypeBytes:
			col.putBytes(row.Bytes(i))
		}
	}
}

func (w *blockWriter) PutBool(col int, v bool) {
	w.cols[col].putBool(v)
}

func (w *blockWriter) PutInt8(col int, v int8) {
	w.cols[col].putInt8(v)
}

func (w *blockWriter) PutInt16(col int, v int16) {
	w.cols[col].putInt16(v)
}

func (w *blockWriter) PutInt32(col int, v int32) {
	w.cols[col].putInt32(v)
}

func (w *blockWriter) PutInt64(col int, v int64) {
	w.cols[col].putInt64(v)
}

func (w *blockWriter) PutFloat32(col int, v float32) {
	w.cols[col].putFloat32(v)
}

func (w *blockWriter) PutFloat64(col int, v float64) {
	w.cols[col].putFloat64(v)
}

func (w *blockWriter) PutBytes(col int, v []byte) {
	w.cols[col].putBytes(v)
}

func (w *blockWriter) PutNull(col int) {
	w.cols[col].putNull()
}
