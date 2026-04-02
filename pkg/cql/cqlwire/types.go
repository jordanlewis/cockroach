// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cqlwire

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
)

// ReadShort reads a CQL [short] (2 bytes, unsigned, big-endian) from r.
func ReadShort(r io.Reader) (uint16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, errors.Wrap(err, "reading [short]")
	}
	return binary.BigEndian.Uint16(buf[:]), nil
}

// ReadInt reads a CQL [int] (4 bytes, signed, big-endian) from r.
func ReadInt(r io.Reader) (int32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, errors.Wrap(err, "reading [int]")
	}
	return int32(binary.BigEndian.Uint32(buf[:])), nil
}

// ReadLong reads a CQL [long] (8 bytes, signed, big-endian) from r.
func ReadLong(r io.Reader) (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, errors.Wrap(err, "reading [long]")
	}
	return int64(binary.BigEndian.Uint64(buf[:])), nil
}

// ReadString reads a CQL [string] ([short] n, followed by n bytes of UTF-8)
// from r.
func ReadString(r io.Reader) (string, error) {
	n, err := ReadShort(r)
	if err != nil {
		return "", errors.Wrap(err, "reading [string] length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", errors.Wrap(err, "reading [string] data")
	}
	return string(buf), nil
}

// ReadLongString reads a CQL [long string] ([int] n, followed by n bytes of
// UTF-8) from r.
func ReadLongString(r io.Reader) (string, error) {
	n, err := ReadInt(r)
	if err != nil {
		return "", errors.Wrap(err, "reading [long string] length")
	}
	if n < 0 {
		return "", errors.New("negative [long string] length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", errors.Wrap(err, "reading [long string] data")
	}
	return string(buf), nil
}

// ReadBytes reads a CQL [bytes] ([int] n, followed by n bytes) from r. A
// negative n indicates a null value, returned as (nil, nil).
func ReadBytes(r io.Reader) ([]byte, error) {
	n, err := ReadInt(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading [bytes] length")
	}
	if n < 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, errors.Wrap(err, "reading [bytes] data")
	}
	return buf, nil
}

// ReadShortBytes reads a CQL [short bytes] ([short] n, followed by n bytes)
// from r.
func ReadShortBytes(r io.Reader) ([]byte, error) {
	n, err := ReadShort(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading [short bytes] length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, errors.Wrap(err, "reading [short bytes] data")
	}
	return buf, nil
}

// ReadStringList reads a CQL [string list] ([short] n, followed by n
// [string]s) from r.
func ReadStringList(r io.Reader) ([]string, error) {
	n, err := ReadShort(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading [string list] length")
	}
	list := make([]string, n)
	for i := range list {
		s, err := ReadString(r)
		if err != nil {
			return nil, errors.Wrapf(err, "reading [string list] element %d", i)
		}
		list[i] = s
	}
	return list, nil
}

// ReadStringMap reads a CQL [string map] ([short] n, followed by n pairs of
// [string] key and [string] value) from r.
func ReadStringMap(r io.Reader) (map[string]string, error) {
	n, err := ReadShort(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading [string map] length")
	}
	m := make(map[string]string, n)
	for i := 0; i < int(n); i++ {
		k, err := ReadString(r)
		if err != nil {
			return nil, errors.Wrapf(err, "reading [string map] key %d", i)
		}
		v, err := ReadString(r)
		if err != nil {
			return nil, errors.Wrapf(err, "reading [string map] value %d", i)
		}
		m[k] = v
	}
	return m, nil
}

// ReadStringMultiMap reads a CQL [string multimap] ([short] n, followed by n
// pairs of [string] key and [string list] value) from r.
func ReadStringMultiMap(r io.Reader) (map[string][]string, error) {
	n, err := ReadShort(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading [string multimap] length")
	}
	m := make(map[string][]string, n)
	for i := 0; i < int(n); i++ {
		k, err := ReadString(r)
		if err != nil {
			return nil, errors.Wrapf(err, "reading [string multimap] key %d", i)
		}
		v, err := ReadStringList(r)
		if err != nil {
			return nil, errors.Wrapf(err, "reading [string multimap] value %d", i)
		}
		m[k] = v
	}
	return m, nil
}

// ReadConsistency reads a CQL [consistency] level (encoded as [short]) from r.
func ReadConsistency(r io.Reader) (Consistency, error) {
	v, err := ReadShort(r)
	if err != nil {
		return 0, errors.Wrap(err, "reading [consistency]")
	}
	return Consistency(v), nil
}

// WriteShort writes a CQL [short] to w.
func WriteShort(w io.Writer, v uint16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

// WriteInt writes a CQL [int] to w.
func WriteInt(w io.Writer, v int32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	_, err := w.Write(buf[:])
	return err
}

// WriteLong writes a CQL [long] to w.
func WriteLong(w io.Writer, v int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	_, err := w.Write(buf[:])
	return err
}

// WriteString writes a CQL [string] to w.
func WriteString(w io.Writer, s string) error {
	if len(s) > maxShort {
		return errors.Newf("[string] too long: %d bytes", len(s))
	}
	if err := WriteShort(w, uint16(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

// WriteLongString writes a CQL [long string] to w.
func WriteLongString(w io.Writer, s string) error {
	if err := WriteInt(w, int32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

// WriteBytes writes a CQL [bytes] to w. A nil value encodes a null
// (length = -1).
func WriteBytes(w io.Writer, b []byte) error {
	if b == nil {
		return WriteInt(w, -1)
	}
	if err := WriteInt(w, int32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// WriteShortBytes writes a CQL [short bytes] to w.
func WriteShortBytes(w io.Writer, b []byte) error {
	if len(b) > maxShort {
		return errors.Newf("[short bytes] too long: %d bytes", len(b))
	}
	if err := WriteShort(w, uint16(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// WriteStringList writes a CQL [string list] to w.
func WriteStringList(w io.Writer, list []string) error {
	if len(list) > maxShort {
		return errors.Newf("[string list] too long: %d elements", len(list))
	}
	if err := WriteShort(w, uint16(len(list))); err != nil {
		return err
	}
	for _, s := range list {
		if err := WriteString(w, s); err != nil {
			return err
		}
	}
	return nil
}

// WriteStringMap writes a CQL [string map] to w. Iteration order is
// non-deterministic.
func WriteStringMap(w io.Writer, m map[string]string) error {
	if len(m) > maxShort {
		return errors.Newf("[string map] too long: %d entries", len(m))
	}
	if err := WriteShort(w, uint16(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := WriteString(w, k); err != nil {
			return err
		}
		if err := WriteString(w, v); err != nil {
			return err
		}
	}
	return nil
}

// WriteStringMultiMap writes a CQL [string multimap] to w. Iteration order is
// non-deterministic.
func WriteStringMultiMap(w io.Writer, m map[string][]string) error {
	if len(m) > maxShort {
		return errors.Newf("[string multimap] too long: %d entries", len(m))
	}
	if err := WriteShort(w, uint16(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := WriteString(w, k); err != nil {
			return err
		}
		if err := WriteStringList(w, v); err != nil {
			return err
		}
	}
	return nil
}

// WriteConsistency writes a CQL [consistency] level to w.
func WriteConsistency(w io.Writer, c Consistency) error {
	return WriteShort(w, uint16(c))
}

const maxShort = 1<<16 - 1
