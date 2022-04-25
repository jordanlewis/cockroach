// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package encoding2

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var sink string

func BenchmarkDecodeNonsortingUvarint(b *testing.B) {
	buf := make([]byte, 0, b.N*MaxNonsortingUvarintLen)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < b.N; i++ {
		buf = EncodeNonsortingUvarint(buf, uint64(rng.Int63()))
	}
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _, _, err = DecodeNonsortingUvarint(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	sink = fmt.Sprint(len(buf))
}

func BenchmarkDecodeNonsortingUvarint2(b *testing.B) {
	buf := make([]byte, 0, b.N*MaxNonsortingUvarintLen)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < b.N; i++ {
		buf = EncodeNonsortingUvarint(buf, uint64(rng.Int63()))
	}
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _, _, err = DecodeNonsortingUvarint2(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	sink = fmt.Sprint(len(buf))
}

func BenchmarkDecodeUint64(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint64Ascending(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUint64Ascending(vals[i%len(vals)])
	}
}

func BenchmarkDecodeUint64_2(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint64Ascending(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUint64Ascending2(vals[i%len(vals)])
	}
}
