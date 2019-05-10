// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License")

// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpch

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
)

var regionNames = [...]string{`AFRICA`, `AMERICA`, `ASIA`, `EUROPE`, `MIDDLE EAST`}
var nations = [...]struct {
	name      string
	regionKey int
}{
	{name: `ALGERIA`, regionKey: 0},
	{name: `ARGENTINA`, regionKey: 1},
	{name: `BRAZIL`, regionKey: 1},
	{name: `CANADA`, regionKey: 1},
	{name: `EGYPT`, regionKey: 4},
	{name: `ETHIOPIA`, regionKey: 0},
	{name: `FRANCE`, regionKey: 3},
	{name: `GERMANY`, regionKey: 3},
	{name: `INDIA`, regionKey: 2},
	{name: `INDONESIA`, regionKey: 2},
	{name: `IRAN`, regionKey: 4},
	{name: `IRAQ`, regionKey: 4},
	{name: `JAPAN`, regionKey: 2},
	{name: `JORDAN`, regionKey: 4},
	{name: `KENYA`, regionKey: 0},
	{name: `MOROCCO`, regionKey: 0},
	{name: `MOZAMBIQUE`, regionKey: 0},
	{name: `PERU`, regionKey: 1},
	{name: `CHINA`, regionKey: 2},
	{name: `ROMANIA`, regionKey: 3},
	{name: `SAUDI ARABIA`, regionKey: 4},
	{name: `VIETNAM`, regionKey: 2},
	{name: `RUSSIA`, regionKey: 3},
	{name: `UNITED KINGDOM`, regionKey: 3},
	{name: `UNITED STATES`, regionKey: 1},
}

var nationColTypes = []types.T{
	types.Int16,
	types.Bytes,
	types.Bytes,
}

func (w *tpch) tpchNationInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng

	regionKey := batchIdx
	cb.Reset(nationColTypes, 1)
	cb.ColVec(0).Int16()[0] = int16(regionKey)                         // r_regionkey
	cb.ColVec(1).Bytes()[0] = []byte(regionNames[regionKey])           // r_name
	cb.ColVec(2).Bytes()[0] = randTextString(rng, w.textPool, 31, 115) // r_comment
}

var regionColTypes = []types.T{
	types.Int16,
	types.Bytes,
	types.Int16,
	types.Bytes,
}

func (w *tpch) tpchRegionInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng

	nationKey := batchIdx
	nation := nations[nationKey]
	cb.Reset(regionColTypes, 1)
	cb.ColVec(0).Int16()[0] = int16(nationKey)                         // n_nationkey
	cb.ColVec(1).Bytes()[0] = []byte(nation.name)                      // n_name
	cb.ColVec(2).Int16()[0] = int16(nation.regionKey)                  // n_regionkey
	cb.ColVec(3).Bytes()[0] = randTextString(rng, w.textPool, 31, 115) // r_comment
}

var supplierColTypes = []types.T{
	types.Int64,
	types.Bytes,
	types.Bytes,
	types.Int16,
	types.Bytes,
	types.Float32,
	types.Bytes,
}

func (w *tpch) tpchSupplierInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng

	suppKey := int64(batchIdx)
	nationKey := int16(randInt(rng, 0, 24))
	cb.Reset(supplierColTypes, 1)
	cb.ColVec(0).Int64()[0] = suppKey                                       // s_suppkey
	cb.ColVec(1).Bytes()[0] = []byte(fmt.Sprintf(`Supplier#%09d`, suppKey)) // s_name
	cb.ColVec(2).Bytes()[0] = randVString(rng, a, 10, 40)                   // s_address
	cb.ColVec(3).Int16()[0] = nationKey                                     // s_nationkey
	cb.ColVec(4).Bytes()[0] = randPhone(rng, nationKey)                     // s_phone
	cb.ColVec(5).Float32()[0] = randFloat(rng, -99999, 999999, 100)         // s_acctbal
	cb.ColVec(6).Bytes()[0] = randTextString(rng, w.textPool, 25, 100)      // s_comment
}
