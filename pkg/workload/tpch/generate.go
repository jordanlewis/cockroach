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

	"golang.org/x/exp/rand"
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

func (w *tpch) tpchNationInitialRow(batchIdx int) []interface{} {
	rng := rand.New(rand.NewSource(w.seed + uint64(batchIdx)))

	regionKey := batchIdx
	return []interface{}{
		regionKey,                    // r_regionkey
		regionNames[regionKey],       // r_name
		randTextString(rng, 31, 115), // r_comment
	}
}

func (w *tpch) tpchRegionInitialRow(batchIdx int) []interface{} {
	rng := rand.New(rand.NewSource(w.seed + uint64(batchIdx)))

	nationKey := batchIdx
	nation := nations[nationKey]
	return []interface{}{
		nationKey,                    // n_nationkey
		nation.name,                  // n_name
		nation.regionKey,             // n_regionkey
		randTextString(rng, 31, 114), // n_comment
	}
}

func (w *tpch) tpchSupplierInitialRow(batchIdx int) []interface{} {
	rng := rand.New(rand.NewSource(w.seed + uint64(batchIdx)))

	suppKey := batchIdx
	nationKey := randInt(rng, 0, 24)
	return []interface{}{
		suppKey,                               // s_suppkey
		fmt.Sprintf(`Supplier#%09d`, suppKey), // s_name
		randVString(rng, 10, 40),              // s_address
		nationKey,                             // s_nationkey
		randPhone(rng, nationKey),             // s_phone
		randFloat(rng, -99999, 999999, 100),
		randTextString(rng, 25, 100), // s_comment
	}
}
