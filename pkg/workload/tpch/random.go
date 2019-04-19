// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpch

import (
	"fmt"

	"golang.org/x/exp/rand"
)

const alphabet = `abcdefghijklmnopqrstuvwxyz`
const alphanumericLen64 = `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890, `

// randInt returns a random value between x and y inclusively, with a mean of
// (x+y)/2. See 4.2.2.3.
func randInt(rng *rand.Rand, x, y int) int {
	return rng.Intn(y-x+1) + x
}

func randFloat(rng *rand.Rand, x, y, shift int) float32 {
	return float32(randInt(rng, x, y)) / float32(shift)
}

// TODO(dan): This is not even a little bit right. See 4.2.2.10.
func randTextString(rng *rand.Rand, minLen, maxLen int) string {
	buf := make([]byte, randInt(rng, minLen, maxLen))
	for i := range buf {
		buf[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(buf)
}

// randVString returns "a string comprised of randomly generated alphanumeric
// characters within a character set of at least 64 symbols. The length of the
// string is a random value between min and max inclusive". See 4.2.2.7.
func randVString(rng *rand.Rand, minLen, maxLen int) string {
	buf := make([]byte, randInt(rng, minLen, maxLen))
	for i := range buf {
		buf[i] = alphanumericLen64[rng.Intn(len(alphanumericLen64))]
	}
	return string(buf)
}

// randPhone returns a phone number generated according to 4.2.2.9.
func randPhone(rng *rand.Rand, nationKey int) string {
	countryCode := nationKey + 10
	localNumber1 := randInt(rng, 100, 999)
	localNumber2 := randInt(rng, 1000, 9999)
	localNumber3 := randInt(rng, 1000, 9999)
	return fmt.Sprintf(`%d-%d-%d-%d`, countryCode, localNumber1, localNumber2, localNumber3)
}
