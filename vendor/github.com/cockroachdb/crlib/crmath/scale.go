// Copyright 2025 The Cockroach Authors.
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

package crmath

import (
	"math"
	"math/bits"
)

// ScaleUint64 returns the ceiling of ax/b, clamped to the uint64 range. Panics
// if b is zero.
//
// More precisely, returns:
//
//	min( ⌈ x*a / b ⌉, math.MaxUint64 )
func ScaleUint64(x uint64, a uint64, b uint64) uint64 {
	var quo, rem uint64
	if x < math.MaxUint32 && a < math.MaxUint32 {
		prod := a * x
		quo = prod / b
		rem = prod % b
	} else {
		hi, lo := bits.Mul64(x, a)
		if hi >= b && b != 0 {
			return math.MaxUint64
		}
		quo, rem = bits.Div64(hi, lo, b)
	}
	if rem == 0 {
		return quo
	}
	return quo + 1
}
