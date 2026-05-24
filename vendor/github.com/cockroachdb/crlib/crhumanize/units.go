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

package crhumanize

import (
	"fmt"
	"strings"
)

var IEC = makeUnits(1024, "Ki", "Mi", "Gi", "Ti", "Pi", "Ei")
var SI = makeUnits(1000, "K", "M", "G", "T", "P", "E")

type Units struct {
	prefixes []string
	scales   []uint64
}

func makeUnits(scale uint64, prefixes ...string) Units {
	u := Units{prefixes: prefixes}
	u.scales = make([]uint64, len(prefixes))
	u.scales[0] = scale
	for i := 1; i < len(prefixes); i++ {
		u.scales[i] = u.scales[i-1] * scale
	}
	return u
}

// Round finds the closest unit and returns the scaled value and unit prefix.
func (u Units) Round(value uint64) (scaled float64, prefix string) {
	n := 0
	scale := uint64(1)
	// Find the largest unit <= value.
	for ; n < len(u.scales) && value >= u.scales[n]; n++ {
		scale = u.scales[n]
	}
	// If the scaled value requires four digits, move up to the next scale.
	if n < len(u.scales) && (value+scale-1)/scale >= 1000 {
		n++
	}
	if n == 0 {
		return float64(value), ""
	}
	return float64(value) / float64(u.scales[n-1]), u.prefixes[n-1]
}

// Exact finds the largest unit that is an exact divisor of the value.
func (u Units) Exact(value uint64) (scaled uint64, prefix string) {
	if value == 0 {
		return 0, ""
	}
	n := 0
	for ; n < len(u.scales) && value%u.scales[n] == 0; n++ {
	}
	if n == 0 {
		return value, ""
	}
	return value / u.scales[n-1], u.prefixes[n-1]
}

func parseUnit(s string) (scale uint64, _ error) {
	if s == "" {
		return 1, nil
	}
	for _, u := range []Units{SI, IEC} {
		for i := range u.prefixes {
			if strings.EqualFold(s, u.prefixes[i]) {
				return u.scales[i], nil
			}
		}
	}
	return 0, fmt.Errorf("unknown unit %q", s)
}
