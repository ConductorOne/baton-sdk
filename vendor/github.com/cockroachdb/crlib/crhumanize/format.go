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
	"strconv"
	"strings"
)

// FmtFlag is an optional flag that can be passed to formatting functions.
type FmtFlag uint8

const (
	// Exact formats the value without loss of precision; the result can always be
	// parsed back into the original value.
	//
	// Examples: "1,000 B", "1,234 KiB", "21,000 GiB", "1,000,000 B".
	Exact FmtFlag = iota

	// Compact omits the space between the value and the unit.
	Compact

	// OmitI omits the "i" in the IEC units. Use with care: the result should
	// never be parsed back, as the units will be interpreted as SI units.
	OmitI

	numFlags
)

// Format a value using the given unit prefixes (SI or IEC). An arbitrary
// unit string is appended at the end.
//
// Unless Exact is used, the result is approximate (within ~5%).
func Format[T Integer](value T, units Units, unit string, flags ...FmtFlag) SafeString {
	var b strings.Builder
	asUint64 := uint64(value)
	if value < 0 {
		b.WriteByte('-')
		// Note: uint64(-value) would not work correctly when value is the minimum
		// value for a smaller type (e.g. -128 with T=uint8).
		asUint64 = -asUint64
	}
	formatUint64(&b, asUint64, units, unit, flags...)
	return SafeString(b.String())
}

func formatUint64(b *strings.Builder, value uint64, units Units, unit string, flags ...FmtFlag) {
	var flagBitmap [numFlags]bool
	for _, flag := range flags {
		flagBitmap[flag] = true
	}
	var unitPrefix string
	if flagBitmap[Exact] {
		var scaled uint64
		scaled, unitPrefix = units.Exact(value)
		valStr := strconv.FormatUint(scaled, 10)
		b.Grow(len(valStr)*4/3 + len(unitPrefix) + 2 + len(unit))
		// Add commas to make large numbers readable.
		n := 1 + (len(valStr)-1)%3 // length of the first digit group.
		b.WriteString(valStr[:n])
		for i := n; i < len(valStr); i += 3 {
			b.WriteByte(',')
			b.WriteString(valStr[i : i+3])
		}
	} else {
		var scaled float64
		scaled, unitPrefix = units.Round(value)
		digits := 0
		if scaled < 10 {
			digits = 1
		}
		valStr := string(Float(scaled, digits))
		b.Grow(len(valStr) + len(unitPrefix) + len(unit) + 2)
		b.WriteString(valStr)
	}
	if !flagBitmap[Compact] && len(unitPrefix)+len(unit) > 0 {
		b.WriteByte(' ')
	}
	if flagBitmap[OmitI] {
		if flagBitmap[Exact] {
			// Exact guarantees that the value will be parsed back exactly, which is
			// not the case if we omit the i in IEC units.
			panic("Exact and OmitI flags cannot be used together")
		}
		unitPrefix = strings.TrimSuffix(unitPrefix, "i")
	}
	b.WriteString(unitPrefix)
	b.WriteString(unit)
}
