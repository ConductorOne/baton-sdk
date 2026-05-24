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
	"math"
	"strconv"
	"strings"
)

// Float formats the given float with the specified number of decimal digits.
// Trailing 0 decimals are stripped.
func Float(value float64, decimalDigits int) SafeString {
	s := strconv.FormatFloat(value, 'f', decimalDigits, 64)
	s = stripTrailingZeroDecimals(s)
	return SafeString(s)
}

func stripTrailingZeroDecimals(s string) string {
	if !strings.ContainsRune(s, '.') {
		return s
	}
	for s[len(s)-1] == '0' {
		s = s[:len(s)-1]
	}
	if s[len(s)-1] == '.' {
		s = s[:len(s)-1]
	}
	return s
}

// Percent formats (numerator/denominator) as a percentage. At most one decimal
// digit is used (only when the integer part is a single digit). If denominator
// is 0, returns the empty string.
//
// Values very close to 0 are formatted as ~0% to indicate that the value is
// non-zero.
//
// Examples: "0.2%", "12%".
func Percent[T Numeric](numerator, denominator T) SafeString {
	if denominator == 0 {
		return ""
	}
	if numerator == 0 {
		return "0%"
	}
	value := (float64(numerator) / float64(denominator)) * 100
	if math.Abs(value) < 0.05 {
		return "~0%"
	}
	decimalDigits := 0
	if math.Abs(value) < 9.95 {
		decimalDigits = 1
	}
	return Float(value, decimalDigits) + "%"
}
