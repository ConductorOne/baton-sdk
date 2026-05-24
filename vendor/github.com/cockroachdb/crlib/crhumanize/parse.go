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
	"math/big"
	"strings"
	"unsafe"
)

// Parse a string representation of a humanized value.
//
// Unless they have the i indicator, the units are interpreted as SI units. For
// example, "1 K" = 1,000 whereas "1 Ki" = 1,024.
//
// An arbitrary unit can be specified; it is optional in the string. The space
// separator is also optional.
func Parse[T Integer](s string, unit string) (T, error) {
	value, scale, err := preParse(s, unit)
	if err != nil {
		return T(0), err
	}
	unsignedType := T(0)-1 > T(0)
	if unsignedType && value.Sign() < 0 {
		return T(0), fmt.Errorf("value cannot be negative in %q", s)
	}

	valBits := int(unsafe.Sizeof(T(0)) * 8)
	if !unsignedType {
		valBits--
	}

	// Check that the value is in the range of the type T.
	if ok := func() bool {
		valBitLen := value.BitLen()
		if valBitLen <= valBits {
			return true
		}
		if valBitLen == valBits+1 && int(value.TrailingZeroBits()) == valBits {
			// The absolute value is exactly 1 << valBits.
			if value.Sign() < 0 {
				// Ok for negative values.
				return true
			}
			if scale != 1 {
				// We also tolerate it as a special case when a unit prefix was used: we
				// want to be able to parse anything that format() produces, and for
				// example format(MaxInt32, IEC) produces "2 Gi".
				value.Sub(value, big.NewInt(1))
				return true
			}
		}
		return false
	}(); !ok {
		return 0, fmt.Errorf("cannot parse value from %q: out of range", s)
	}

	if unsignedType {
		return T(value.Uint64()), nil
	}
	return T(value.Int64()), nil
}

func preParse(s string, unit string) (value *big.Int, scale uint64, _ error) {
	s = strings.TrimSpace(s)

	numStr := s
	for i, r := range s {
		if r == '-' || (r >= '0' && r <= '9') || r == '.' || r == ',' {
			continue
		}
		numStr = s[:i]
		break
	}
	suffix := strings.TrimSpace(s[len(numStr):])
	// Remove the unit (but don't require it).
	if n := len(unit); n > 0 && len(suffix) >= n && strings.EqualFold(suffix[len(suffix)-n:], unit) {
		suffix = suffix[:len(suffix)-n]
	}

	numStr = strings.ReplaceAll(numStr, ",", "")
	// To avoid loss of precision and numeric overflow, we use big.Float and big.Int.
	number, _, err := big.ParseFloat(numStr, 10, 128, big.ToNearestEven)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot parse number from %q", s)
	}

	scale, err = parseUnit(suffix)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot parse %q: %v", s, err)
	}

	if scale == 1 && !number.IsInt() {
		// Don't allow floating-point numbers without a unit prefix.
		return nil, 0, fmt.Errorf("cannot parse %q: number without unit prefix must be an integer", s)
	}

	if scale > 1 {
		var scaleFloat big.Float
		scaleFloat.SetPrec(128)
		scaleFloat.SetUint64(scale)
		number.Mul(number, &scaleFloat)
	}

	// Round the number to the nearest integer.
	if number.Sign() < 0 {
		number.Sub(number, big.NewFloat(0.5))
	} else {
		number.Add(number, big.NewFloat(0.5))
	}
	value, _ = number.Int(nil)

	return value, scale, nil
}
