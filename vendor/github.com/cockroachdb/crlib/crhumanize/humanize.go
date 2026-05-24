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

// Bytes formats a byte value in IEC units.
//
// Unless Exact is used, the result is approximate (within ~5%).
//
// Examples: "1.5 MiB", "21 GiB", "3 B".
func Bytes[T Integer](bytes T, flags ...FmtFlag) SafeString {
	return Format(bytes, IEC, "B", flags...)
}

// ParseBytes parses a string representation of a byte value.
//
// Unless they have the i indicator, the units are interpreted as SI units. For
// example, "1 KB" = 1,000 whereas "1 KiB" = 1,024.
//
// The space separator and the B suffix are optional.
func ParseBytes[T Integer](s string) (T, error) {
	return Parse[T](s, "B")
}

// BytesPerSec formats a bytes-per-second value in IEC units.
//
// Unless Exact is used, the result is approximate (within 5%).
//
// Examples: "1.5 MiB/s", "21 GiB/s", "3 B/s".
func BytesPerSec[T Integer](bytes T, flags ...FmtFlag) SafeString {
	return Format(bytes, IEC, "B/s", flags...)
}

// ParseBytesPerSec parses a string representation of a bytes-per-second value.
//
// Unless they have the i indicator, the units are interpreted as SI units. For
// example, "1 KB/s" = 1,000 whereas "1 KiB/s" = 1,024.
//
// The space separator and the B/s suffix are optional.
func ParseBytesPerSec[T Integer](s string) (T, error) {
	return Parse[T](s, "B/s")
}

// Count formats a unitless value in SI units.
//
// Unless Exact is used, the result is approximate (within 5%).
//
// Examples: "123", "1.2 K", "12 M".
func Count[T Integer](count T, flags ...FmtFlag) SafeString {
	return Format(count, SI, "", flags...)
}

// ParseCount parses a string representation of a unitless value.
//
// Unless they have the i indicator, the units are interpreted as SI units. For
// example, "1 K" = 1,000 whereas "1 Ki" = 1,024.
//
// The space separator is optional.
func ParseCount[T Integer](s string) (T, error) {
	return Parse[T](s, "")
}

// Integer is a constraint that permits any integer type.
type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// Numeric is a constraint that permits any integer or floating-point type.
type Numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr | ~float32 | ~float64
}

// SafeString represents a human readable representation of a value. It
// implements a `SafeValue()` marker method (implementing the
// github.com/cockroachdb/redact.SafeValue interface) to signal that it
// represents a string that does not need to be redacted.
type SafeString string

// SafeValue implements cockroachdb/redact.SafeValue.
func (fs SafeString) SafeValue() {}

// String implements fmt.Stringer.
func (fs SafeString) String() string { return string(fs) }
