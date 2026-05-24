// Copyright 2024 The Cockroach Authors.
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

package crencoding

import "math/bits"

// UvarintLen32 returns the number of bytes necessary for the Go
// encoding/binary.Uvarint encoding.
//
// It is always equivalent to len(binary.AppendUvarint(nil, uint32(x))) but faster.
func UvarintLen32(x uint32) int {
	// We |1 to avoid the special case of x=0.
	b := uint32(bits.Len32(x|1)) + 6
	// The result is b / 7. Instead of dividing by 7, we multiply by 37 which is
	// approximately 2^8/7 and then divide by 2^8. This approximation is exact for
	// small values in the range we care about.
	return int((b * 37) >> 8)
}

// UvarintLen64 returns the number of bytes necessary for the Go
// encoding/binary.Uvarint encoding.
//
// It is always equivalent to len(binary.AppendUvarint(nil, x)) but faster.
func UvarintLen64(x uint64) int {
	// We |1 to avoid the special case of x=0.
	b := uint32(bits.Len64(x|1)) + 6
	// The result is b / 7. Instead of dividing by 7, we multiply by 37 which is
	// approximately 2^8/7 and then divide by 2^8. This approximation is exact for
	// small values in the range we care about.
	return int((b * 37) >> 8)
}
