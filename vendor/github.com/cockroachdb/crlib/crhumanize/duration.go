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
	"time"
)

// Duration returns a simplified approximation (±5%) of a duration.
//
// Examples:
//   - 123.456µs -> "123µs"
//   - 1.234567ms -> "1.2ms"
//   - 59.1s -> "59s"
//   - 1m6.5s -> 1m7s
func Duration(d time.Duration) SafeString {
	if d < 0 {
		return "-" + Duration(-d)
	}
	if d == 0 {
		return "0s"
	}

	if d < 10*time.Minute {
		// Round to a precision that gives us one decimal when the value is a single
		// digit (e.g. "1.5ms" vs "12ms")
		r := time.Nanosecond
		switch {
		case d < time.Microsecond:
		case d < 10*time.Microsecond:
			r = 100 * time.Nanosecond
		case d < time.Millisecond:
			r = time.Microsecond
		case d < 10*time.Millisecond:
			r = 100 * time.Microsecond
		case d < time.Second:
			r = time.Millisecond
		case d < 10*time.Second:
			r = 100 * time.Millisecond
		default:
			r = time.Second
		}
		return SafeString(d.Round(r).String())
	}

	if d > 100*time.Hour {
		d = d.Round(time.Hour)
	} else {
		d = d.Round(time.Minute)
	}
	h := int(d / time.Hour)
	m := int((d % time.Hour) / time.Minute)
	switch {
	case h == 0:
		return SafeString(fmt.Sprintf("%dm", m))
	case m == 0:
		return SafeString(fmt.Sprintf("%dh", h))
	default:
		return SafeString(fmt.Sprintf("%dh%dm", h, m))
	}
}
