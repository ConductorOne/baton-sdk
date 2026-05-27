package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeWorkerCount(t *testing.T) {
	t.Parallel()

	auto := min(max(runtime.GOMAXPROCS(0), 1), 4)

	tests := []struct {
		name string
		in   int
		want int
	}{
		{"minus_one_auto", -1, auto},
		{"zero_sequential", 0, 1},
		{"positive", 7, 7},
		{"below_minus_one_sequential", -99, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeWorkerCount(tt.in)
			require.Equal(t, tt.want, got)
		})
	}
}
