package field

import (
	"testing"

	batonsync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/stretchr/testify/require"
)

func TestEffectiveTaskConcurrency(t *testing.T) {
	t.Parallel()

	auto := batonsync.NormalizeWorkerCount(-1)

	tests := []struct {
		name string
		in   int
		want int
	}{
		{"minus_one_matches_normalize_then_positive", -1, auto},
		{"zero_sequential_becomes_one_slot", 0, 1},
		{"below_minus_one_sequential_becomes_one_slot", -99, 1},
		{"positive_pass_through", 8, 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EffectiveTaskConcurrency(tt.in)
			require.Equal(t, tt.want, got)
		})
	}

	t.Run("matches_normalize_for_positive_inputs", func(t *testing.T) {
		t.Parallel()
		for _, n := range []int{1, 2, 3, 10} {
			require.Equal(t, batonsync.NormalizeWorkerCount(n), EffectiveTaskConcurrency(n),
				"n=%d", n)
		}
	})
}
