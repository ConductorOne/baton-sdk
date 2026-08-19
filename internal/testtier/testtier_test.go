package testtier

import "testing"

func TestEnabled(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset"},
		{name: "one", value: "1", want: true},
		{name: "zero", value: "0"},
		{name: "false", value: "false"},
		{name: "true", value: "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(ExtraEnv, tc.value)
			if got := enabled(ExtraEnv); got != tc.want {
				t.Fatalf("enabled(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
