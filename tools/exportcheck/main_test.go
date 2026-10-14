package main

import "testing"

func TestSizeRoundTrip(t *testing.T) {
	cases := []typeSize{
		{"pkg.T", 21, 0},
		{"pkg.T", 0, 16},
		{"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble.Engine", 305, 37},
	}
	for _, tc := range cases {
		line := sizeLine(tc.name, tc.methods, tc.fields)
		got, ok := parseSizeLine(line)
		if !ok || got != tc {
			t.Fatalf("round trip %q -> %+v ok=%v", line, got, ok)
		}
	}
	if _, ok := parseSizeLine("pkg.Name"); ok {
		t.Fatal("export line parsed as a size")
	}
	if _, ok := parseSizeLine("size pkg.T methods=1 fields=2 trailing"); ok {
		t.Fatal("trailing junk parsed as a size")
	}
}

func TestSizeShrank(t *testing.T) {
	accepted := map[string]bool{
		sizeLine("pkg.T", 30, 10): true,
		"pkg.Unused":              true,
	}
	shrinks := []typeSize{
		{"pkg.T", 30, 10},
		{"pkg.T", 20, 10},
		{"pkg.T", 30, 4},
		{"pkg.T", 1, 1},
	}
	for _, cur := range shrinks {
		if !sizeShrank(accepted, cur) {
			t.Fatalf("%+v should pass against the pin", cur)
		}
	}
	grows := []typeSize{
		{"pkg.T", 31, 10},
		{"pkg.T", 30, 11},
		{"pkg.T", 20, 11},
		{"pkg.Other", 1, 1},
	}
	for _, cur := range grows {
		if sizeShrank(accepted, cur) {
			t.Fatalf("%+v should not pass against the pin", cur)
		}
	}
}

func TestLoadScope(t *testing.T) {
	got, err := loadScope(".", ".")
	if err != nil {
		t.Fatal(err)
	}
	if !got["github.com/conductorone/baton-sdk/tools/exportcheck"] {
		t.Fatalf("scope: %v", got)
	}
	if _, err := loadScope(".", "./no/such/..."); err == nil {
		t.Fatal("missing pattern returned no error")
	}
	if _, err := loadScope(".", ".,"); err == nil {
		t.Fatal("empty pattern returned no error")
	}
}
