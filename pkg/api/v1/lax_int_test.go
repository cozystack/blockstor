/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import (
	"encoding/json"
	"testing"
)

func TestLaxInt32UnmarshalJSON(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  LaxInt32
	}{
		{"bare integer", `2`, 2},
		{"quoted integer (upstream CLI quirk)", `"3"`, 3},
		{"zero", `0`, 0},
		{"quoted zero", `"0"`, 0},
		{"negative", `-1`, -1},
		{"quoted negative", `"-1"`, -1},
		{"null", `null`, 0},
		{"empty string", `""`, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got LaxInt32
			if err := json.Unmarshal([]byte(tc.input), &got); err != nil {
				t.Fatalf("unmarshal %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLaxInt32UnmarshalJSONReject(t *testing.T) {
	bads := []string{`"abc"`, `"1.5"`, `1.5`, `"99999999999999999999"`}
	for _, b := range bads {
		t.Run(b, func(t *testing.T) {
			var got LaxInt32
			if err := json.Unmarshal([]byte(b), &got); err == nil {
				t.Errorf("expected error for %q, got %d", b, got)
			}
		})
	}
}

func TestLaxInt32MarshalJSON(t *testing.T) {
	v := LaxInt32(2)
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != "2" {
		t.Errorf("got %q, want %q (bare integer, no quotes)", string(out), "2")
	}
}

// TestAutoSelectFilterCLIQuirk pins the wire shape that triggered the
// fix: linstor-client serializes integer flags as quoted JSON strings.
// Before LaxInt32, this body returned 400 with
// "cannot unmarshal string into Go struct field AutoSelectFilter.select_filter.place_count of type int32".
func TestAutoSelectFilterCLIQuirk(t *testing.T) {
	body := `{"place_count":"2","additional_place_count":"1","storage_pool":"stand"}`
	var f AutoSelectFilter
	if err := json.Unmarshal([]byte(body), &f); err != nil {
		t.Fatalf("CLI-quoted body should decode: %v", err)
	}
	if f.PlaceCount != 2 {
		t.Errorf("PlaceCount = %d, want 2", f.PlaceCount)
	}
	if f.AdditionalPlaceCount != 1 {
		t.Errorf("AdditionalPlaceCount = %d, want 1", f.AdditionalPlaceCount)
	}
}
