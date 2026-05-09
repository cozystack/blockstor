/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package drbd_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

func TestLowestFreePort_EmptyReturnsLow(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreePort(nil, 7000, 7999)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 7000 {
		t.Errorf("got %d, want 7000", got)
	}
}

func TestLowestFreePort_PicksGap(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreePort([]int32{7000, 7002, 7003}, 7000, 7999)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 7001 {
		t.Errorf("got %d, want 7001", got)
	}
}

func TestLowestFreePort_IgnoresOutOfRange(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreePort([]int32{6000, 8500}, 7000, 7999)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 7000 {
		t.Errorf("got %d, want 7000 (out-of-range ports must not block)", got)
	}
}

func TestLowestFreePort_Exhausted(t *testing.T) {
	t.Parallel()

	taken := make([]int32, 0, 10)
	for i := int32(7000); i <= 7009; i++ {
		taken = append(taken, i)
	}

	_, err := drbd.LowestFreePort(taken, 7000, 7009)
	if !errors.Is(err, drbd.ErrPortPoolExhausted) {
		t.Errorf("err: got %v, want ErrPortPoolExhausted", err)
	}
}

func TestLowestFreeMinor_PicksGap(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreeMinor([]int32{1000, 1001, 1003}, 1000, 65535)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 1002 {
		t.Errorf("got %d, want 1002", got)
	}
}

func TestLowestFreeMinor_Exhausted(t *testing.T) {
	t.Parallel()

	_, err := drbd.LowestFreeMinor([]int32{1000, 1001}, 1000, 1001)
	if !errors.Is(err, drbd.ErrMinorPoolExhausted) {
		t.Errorf("err: got %v, want ErrMinorPoolExhausted", err)
	}
}

func TestParseRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in        string
		low, high int32
		wantErr   bool
	}{
		{"7000-7999", 7000, 7999, false},
		{" 7000 - 7999 ", 7000, 7999, false},
		{"1-1", 1, 1, false},
		{"abc", 0, 0, true},
		{"7000", 0, 0, true},
		{"7999-7000", 0, 0, true}, // low > high
		{"-1", 0, 0, true},
	}

	for _, tc := range cases {
		low, high, err := drbd.ParseRange(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseRange(%q): err=%v wantErr=%v", tc.in, err, tc.wantErr)

			continue
		}

		if !tc.wantErr && (low != tc.low || high != tc.high) {
			t.Errorf("ParseRange(%q): got (%d, %d), want (%d, %d)", tc.in, low, high, tc.low, tc.high)
		}
	}
}
