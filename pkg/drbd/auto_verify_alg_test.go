// SPDX-License-Identifier: Apache-2.0

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
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// LINSTOR's controller stores a verify algorithm on each definition and
// its satellite renders it as verify-alg when none is set explicitly. A
// definition adopted from LINSTOR carries the property; dropping it as a
// controller-only key took verify-alg off its kernels.
func TestFillAutoVerifyAlg(t *testing.T) {
	const (
		auto    = "DrbdOptions/auto-verify-alg"
		disable = "DrbdOptions/auto-verify-algo-disable"
	)

	for _, tc := range []struct {
		name  string
		net   map[string]string
		props map[string]string
		want  string
		set   bool
	}{
		{name: "fills an unset verify-alg", props: map[string]string{auto: "crct10dif"}, want: "crct10dif", set: true},
		{name: "explicit verify-alg wins", net: map[string]string{"verify-alg": "sha256"}, props: map[string]string{auto: "crc32c"}, want: "sha256", set: true},
		{name: "explicit verify-alg in any case wins", net: map[string]string{"Verify-Alg": "sha256"}, props: map[string]string{auto: "crc32c"}},
		{name: "nothing to fill from", props: map[string]string{}},
		{name: "disabled", props: map[string]string{auto: "crc32c", disable: "true"}},
		{name: "disabled by any value but false", props: map[string]string{auto: "crc32c", disable: "yes"}},
		{name: "explicitly not disabled", props: map[string]string{auto: "crc32c", disable: "False"}, want: "crc32c", set: true},
		{name: "windrbd gets crc32c", props: map[string]string{auto: "windrbd"}, want: "crc32c", set: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			net := map[string]string{}
			for k, v := range tc.net {
				net[k] = v
			}

			drbd.FillAutoVerifyAlg(net, tc.props)

			got, set := net["verify-alg"]
			if set != tc.set || got != tc.want {
				t.Errorf("verify-alg = %q (set %v), want %q (set %v); net %v", got, set, tc.want, tc.set, net)
			}
		})
	}
}

func TestSectionLess(t *testing.T) {
	for key, want := range map[string]bool{
		"DrbdOptions/auto-verify-alg":   true,
		"DrbdOptions/Net/verify-alg":    false,
		"DrbdOptions/Resource/quorum":   false,
		"StorPoolName":                  false,
		"Aux/DrbdOptions/auto-anything": false,
	} {
		if got := drbd.SectionLess(key); got != want {
			t.Errorf("SectionLess(%q) = %v, want %v", key, got, want)
		}
	}
}
