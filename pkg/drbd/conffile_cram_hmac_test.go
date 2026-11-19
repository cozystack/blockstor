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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// netBlock returns the rendered `net { … }` block of a .res body.
func netBlock(t *testing.T, body string) string {
	t.Helper()

	start := strings.Index(body, "  net {\n")
	if start < 0 {
		t.Fatalf("no net block in:\n%s", body)
	}

	end := strings.Index(body[start:], "\n  }\n")
	if end < 0 {
		t.Fatalf("unterminated net block in:\n%s", body)
	}

	return body[start : start+end]
}

// A shared secret on its own does not turn authentication on. DRBD
// authenticates a peer only when cram-hmac-alg is set, so a renderer that
// added one next to every secret would switch a resource that ran
// unauthenticated over to authentication on whichever satellite rolled
// first, and its reconnects to peers not rolled yet would fail the
// handshake. The digest comes from DrbdOptions/Net/cram-hmac-alg only.
func TestASharedSecretAloneRendersNoCramHMACAlg(t *testing.T) {
	for _, tc := range []struct {
		name string
		net  drbd.Net
	}{
		{"typed secret", drbd.Net{SharedSecret: "typed"}},
		{"secret as a property", drbd.Net{Options: map[string]string{"shared-secret": "+k6fjase/NG="}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := drbd.Build(drbd.Resource{Name: "pvc-1", Net: tc.net})
			if err != nil {
				t.Fatalf("Build: %v", err)
			}

			net := netBlock(t, got)

			if !strings.Contains(net, "shared-secret") {
				t.Fatalf("the secret was not rendered:\n%s", got)
			}

			if strings.Contains(net, "cram-hmac-alg") {
				t.Errorf("cram-hmac-alg rendered without being asked for:\n%s", got)
			}
		})
	}
}

// An operator's DrbdOptions/Net/cram-hmac-alg is the digest used, and it
// is written once: drbdadm rejects a key repeated in one section.
func TestAnExplicitCramHMACAlgWins(t *testing.T) {
	for _, key := range []string{"cram-hmac-alg", "Cram-HMAC-Alg"} {
		t.Run(key, func(t *testing.T) {
			got, err := drbd.Build(drbd.Resource{
				Name: "pvc-1",
				Net: drbd.Net{Options: map[string]string{
					"shared-secret": "s3cret",
					key:             "sha256",
				}},
			})
			if err != nil {
				t.Fatalf("Build: %v", err)
			}

			net := netBlock(t, got)

			if n := strings.Count(strings.ToLower(net), "cram-hmac-alg"); n != 1 {
				t.Fatalf("want exactly one cram-hmac-alg line, got %d:\n%s", n, got)
			}

			if !strings.Contains(net, key+" sha256;") {
				t.Errorf("the explicit digest was not kept:\n%s", got)
			}
		})
	}
}

// The full net section of a resource adopted from LINSTOR, byte for byte:
// the satellite-level timing knobs from ControllerConfig, and the
// definition's secret with the digest the converter stamps next to it.
func TestAdoptedNetSectionRendersAsLinstorRanIt(t *testing.T) {
	got, err := drbd.Build(drbd.Resource{
		Name: "pvc-1",
		Net: drbd.Net{ProtocolC: true, Options: map[string]string{
			"connect-int":   "15",
			"cram-hmac-alg": drbd.DefaultCramHMACAlg,
			"ping-int":      "12",
			"ping-timeout":  "10",
			"shared-secret": "s3cret",
			"timeout":       "90",
			"verify-alg":    "crc32c",
		}},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	want := "  net {\n" +
		"    connect-int 15;\n" +
		"    cram-hmac-alg sha1;\n" +
		"    ping-int 12;\n" +
		"    ping-timeout 10;\n" +
		"    shared-secret \"s3cret\";\n" +
		"    timeout 90;\n" +
		"    verify-alg crc32c;"

	if net := netBlock(t, got); net != want {
		t.Errorf("net block:\n%s\nwant:\n%s", net, want)
	}
}
