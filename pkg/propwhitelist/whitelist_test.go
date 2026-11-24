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

package propwhitelist_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/propwhitelist"
)

func TestCheckRefusesAKeyNotWhitelisted(t *testing.T) {
	t.Parallel()

	for _, obj := range []propwhitelist.Object{
		propwhitelist.Controller,
		propwhitelist.Node,
		propwhitelist.ResourceDefinition,
		propwhitelist.VolumeDefinition,
		propwhitelist.Resource,
		propwhitelist.StoragePoolDefinition,
	} {
		err := propwhitelist.Check(obj, "DrbdOptions/AutoSnapshot/Keep", "2")

		var refusal *propwhitelist.Error
		if !errors.As(err, &refusal) {
			t.Fatalf("%s: DrbdOptions/AutoSnapshot/Keep accepted (err %v)", obj, err)
		}

		if refusal.Known {
			t.Errorf("%s: refusal says the key is known: %v", obj, refusal)
		}
	}
}

func TestCheckLeavesOtherNamespacesAlone(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"AutoSnapshot/Keep", "Aux/anything", "DrbdOptionsOther/x", "StorDriver/LvmVg"} {
		err := propwhitelist.Check(propwhitelist.ResourceDefinition, key, "x")
		if err != nil {
			t.Errorf("%s refused: %v", key, err)
		}
	}
}

func TestCheckAcceptsWhatLinstorAccepts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		obj        propwhitelist.Object
		key, value string
	}{
		{propwhitelist.ResourceDefinition, "DrbdOptions/Net/protocol", "C"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Net/allow-two-primaries", "YES"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Resource/quorum", "majority"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Resource/quorum", "2"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Resource/on-no-quorum", "suspend-io"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Disk/al-extents", "6007"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Handlers/fence-peer", "/usr/lib/drbd/crm-fence-peer.9.sh"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/auto-quorum", "suspend-io"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/auto-add-quorum-tiebreaker", "False"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/SkipDisk", "True"},
		{propwhitelist.Controller, "DrbdOptions/AutoEvictAllowEviction", "false"},
		{propwhitelist.Controller, "DrbdOptions/Net/max-buffers", "10000"},
		{propwhitelist.Node, "DrbdOptions/AutoEvictAllowEviction", "false"},
		{propwhitelist.Resource, "DrbdOptions/SkipDisk", "True"},
		{propwhitelist.VolumeDefinition, "DrbdOptions/Disk/rs-discard-granularity", "65536"},
	}

	for _, tc := range cases {
		err := propwhitelist.Check(tc.obj, tc.key, tc.value)
		if err != nil {
			t.Errorf("%s %s=%s refused: %v", tc.obj, tc.key, tc.value, err)
		}
	}
}

func TestCheckRefusesAKeyOnTheWrongObject(t *testing.T) {
	t.Parallel()

	cases := []struct {
		obj propwhitelist.Object
		key string
	}{
		// A volume definition takes disk options only.
		{propwhitelist.VolumeDefinition, "DrbdOptions/Net/protocol"},
		// A node takes no DRBD section options at all.
		{propwhitelist.Node, "DrbdOptions/Net/protocol"},
		{propwhitelist.Resource, "DrbdOptions/Resource/quorum"},
	}

	for _, tc := range cases {
		err := propwhitelist.Check(tc.obj, tc.key, "C")

		var refusal *propwhitelist.Error
		if !errors.As(err, &refusal) || refusal.Known {
			t.Errorf("%s %s: want a not-whitelisted refusal, got %v", tc.obj, tc.key, err)
		}
	}
}

func TestCheckRefusesAValueTheRuleRefuses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		key, value, detail string
	}{
		{
			"DrbdOptions/Resource/quorum-minimum-redundancy", "bogus",
			"This value has to match (1 - 32) or (?:all|majority|off).",
		},
		{
			"DrbdOptions/Resource/quorum", "33",
			"This value has to match (1 - 32) or (?:all|majority|off).",
		},
		{"DrbdOptions/Net/protocol", "D", "This value has to match (?:A|B|C)."},
		{"DrbdOptions/Net/allow-two-primaries", "true", "This value must be either 'yes' or 'no'."},
		{"DrbdOptions/Disk/al-extents", "1M", "This value has to match (67 - 65534)."},
		{"DrbdOptions/Net/timeout", "0", "This value  has to match (1 - 600) 1/10 seconds."},
		{
			"DrbdOptions/auto-add-quorum-tiebreaker", "maybe",
			"This value must be either 'yes', 'no', 'true' or 'false'.",
		},
		{"DrbdOptions/auto-diskful", "ten", "This value has to be of type Long."},
	}

	for _, tc := range cases {
		err := propwhitelist.Check(propwhitelist.ResourceDefinition, tc.key, tc.value)

		var refusal *propwhitelist.Error
		if !errors.As(err, &refusal) {
			t.Errorf("%s=%s accepted", tc.key, tc.value)

			continue
		}

		if !refusal.Known || refusal.Detail != tc.detail {
			t.Errorf("%s=%s: got known=%v detail %q, want %q", tc.key, tc.value, refusal.Known, refusal.Detail, tc.detail)
		}
	}
}

func TestCheckAllReportsTheFirstKeyInOrder(t *testing.T) {
	t.Parallel()

	err := propwhitelist.CheckAll(propwhitelist.ResourceDefinition, map[string]string{
		"DrbdOptions/Net/protocol":      "C",
		"DrbdOptions/Zeta/unknown":      "1",
		"DrbdOptions/AutoSnapshot/Keep": "2",
	})

	var refusal *propwhitelist.Error
	if !errors.As(err, &refusal) || refusal.Key != "DrbdOptions/AutoSnapshot/Keep" {
		t.Fatalf("got %v, want the refusal of DrbdOptions/AutoSnapshot/Keep", err)
	}
}

func TestCheckAcceptsBlockstorsOwnKeysWhereTheyAreRead(t *testing.T) {
	t.Parallel()

	accepted := []struct {
		obj        propwhitelist.Object
		key, value string
	}{
		{propwhitelist.ResourceDefinition, "DrbdOptions/AutoAddQuorumTiebreaker", "false"},
		{propwhitelist.Controller, "DrbdOptions/AutoAddQuorumTiebreaker", "yes"},
		{propwhitelist.Node, "DrbdOptions/TcpPortRange", "21000-21999"},
		{propwhitelist.Node, "DrbdOptions/MinorNrRange", "21000-29999"},
	}

	for _, c := range accepted {
		err := propwhitelist.Check(c.obj, c.key, c.value)
		if err != nil {
			t.Errorf("%s %s=%s refused: %v", c.obj, c.key, c.value, err)
		}
	}

	refused := []struct {
		obj        propwhitelist.Object
		key, value string
		known      bool
	}{
		{propwhitelist.Node, "DrbdOptions/AutoAddQuorumTiebreaker", "false", false},
		{propwhitelist.ResourceDefinition, "DrbdOptions/TcpPortRange", "21000-21999", false},
		{propwhitelist.Controller, "DrbdOptions/MinorNrRange", "21000-29999", false},
		{propwhitelist.ResourceDefinition, "DrbdOptions/AutoAddQuorumTiebreaker", "1", true},
		{propwhitelist.Node, "DrbdOptions/TcpPortRange", "21000", true},
	}

	for _, c := range refused {
		var refusal *propwhitelist.Error

		err := propwhitelist.Check(c.obj, c.key, c.value)
		if !errors.As(err, &refusal) {
			t.Errorf("%s %s=%s accepted", c.obj, c.key, c.value)

			continue
		}

		if refusal.Known != c.known {
			t.Errorf("%s %s=%s: Known = %v, want %v", c.obj, c.key, c.value, refusal.Known, c.known)
		}
	}
}

func TestNormalizeStoresBooleansAsLinstorDoes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		obj               propwhitelist.Object
		key, value, wants string
	}{
		{propwhitelist.ResourceDefinition, "DrbdOptions/Net/load-balance-paths", "Yes", "yes"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Net/allow-two-primaries", "NO", "no"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/auto-add-quorum-tiebreaker", "No", "false"},
		{propwhitelist.Controller, "DrbdOptions/AutoEvictAllowEviction", "YES", "true"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/AutoAddQuorumTiebreaker", "False", "false"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Net/protocol", "C", "C"},
		{propwhitelist.ResourceDefinition, "Aux/Flag", "Yes", "Yes"},
		{propwhitelist.ResourceDefinition, "DrbdOptions/Net/load-balance-paths", "Maybe", "Maybe"},
	}

	for _, c := range cases {
		if got := propwhitelist.Normalize(c.obj, c.key, c.value); got != c.wants {
			t.Errorf("%s %s=%s: normalized to %q, want %q", c.obj, c.key, c.value, got, c.wants)
		}
	}
}
