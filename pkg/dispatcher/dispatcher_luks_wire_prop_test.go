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

package dispatcher_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// A `LuksPassphrase` property set on the replica is the satellite's
// wire key itself; it must never reach the wire, whether it arrives
// through the resolved props or straight from the replica (no resolved
// props at all), nor may any other passphrase property stand in for it.
func TestLUKSReplicaWirePropNeverReachesTheWire(t *testing.T) {
	const name = "pvc-luks-replica-prop"

	replicaProps := map[string]string{passphrase.WirePropKey: "from-a-replica-prop"}

	t.Run("resolved props", func(t *testing.T) {
		got := dispatcher.BuildDesired(luksTestTarget(name, replicaProps), nil, nil, nil,
			luksTestRD(name), map[string]string{passphrase.WirePropKey: "from-a-replica-prop"})

		assertNoPassphraseOnWire(t, got)
	})

	t.Run("no resolved props", func(t *testing.T) {
		got := dispatcher.BuildDesired(luksTestTarget(name, map[string]string{
			passphrase.WirePropKey: "from-a-replica-prop",
		}), nil, nil, nil, luksTestRD(name), nil)

		assertNoPassphraseOnWire(t, got)
	})

	t.Run("with the master-key property", func(t *testing.T) {
		got := dispatcher.BuildDesired(luksTestTarget(name, replicaProps), nil, nil, nil,
			luksTestRD(name), map[string]string{
				passphrase.WirePropKey:      "from-a-replica-prop",
				passphrase.PropKeyCanonical: "from-a-controller-prop",
			})

		assertNoPassphraseOnWire(t, got)
	})
}

// With no resolved props the wire props start as the replica's own map;
// dropping the passphrase key from the wire must leave the replica, an
// object out of the caller's cache, as it was.
func TestLUKSWirePropDropLeavesTheReplicaAlone(t *testing.T) {
	const name = "pvc-luks-replica-untouched"

	target := luksTestTarget(name, map[string]string{passphrase.WirePropKey: "from-a-replica-prop", "Aux/keep": "yes"})

	_ = dispatcher.BuildDesired(target, nil, nil, nil, luksTestRD(name), nil)

	if target.Spec.Props[passphrase.WirePropKey] != "from-a-replica-prop" || target.Spec.Props["Aux/keep"] != "yes" {
		t.Errorf("the replica's props were edited: %v", target.Spec.Props)
	}
}
