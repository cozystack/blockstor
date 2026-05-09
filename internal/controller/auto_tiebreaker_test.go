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

package controller_test

import (
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestIsAutoTieBreakerEnabledDefault: a fresh RD with no Props →
// auto-tiebreaker is enabled. This is the operator-default piraeus-
// operator and `linstor` rely on so 2-replica RDs automatically
// land a DISKLESS witness for quorum.
func TestIsAutoTieBreakerEnabledDefault(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{}
	if !controllerpkg.IsAutoTieBreakerEnabled(rd) {
		t.Errorf("default: got false, want true (default-on contract)")
	}
}

// TestIsAutoTieBreakerEnabledEmptyProps: an RD with non-nil but
// empty Props map still returns true — the prop's mere absence is
// the trigger, not the absence of the whole map.
func TestIsAutoTieBreakerEnabledEmptyProps(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{},
		},
	}

	if !controllerpkg.IsAutoTieBreakerEnabled(rd) {
		t.Errorf("empty props: got false, want true")
	}
}

// TestIsAutoTieBreakerEnabledExplicitFalse: only an explicit
// "false" disables it. Case-insensitive — covers `false`, `False`,
// `FALSE`, `fAlSe` because operators get inventive with caps.
func TestIsAutoTieBreakerEnabledExplicitFalse(t *testing.T) {
	t.Parallel()

	for _, val := range []string{"false", "False", "FALSE", "fAlSe"} {
		rd := &blockstoriov1alpha1.ResourceDefinition{
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				Props: map[string]string{
					"DrbdOptions/AutoAddQuorumTiebreaker": val,
				},
			},
		}

		if controllerpkg.IsAutoTieBreakerEnabled(rd) {
			t.Errorf("value %q must disable auto-tiebreaker", val)
		}
	}
}

// TestIsAutoTieBreakerEnabledNonFalseValuesEnable: any value that
// isn't case-insensitive "false" leaves auto-tiebreaker on. Pins
// the fail-safe semantic — typos / unrelated values must NOT
// silently disable witness creation, because that would let a
// 2-replica RD lose quorum on a single failure.
func TestIsAutoTieBreakerEnabledNonFalseValuesEnable(t *testing.T) {
	t.Parallel()

	for _, val := range []string{"true", "True", "yes", "1", "", "maybe"} {
		rd := &blockstoriov1alpha1.ResourceDefinition{
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				Props: map[string]string{
					"DrbdOptions/AutoAddQuorumTiebreaker": val,
				},
			},
		}

		if !controllerpkg.IsAutoTieBreakerEnabled(rd) {
			t.Errorf("value %q must NOT disable auto-tiebreaker (only `false` does)", val)
		}
	}
}
