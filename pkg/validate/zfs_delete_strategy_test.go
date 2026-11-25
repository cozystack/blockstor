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

package validate_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/validate"
)

// The first scope that sets the property wins, even with a value that only
// parses to the default, as LINSTOR's PriorityProps lookup does.
func TestEffectiveZFSDeleteStrategyPriority(t *testing.T) {
	t.Parallel()

	key := validate.ZFSDeleteStrategyProp

	cases := []struct {
		name   string
		scopes []map[string]string
		want   validate.ZFSDeleteStrategy
	}{
		{"unset", nil, validate.ZFSDeleteStrategyDynamic},
		{"rd over rg", []map[string]string{{key: "rename"}, {key: "DELETE"}}, validate.ZFSDeleteStrategyRename},
		{"rg when rd unset", []map[string]string{nil, {key: "delete"}}, validate.ZFSDeleteStrategyDelete},
		{"controller last", []map[string]string{{}, {}, {key: "DELETE"}}, validate.ZFSDeleteStrategyDelete},
		{"bad rd value shadows rg", []map[string]string{{key: "x"}, {key: "DELETE"}}, validate.ZFSDeleteStrategyDynamic},
	}

	for _, tc := range cases {
		if got := validate.EffectiveZFSDeleteStrategy(tc.scopes...); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}

	if !validate.ZFSDeleteRefusal(validate.ZFSDeleteStrategyDelete, true) ||
		validate.ZFSDeleteRefusal(validate.ZFSDeleteStrategyDelete, false) ||
		validate.ZFSDeleteRefusal(validate.ZFSDeleteStrategyRename, true) ||
		validate.ZFSDeleteRefusal(validate.ZFSDeleteStrategyDynamic, true) {
		t.Errorf("ZFSDeleteRefusal: only DELETE with snapshots refuses")
	}
}
