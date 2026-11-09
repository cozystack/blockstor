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

package cli

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The wire spells a replica's node as it was registered, Node-1, while the
// operator names the node node-1. The pool a restore or a create infers is
// the one of that node's own replica, or of a sibling, never the other way.
func TestPoolInferenceReadsTheNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	run := &runContext{Store: st, Flags: &flagSet{Values: map[string]string{}}}

	for _, node := range []string{"Node-1", "Node-2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "pvc-mix",
			NodeName: node,
			Props:    map[string]string{storPoolNameProp: "pool-" + node},
		}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}
	}

	if pool, err := sourcePoolOn(ctx, run, "pvc-mix", "node-2"); err != nil || pool != "pool-Node-2" {
		t.Errorf("sourcePoolOn(node-2) = %q, %v; want the pool of the replica on Node-2", pool, err)
	}

	if pool, err := resolveStorPool(ctx, run, "pvc-mix", "node-1"); err != nil || pool != "pool-Node-2" {
		t.Errorf("resolveStorPool(node-1) = %q, %v; want the sibling's pool, not node-1's own", pool, err)
	}
}
