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

package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A restore retried after its rollback, while a node holding a replica of the
// rolled-back definition is down, is refused the way the plain create is: 409,
// FAIL_EXISTS_RSC_DFN, and a message naming the node. The adopt path read the
// refusal as a leftover definition, found none, and answered 404.
func TestRestoreIntoANameStillBeingDeletedAnswers409(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-drain")
	seedRestoreSnapshot(t, st, "src-drain", "snap-drain", []string{"node-a"})

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-drain", NodeName: "node-down", Flags: []string{apiv1.ResourceFlagDelete},
	}); err != nil {
		t.Fatalf("seed the draining replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	raw, err := json.Marshal(map[string]any{"to_resource": "dst-drain", "node_names": []string{"node-a"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/src-drain/snapshot-restore-resource/snap-drain", raw)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("restore over a draining replica: status %d, want 409", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal: %v (%d entries)", err, len(rcs))
	}

	if rcs[0].RetCode&apiCallRcFailExistsRscDfn != apiCallRcFailExistsRscDfn {
		t.Errorf("ret_code %#x lacks FAIL_EXISTS_RSC_DFN", rcs[0].RetCode)
	}

	if !strings.Contains(rcs[0].Message, "node-down") {
		t.Errorf("message %q does not name the draining node", rcs[0].Message)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-drain"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the refusal left a definition behind: err=%v", err)
	}
}
