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

package satellite

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite/intent"
)

// The .res file names a peer as it was rendered, Node-2; the intent now
// spells it node-2. The peer has not left, so it is not dropped.
func TestARenamedPeerSpellingIsNotARemovedPeer(t *testing.T) {
	t.Parallel()

	res := filepath.Join(t.TempDir(), "pvc.res")
	body := "resource \"pvc\" {\n  on Node-1 {\n  }\n  on Node-2 {\n  }\n  on node-3 {\n  }\n}\n"

	if err := os.WriteFile(res, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	dr := &intent.DesiredResource{Name: "pvc", Peers: []intent.DesiredPeer{{Name: "node-2"}}}

	got := computeRemovedPeers(res, dr, "node-1")
	if len(got) != 1 || got[0] != "node-3" {
		t.Errorf("removed peers %v; want only node-3", got)
	}
}
