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

package controllers_test

import (
	"testing"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/satellite/stream"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A satellite with no stream TLS configured cannot fetch from a peer. The
// fetcher has to answer storage.ErrNotFound then, so the restore falls back
// to a blank volume and a DRBD resync. Any other error requeues the
// restore, and with the stream off it never succeeds.
func TestSnapshotFetcherWithTheStreamOffFallsBackToAResync(t *testing.T) {
	t.Parallel()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("rd.snap")},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "rd",
			SnapshotName:           "snap",
			Nodes:                  []string{"self", "peer"},
		},
	}
	peer := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "peer"},
		Spec: blockstoriov1alpha1.NodeSpec{
			NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{{Name: "default", Address: "127.0.0.1"}},
		},
	}

	fetcher := &controllers.SnapshotFetcher{
		Client:   fake.NewClientBuilder().WithScheme(newSnapshotScheme(t)).WithObjects(snap, peer).Build(),
		NodeName: "self",
		Stream:   stream.NewClient(stream.TLSFiles{}),
	}

	body, _, err := fetcher.Fetch(t.Context(), "rd", "snap", 0)
	if body != nil {
		_ = body.Close()
	}

	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("fetch with the stream off = %v, want storage.ErrNotFound", err)
	}
}
