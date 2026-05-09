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
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestControllerPropsFiltersByInstance: only KVEntry rows with
// Instance="ControllerProps" surface in the result. The KV store
// is shared across instances (csi-volumes for piraeus-csi metadata,
// snapshot-policies, ControllerProps for cluster DRBD options) —
// without the filter the dispatcher would feed CSI volume metadata
// into the satellite's prop bag, polluting the .res file.
func TestControllerPropsFiltersByInstance(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	objs := []client.Object{
		// In-instance: must surface.
		&blockstoriov1alpha1.KVEntry{
			ObjectMeta: metav1.ObjectMeta{Name: "controllerprops-quorum"},
			Spec: blockstoriov1alpha1.KVEntrySpec{
				Instance: "ControllerProps",
				Key:      "DrbdOptions/Resource/quorum",
				Value:    "majority",
			},
		},
		// In-instance: must surface.
		&blockstoriov1alpha1.KVEntry{
			ObjectMeta: metav1.ObjectMeta{Name: "controllerprops-net"},
			Spec: blockstoriov1alpha1.KVEntrySpec{
				Instance: "ControllerProps",
				Key:      "DrbdOptions/Net/protocol",
				Value:    "C",
			},
		},
		// Other instance (csi-volumes): MUST NOT surface in the
		// controller props bag — it's CSI driver metadata, not DRBD
		// options.
		&blockstoriov1alpha1.KVEntry{
			ObjectMeta: metav1.ObjectMeta{Name: "csi-volumes-vol1"},
			Spec: blockstoriov1alpha1.KVEntrySpec{
				Instance: "csi-volumes",
				Key:      "vol1/sizeBytes",
				Value:    "1073741824",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	got, err := rec.ControllerProps(context.Background())
	if err != nil {
		t.Fatalf("ControllerProps: %v", err)
	}

	if got["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop missing or wrong: %+v", got)
	}

	if got["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("protocol prop missing or wrong: %+v", got)
	}

	// Critical: csi-volumes data must NOT leak into the controller
	// prop bag.
	if _, leaked := got["vol1/sizeBytes"]; leaked {
		t.Errorf("csi-volumes data leaked into ControllerProps bag: %+v", got)
	}
}

// TestControllerPropsEmpty: fresh cluster (no KVEntry rows) → empty
// (non-nil) map, no error. The merger downstream treats empty as
// "no cluster-level overrides" and falls through to RG/RD/Resource.
func TestControllerPropsEmpty(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	got, err := rec.ControllerProps(context.Background())
	if err != nil {
		t.Fatalf("ControllerProps: %v", err)
	}

	if got == nil {
		t.Errorf("got nil, want empty non-nil map")
	}

	if len(got) != 0 {
		t.Errorf("len: got %d, want 0", len(got))
	}
}
