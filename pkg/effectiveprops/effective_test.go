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

package effectiveprops_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

// TestLegacyControllerPropsFiltersByInstance: only KVEntry rows
// with Instance="ControllerProps" surface. The KV store is
// shared across instances (csi-volumes for piraeus-csi metadata,
// snapshot-policies, ControllerProps for cluster DRBD options) —
// without the filter the resolver would feed CSI volume
// metadata into the prop bag, polluting the .res file.
func TestLegacyControllerPropsFiltersByInstance(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	objs := []client.Object{
		&blockstoriov1alpha1.KVEntry{
			ObjectMeta: metav1.ObjectMeta{Name: "controllerprops-quorum"},
			Spec: blockstoriov1alpha1.KVEntrySpec{
				Instance: "ControllerProps",
				Key:      "DrbdOptions/Resource/quorum",
				Value:    "majority",
			},
		},
		&blockstoriov1alpha1.KVEntry{
			ObjectMeta: metav1.ObjectMeta{Name: "controllerprops-net"},
			Spec: blockstoriov1alpha1.KVEntrySpec{
				Instance: "ControllerProps",
				Key:      "DrbdOptions/Net/protocol",
				Value:    "C",
			},
		},
		// csi-volumes data MUST NOT surface.
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

	got, err := effectiveprops.LegacyControllerProps(context.Background(), cli)
	if err != nil {
		t.Fatalf("LegacyControllerProps: %v", err)
	}

	if got["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop missing or wrong: %+v", got)
	}

	if got["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("protocol prop missing or wrong: %+v", got)
	}

	if _, leaked := got["vol1/sizeBytes"]; leaked {
		t.Errorf("csi-volumes data leaked into ControllerProps bag: %+v", got)
	}
}

// TestLegacyControllerPropsEmpty: fresh cluster (no KVEntry rows)
// → empty (non-nil) map, no error. Downstream resolver treats
// empty as "no cluster-level overrides" and falls through.
func TestLegacyControllerPropsEmpty(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	got, err := effectiveprops.LegacyControllerProps(context.Background(), cli)
	if err != nil {
		t.Fatalf("LegacyControllerProps: %v", err)
	}

	if got == nil {
		t.Errorf("got nil, want empty non-nil map")
	}

	if len(got) != 0 {
		t.Errorf("len: got %d, want 0", len(got))
	}
}
