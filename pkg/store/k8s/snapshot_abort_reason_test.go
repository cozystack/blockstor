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

package k8s_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// The controller's record of an aborted take has no wire counterpart. A
// REST modify of the snapshot rebuilds the spec from the wire and must
// keep it, or the next orchestration pass takes the snapshot for one
// that never started and freezes I/O again.
func TestSnapshotUpdateKeepsTheAbortRecord(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	const reason = "snapshot aborted: test"

	crd := &crdv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: crdv1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
			AbortReason:            reason,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(crd).Build()
	st := k8s.New(cli)
	ctx := context.Background()

	snap, err := st.Snapshots().Get(ctx, "pvc-1", "snap-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	snap.Props = map[string]string{"Aux/foo": "bar"}

	if err := st.Snapshots().Update(ctx, &snap); err != nil {
		t.Fatalf("update: %v", err)
	}

	var got crdv1alpha1.Snapshot
	if err := cli.Get(ctx, ctrlclient.ObjectKey{Name: crd.Name}, &got); err != nil {
		t.Fatalf("re-read: %v", err)
	}

	if got.Spec.AbortReason != reason {
		t.Errorf("abort record after a REST modify = %q, want %q", got.Spec.AbortReason, reason)
	}
}
