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

package k8s

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestCrdToWireResourceStampsCreateTimestamp pins the parity fix:
// upstream LINSTOR populates the `create_timestamp` wire field (the
// Python CLI's `CreatedOn` column, in unix milliseconds). blockstor
// sources it, persistence-free, from the backing Resource CRD's
// metadata.creationTimestamp — per replica.
func TestCrdToWireResourceStampsCreateTimestamp(t *testing.T) {
	t.Parallel()

	// A fixed instant; metav1.Time has second granularity on the wire,
	// so truncate to seconds to make the millisecond expectation exact.
	created := metav1.NewTime(time.Unix(1_780_000_000, 0).UTC())
	crd := &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "orc-tb.dev-worker-1",
			CreationTimestamp: created,
		},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "orc-tb",
			NodeName:               "dev-worker-1",
		},
	}

	wire := crdToWireResource(crd)
	want := created.UnixMilli()
	if wire.CreateTimestamp != want {
		t.Fatalf("CreateTimestamp: got %d, want %d (creationTimestamp in unix ms)",
			wire.CreateTimestamp, want)
	}
}

// TestCrdToWireResourceZeroCreateTimestamp: an unset creationTimestamp
// (e.g. a hand-built CRD) maps to 0 so `omitempty` drops the field
// rather than emitting a 1970 epoch CreatedOn.
func TestCrdToWireResourceZeroCreateTimestamp(t *testing.T) {
	t.Parallel()

	crd := &crdv1alpha1.Resource{
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "no-ts",
			NodeName:               "dev-worker-1",
		},
	}

	if got := crdToWireResource(crd).CreateTimestamp; got != 0 {
		t.Fatalf("CreateTimestamp on a zero-creationTimestamp CRD: got %d, want 0", got)
	}
}
