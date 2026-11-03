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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// The satellite labels its devices with its node's CR name, node-1; `ps cdp`
// names the node as registered, Node-1, and still lists them.
func TestDevicesAreListedUnderAnotherSpellingOfTheirNode(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	dev := &crdv1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1-sdb",
			Labels: map[string]string{crdv1alpha1.PhysicalDeviceLabelNode: "node-1"},
		},
	}

	st := k8s.New(fake.NewClientBuilder().WithScheme(scheme).WithObjects(dev).Build())

	for _, asked := range []string{"node-1", "Node-1", "NODE-1"} {
		got, err := st.PhysicalDevices().ListForNode(t.Context(), asked)
		if err != nil || len(got) != 1 {
			t.Errorf("ListForNode(%s) = %d devices, %v; want the device labelled node-1", asked, len(got), err)
		}
	}
}
