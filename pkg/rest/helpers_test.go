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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// testRESTNamespace is the namespace REST-test helpers pin the
// server to. Pre-Phase-10.6 the cluster passphrase + controller
// props lived in a KVEntry CRD with no namespace; today both
// route through namespace-scoped objects (Secret + cluster-scoped
// ControllerConfig), and the tests pick a stable name so a
// missing namespace doesn't surface as a misleading 500.
const testRESTNamespace = "blockstor-system"

// newFakeRESTClient builds a controller-runtime fake client with
// the project's CRDs + core/v1 in scheme. Used by every REST
// test that needs the Server's `Client` field to be non-nil
// (encryption, controller-properties, physical-storage cascade
// ownership). No seed objects — tests Create what they need.
func newFakeRESTClient(t *testing.T) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 to scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor to scheme: %v", err)
	}

	return fake.NewClientBuilder().WithScheme(scheme).Build()
}
