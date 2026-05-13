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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// These tests cover Bug 55 fix. blockstor's StoragePool CRDs may
// carry an operator-chosen metadata.name (e.g. piraeus's
// `zfs-thin-w3` produced via `kubectl apply -f`) that does NOT
// follow blockstor's canonical `<node>.<pool>` shape. Get/Delete
// must resolve the underlying CRD by Spec.NodeName + Spec.PoolName,
// not by reconstructing the canonical name. Without this fix,
// `linstor sp d <node> <pool>` reported "already absent" while
// List() kept showing the pool — the store's by-key path was
// issuing a Delete against a CRD name that did not exist.

func bug55NewFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&crdv1alpha1.StoragePool{}).
		Build()
}

// operatorChosenPool builds a StoragePool CRD whose metadata.name
// does NOT match blockstor's canonical "<node>.<pool>" shape — the
// situation piraeus produces when its operator names CRDs after the
// pool's role rather than the (node, pool) tuple.
func operatorChosenPool() *crdv1alpha1.StoragePool {
	return &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zfs-thin-w3",
			// Deliberately no LabelNodeName/LabelPoolName: the
			// operator's `kubectl apply -f` doesn't know about
			// blockstor's internal label scheme.
		},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName:     "worker-3",
			PoolName:     "zfs-thin",
			ProviderKind: "ZFS_THIN",
		},
	}
}

// TestStoragePoolDeleteByOperatorChosenCRDName reproduces the
// user-reported bug: a CRD created with metadata.name="zfs-thin-w3"
// + Spec.NodeName="worker-3" + Spec.PoolName="zfs-thin" must be
// deletable via Delete(ctx, "worker-3", "zfs-thin"). Before Bug 55
// fix, this returned ErrNotFound and the CRD survived.
func TestStoragePoolDeleteByOperatorChosenCRDName(t *testing.T) {
	t.Parallel()

	pool := operatorChosenPool()
	cli := bug55NewFakeClient(t, pool)
	s := k8s.New(cli)

	err := s.StoragePools().Delete(t.Context(), "worker-3", "zfs-thin")
	if err != nil {
		t.Fatalf("Delete: got %v, want nil", err)
	}

	// Verify CRD is actually gone from the apiserver. We look it
	// up by the operator-chosen name — if Delete had used the
	// canonical "worker-3.zfs-thin" shape, this Get would still
	// succeed (the bug).
	var got crdv1alpha1.StoragePool

	err = cli.Get(t.Context(), types.NamespacedName{Name: "zfs-thin-w3"}, &got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("CRD still present after Delete: err=%v (expected IsNotFound)", err)
	}
}

// TestStoragePoolGetByOperatorChosenCRDName mirrors the Delete
// test for the Get path: the same Spec-based resolution must work
// for /v1/nodes/{node}/storage-pools/{pool}.
func TestStoragePoolGetByOperatorChosenCRDName(t *testing.T) {
	t.Parallel()

	pool := operatorChosenPool()
	cli := bug55NewFakeClient(t, pool)
	s := k8s.New(cli)

	got, err := s.StoragePools().Get(t.Context(), "worker-3", "zfs-thin")
	if err != nil {
		t.Fatalf("Get: got %v, want nil", err)
	}

	if got.NodeName != "worker-3" {
		t.Errorf("NodeName: got %q, want worker-3", got.NodeName)
	}

	if got.StoragePoolName != "zfs-thin" {
		t.Errorf("StoragePoolName: got %q, want zfs-thin", got.StoragePoolName)
	}

	if got.ProviderKind != "ZFS_THIN" {
		t.Errorf("ProviderKind: got %q, want ZFS_THIN", got.ProviderKind)
	}
}

// TestStoragePoolDeleteReturnsNotFoundWhenNoMatch: Delete on a
// (node, pool) that no CRD claims must surface ErrNotFound — the
// REST DELETE handler folds that into a 200 idempotent envelope.
// A different error shape would break linstor-client's idempotent
// retry assumption.
func TestStoragePoolDeleteReturnsNotFoundWhenNoMatch(t *testing.T) {
	t.Parallel()

	cli := bug55NewFakeClient(t)
	s := k8s.New(cli)

	err := s.StoragePools().Delete(t.Context(), "ghost-node", "ghost-pool")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Delete ghost: got %v, want ErrNotFound", err)
	}
}

// TestStoragePoolDeleteMultiMatchSurfacesError: data-integrity
// violation — two CRDs both claim Spec.NodeName=N + Spec.PoolName=P.
// The store must refuse to auto-pick a winner (deleting the wrong
// one would silently corrupt registry state); it surfaces an error
// naming all colliding CRD names so the operator can resolve.
func TestStoragePoolDeleteMultiMatchSurfacesError(t *testing.T) {
	t.Parallel()

	first := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "zfs-thin-w3"},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName: "worker-3", PoolName: "zfs-thin", ProviderKind: "ZFS_THIN",
		},
	}
	second := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "another-name-for-same-pool"},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName: "worker-3", PoolName: "zfs-thin", ProviderKind: "ZFS_THIN",
		},
	}

	cli := bug55NewFakeClient(t, first, second)
	s := k8s.New(cli)

	err := s.StoragePools().Delete(t.Context(), "worker-3", "zfs-thin")
	if err == nil {
		t.Fatalf("Delete multi-match: got nil, want error")
	}

	// Must NOT be ErrNotFound — the REST handler would otherwise
	// silently fold this into a 200 success while two duplicate
	// CRDs remain in the cluster.
	if errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Delete multi-match: got ErrNotFound, want data-integrity error")
	}

	msg := err.Error()
	if !strings.Contains(msg, "zfs-thin-w3") || !strings.Contains(msg, "another-name-for-same-pool") {
		t.Errorf("error message must name both colliding CRDs; got: %q", msg)
	}

	// And we did NOT delete either CRD (the operator should pick).
	var first1, second1 crdv1alpha1.StoragePool
	if err := cli.Get(t.Context(), types.NamespacedName{Name: "zfs-thin-w3"}, &first1); err != nil {
		t.Errorf("first CRD must still exist after multi-match refusal: %v", err)
	}

	if err := cli.Get(t.Context(), types.NamespacedName{Name: "another-name-for-same-pool"}, &second1); err != nil {
		t.Errorf("second CRD must still exist after multi-match refusal: %v", err)
	}
}

// TestStoragePoolDeleteByCanonicalNameStillWorks: regression guard
// for blockstor's own Create path. Pools created via REST land with
// metadata.name = crdName(node, pool); the resolver's fast-path
// must keep them deletable without falling through to the full
// List scan.
func TestStoragePoolDeleteByCanonicalNameStillWorks(t *testing.T) {
	t.Parallel()

	canonical := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1.thin", // matches crdName("n1", "thin")
		},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName: "n1", PoolName: "thin", ProviderKind: "LVM_THIN",
		},
	}

	cli := bug55NewFakeClient(t, canonical)
	s := k8s.New(cli)

	if err := s.StoragePools().Delete(t.Context(), "n1", "thin"); err != nil {
		t.Fatalf("Delete canonical: got %v, want nil", err)
	}

	var got crdv1alpha1.StoragePool

	err := cli.Get(t.Context(), types.NamespacedName{Name: "n1.thin"}, &got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("canonical CRD still present after Delete: err=%v", err)
	}
}

// TestStoragePoolGetByOperatorChosenCRDName_NoCollideOnCanonical:
// the resolver's fast-path Get tries the canonical name first. If
// an UNRELATED CRD happens to occupy that canonical name (e.g.
// blockstor created `worker-3.zfs-thin` for a now-Spec-renamed
// pool, and a fresh operator CRD with Spec.NodeName=worker-3 +
// Spec.PoolName=zfs-thin lives under "zfs-thin-w3"), the resolver
// must NOT return the fast-path miss as the answer. It must verify
// the fast-path CRD's Spec actually matches.
func TestStoragePoolGetVerifiesSpecOnFastPath(t *testing.T) {
	t.Parallel()

	// Stale CRD parked at the canonical name with a DIFFERENT Spec
	// (someone manually edited Spec.PoolName but didn't rename).
	stale := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-3.zfs-thin"},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName: "worker-3", PoolName: "renamed-pool", ProviderKind: "ZFS_THIN",
		},
	}
	// The real (worker-3, zfs-thin) lives at an operator-chosen name.
	real0 := operatorChosenPool()

	cli := bug55NewFakeClient(t, stale, real0)
	s := k8s.New(cli)

	got, err := s.StoragePools().Get(t.Context(), "worker-3", "zfs-thin")
	if err != nil {
		t.Fatalf("Get: got %v, want nil", err)
	}

	if got.StoragePoolName != "zfs-thin" {
		t.Errorf("StoragePoolName: got %q, want zfs-thin (resolver picked the wrong CRD)", got.StoragePoolName)
	}
}
