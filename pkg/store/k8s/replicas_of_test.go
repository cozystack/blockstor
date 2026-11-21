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
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// unscopedListCounter counts the Resource lists sent without a field
// selector: reads of every replica in the cluster.
type unscopedListCounter struct {
	client.Reader

	unscoped atomic.Int64
}

func (c *unscopedListCounter) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var o client.ListOptions
	o.ApplyOptions(opts)

	if _, ok := list.(*crdv1alpha1.ResourceList); ok && o.FieldSelector == nil {
		c.unscoped.Add(1)
	}

	return c.Reader.List(ctx, list, opts...) //nolint:wrapcheck // test double
}

func replicasOfScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

func replicaRow(name, rd, node string) *crdv1alpha1.Resource {
	return &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: node},
	}
}

// replicasOfCluster is a cluster with many definitions, one of them
// pvc-mix with a replica written in another spelling of its name.
func replicasOfCluster(t *testing.T, indexed bool) client.Client {
	t.Helper()

	objs := make([]client.Object, 0, 53)
	objs = append(objs,
		&crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"}},
		replicaRow("pvc-mix.n1", "pvc-mix", "n1"),
		replicaRow("pvc-mix.n2", "Pvc-Mix", "N2"),
	)

	for i := range 50 {
		objs = append(objs, replicaRow(fmt.Sprintf("pvc-%02d.n1", i), fmt.Sprintf("pvc-%02d", i), "n1"))
	}

	builder := fake.NewClientBuilder().WithScheme(replicasOfScheme(t)).WithObjects(objs...)
	if indexed {
		builder = builder.WithIndex(&crdv1alpha1.Resource{}, k8s.FieldResourceDefinitionName, func(o client.Object) []string {
			return []string{o.(*crdv1alpha1.Resource).Spec.ResourceDefinitionName}
		})
	}

	return builder.Build()
}

func replicaNames(replicas []crdv1alpha1.Resource) []string {
	out := make([]string, 0, len(replicas))
	for i := range replicas {
		out = append(out, replicas[i].Name)
	}

	slices.Sort(out)

	return out
}

// The controller and every satellite read a definition's replicas past
// the cache on every pass. They must read that definition's replicas
// only, never every Resource in the cluster: on a cluster with a few
// hundred replicas that read kept them at their CPU limit. Every
// spelling of the definition a replica carries is still found.
func TestReplicasOfReadsOnlyTheDefinitionsReplicas(t *testing.T) {
	t.Parallel()

	cli := replicasOfCluster(t, true)
	reader := &unscopedListCounter{Reader: cli}

	var got crdv1alpha1.ResourceList

	err := k8s.ReplicasOf(context.Background(), reader, cli, "pvc-mix", &got)
	if err != nil {
		t.Fatalf("ReplicasOf: %v", err)
	}

	if want := []string{"pvc-mix.n1", "pvc-mix.n2"}; !slices.Equal(replicaNames(got.Items), want) {
		t.Errorf("replicas of pvc-mix = %v, want %v", replicaNames(got.Items), want)
	}

	if n := reader.unscoped.Load(); n != 0 {
		t.Errorf("read every Resource in the cluster %d time(s) for one definition", n)
	}
}

// A reader that cannot select on the field gets the whole-cluster read,
// and the same answer.
func TestReplicasOfFallsBackWhenTheSelectorIsRefused(t *testing.T) {
	t.Parallel()

	cli := replicasOfCluster(t, false)

	var got crdv1alpha1.ResourceList

	err := k8s.ReplicasOf(context.Background(), cli, nil, "pvc-mix", &got)
	if err != nil {
		t.Fatalf("ReplicasOf: %v", err)
	}

	if want := []string{"pvc-mix.n1", "pvc-mix.n2"}; !slices.Equal(replicaNames(got.Items), want) {
		t.Errorf("replicas of pvc-mix = %v, want %v", replicaNames(got.Items), want)
	}
}

// requestCounter counts every request a reader sends, and how many of the
// Resource lists among them carried no field selector.
type requestCounter struct {
	client.Reader

	gets, lists, unscoped atomic.Int64
	refuseSelector        bool
}

func (c *requestCounter) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.gets.Add(1)

	return c.Reader.Get(ctx, key, obj, opts...) //nolint:wrapcheck // test double
}

func (c *requestCounter) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.lists.Add(1)

	var o client.ListOptions
	o.ApplyOptions(opts)

	if _, ok := list.(*crdv1alpha1.ResourceList); ok && o.FieldSelector == nil {
		c.unscoped.Add(1)
	}

	if c.refuseSelector && o.FieldSelector != nil {
		// What the API server answers for a CRD that predates the
		// selectable field.
		return apierrors.NewBadRequest(`Unable to find "blockstor.org/v1alpha1, Resource=resources" that match ` +
			`label selector "", field selector "spec.resourceDefinitionName=pvc-mix": ` +
			`field label not supported: spec.resourceDefinitionName`)
	}

	return c.Reader.List(ctx, list, opts...) //nolint:wrapcheck // test double
}

// keyIndexedCluster is replicasOfCluster with the cache's definition-key
// index registered as well, as a manager's cache has it.
func keyIndexedCluster(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	return fake.NewClientBuilder().WithScheme(replicasOfScheme(t)).WithObjects(objs...).
		WithIndex(&crdv1alpha1.Resource{}, k8s.FieldResourceDefinitionName, func(o client.Object) []string {
			return []string{o.(*crdv1alpha1.Resource).Spec.ResourceDefinitionName}
		}).
		WithIndex(&crdv1alpha1.Resource{}, k8s.FieldResourceDefinitionKey, k8s.ReplicaKeyIndexer).
		Build()
}

// The spellings the cached replicas carry are looked up on every pass of
// every definition. Through the cache's definition-key index that touches
// the definition's replicas only; a walk of the whole cache copies every
// Resource in it, once per pass.
func TestReplicasOfLooksUpCachedSpellingsThroughTheIndex(t *testing.T) {
	t.Parallel()

	objs := []client.Object{
		&crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"}},
		replicaRow("pvc-mix.n1", "pvc-mix", "n1"),
		replicaRow("pvc-mix.n2", "Pvc-Mix", "N2"),
	}
	for i := range 20 {
		objs = append(objs, replicaRow(fmt.Sprintf("pvc-%02d.n1", i), fmt.Sprintf("pvc-%02d", i), "n1"))
	}

	cli := keyIndexedCluster(t, objs...)
	cache := &requestCounter{Reader: cli}

	var got crdv1alpha1.ResourceList

	err := k8s.ReplicasOf(context.Background(), cli, cache, "pvc-mix", &got)
	if err != nil {
		t.Fatalf("ReplicasOf: %v", err)
	}

	if want := []string{"pvc-mix.n1", "pvc-mix.n2"}; !slices.Equal(replicaNames(got.Items), want) {
		t.Errorf("replicas of pvc-mix = %v, want %v", replicaNames(got.Items), want)
	}

	if n := cache.unscoped.Load(); n != 0 {
		t.Errorf("walked every cached Resource %d time(s) to find the spellings of one definition", n)
	}
}

// The definition's own spellings are fixed when it is created, so the
// cache names them as well as the API server does. Asking the server on
// every pass of every definition is a request each time for nothing.
func TestReplicasOfTakesACachedDefinitionsSpellingsFromTheCache(t *testing.T) {
	t.Parallel()

	cli := keyIndexedCluster(t,
		&crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"}},
		replicaRow("pvc-mix.n1", "pvc-mix", "n1"),
	)
	reader := &requestCounter{Reader: cli}

	var got crdv1alpha1.ResourceList

	err := k8s.ReplicasOf(context.Background(), reader, cli, "pvc-mix", &got)
	if err != nil {
		t.Fatalf("ReplicasOf: %v", err)
	}

	if n := reader.gets.Load(); n != 0 {
		t.Errorf("asked the API server for the definition %d time(s) although the cache holds it", n)
	}

	if want := []string{"pvc-mix.n1"}; !slices.Equal(replicaNames(got.Items), want) {
		t.Errorf("replicas of pvc-mix = %v, want %v", replicaNames(got.Items), want)
	}
}

// A server whose CRD predates the selectable field refuses the selector.
// Once it has, every later read goes straight to the whole-cluster list,
// one request as before the scoped read existed, instead of asking for the
// definition and the refused list first each time.
//
// Not parallel: the refusal is remembered for the process.
//
//nolint:paralleltest // process-wide state
func TestReplicasOfStopsAskingAServerThatRefusedTheField(t *testing.T) {
	t.Cleanup(k8s.ForgetSelectorRefusal)

	cli := replicasOfCluster(t, true)
	reader := &requestCounter{Reader: cli, refuseSelector: true}

	for pass := range 2 {
		reader.gets.Store(0)
		reader.lists.Store(0)

		var got crdv1alpha1.ResourceList

		err := k8s.ReplicasOf(context.Background(), reader, nil, "pvc-mix", &got)
		if err != nil {
			t.Fatalf("pass %d: ReplicasOf: %v", pass, err)
		}

		if want := []string{"pvc-mix.n1", "pvc-mix.n2"}; !slices.Equal(replicaNames(got.Items), want) {
			t.Errorf("pass %d: replicas of pvc-mix = %v, want %v", pass, replicaNames(got.Items), want)
		}
	}

	if n := reader.gets.Load() + reader.lists.Load(); n != 1 {
		t.Errorf("a read after the refusal sent %d requests, want the one whole-cluster list", n)
	}
}
