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

package controllers

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// TestObserverRecordsThePromotionScore: the kernel's promotion score
// and may_promote flag reach Resource.Status from the events2 resource
// frame and survive the connection frames that follow it. linstor-csi
// reports a volume abnormal on every replica whose score is 0, so a
// score that never reaches Status marks every volume abnormal.
func TestObserverRecordsThePromotionScore(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	const (
		rdName   = "pvc-score"
		nodeName = "node-a"
	)

	crName := k8s.ResourceCRName(rdName, nodeName)
	replica := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: crName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeName,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: rdName}},
			&blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}, replica).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	o := &ObserverRunnable{Client: cli, NodeName: nodeName}
	ctx := context.Background()

	for _, line := range []string{
		"exists resource name:pvc-score role:Secondary suspended:no force-io-failures:no may_promote:no promotion_score:10102",
		"exists connection name:pvc-score peer-node-id:1 conn-name:node-b connection:Connected role:Secondary",
	} {
		ev, err := drbd.ParseEvent(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}

		obs, ok := translateEvent(ev)
		if !ok {
			t.Fatalf("translateEvent rejected %q", line)
		}

		o.mergeResource(&obs)

		if err := o.writeStatus(ctx, &obs); err != nil {
			t.Fatalf("writeStatus after %q: %v", line, err)
		}
	}

	var got blockstoriov1alpha1.Resource
	if err := cli.Get(ctx, client.ObjectKey{Name: crName}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Status.PromotionScore == nil || *got.Status.PromotionScore != 10102 {
		t.Errorf("Status.PromotionScore = %v, want 10102", got.Status.PromotionScore)
	}

	if got.Status.MayPromote == nil || *got.Status.MayPromote {
		t.Errorf("Status.MayPromote = %v, want false", got.Status.MayPromote)
	}
}

// TestObserverPromotionTokens: an events2 frame that does not carry
// the tokens, or carries one the kernel never writes, leaves the
// cached value alone instead of recording a score of 0.
func TestObserverPromotionTokens(t *testing.T) {
	t.Parallel()

	if got := parsePromotionScore("0"); got == nil || *got != 0 {
		t.Errorf("parsePromotionScore(0) = %v, want 0", got)
	}

	for _, raw := range []string{"", "x", "99999999999"} {
		if got := parsePromotionScore(raw); got != nil {
			t.Errorf("parsePromotionScore(%q) = %d, want nil", raw, *got)
		}
	}

	if got := parseMayPromote("yes"); got == nil || !*got {
		t.Errorf("parseMayPromote(yes) = %v, want true", got)
	}

	if got := parseMayPromote(""); got != nil {
		t.Errorf("parseMayPromote(\"\") = %v, want nil", *got)
	}

	o := &ObserverRunnable{}

	first := observation{
		ResourceName:   "pvc-tok",
		HasResource:    true,
		Role:           "Secondary",
		PromotionScore: parsePromotionScore("10102"),
		MayPromote:     parseMayPromote("yes"),
	}
	o.mergeResource(&first)

	again := observation{ResourceName: "pvc-tok", HasResource: true, Role: "Secondary"}
	o.mergeResource(&again)

	if again.PromotionScore == nil || *again.PromotionScore != 10102 || again.MayPromote == nil || !*again.MayPromote {
		t.Errorf("frame without the tokens: score %v may_promote %v, want the cached 10102 / true", again.PromotionScore, again.MayPromote)
	}
}

// TestObserverClearsPromotionOnDestroy: a `destroy resource` frame
// carries no promotion tokens, yet the slot that owned the score is
// gone. Both fields must leave Resource.Status, as LINSTOR drops them
// on stream close; a cached score would have the view call a
// torn-down replica promotable and hide it from linstor-csi's health
// check.
func TestObserverClearsPromotionOnDestroy(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	const (
		rdName   = "pvc-gone"
		nodeName = "node-a"
	)

	crName := k8s.ResourceCRName(rdName, nodeName)
	replica := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: crName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeName,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: rdName}},
			&blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}, replica).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	o := &ObserverRunnable{Client: cli, NodeName: nodeName}
	ctx := context.Background()

	for _, line := range []string{
		"exists resource name:pvc-gone role:Secondary suspended:no force-io-failures:no may_promote:yes promotion_score:10102",
		"destroy resource name:pvc-gone",
	} {
		ev, err := drbd.ParseEvent(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}

		obs, ok := translateEvent(ev)
		if !ok {
			t.Fatalf("translateEvent rejected %q", line)
		}

		o.mergeResource(&obs)

		if err := o.writeStatus(ctx, &obs); err != nil {
			t.Fatalf("writeStatus after %q: %v", line, err)
		}
	}

	var got blockstoriov1alpha1.Resource
	if err := cli.Get(ctx, client.ObjectKey{Name: crName}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Status.PromotionScore != nil {
		t.Errorf("Status.PromotionScore = %d after destroy, want unset", *got.Status.PromotionScore)
	}

	if got.Status.MayPromote != nil {
		t.Errorf("Status.MayPromote = %v after destroy, want unset", *got.Status.MayPromote)
	}
}
