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

package linstormigrate

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
)

// A meta pool left off a converted definition and its replicas stays off
// after an ordinary property update through the store: the store merges the
// overflow props back into the props on every write, so a copy left there
// would return and send the adopted replica down the external-metadata path.
func TestADroppedMetaPoolStaysOffThroughAStoreUpdate(t *testing.T) {
	t.Parallel()

	res := convertTwoReplicas(t, withProps(stackDump("[]",
		layerChain(1, "NODE-A", "PVC-S", "DRBD", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE")),
		PropsContainerRow{PropsInstance: "/RSC_DFNS/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"},
		PropsContainerRow{PropsInstance: "/RSCS/NODE-A/PVC-S", PropKey: "StorPoolNameDrbdMeta", PropValue: "meta"}))

	if len(res.ResourceDefinitions) != 1 || len(res.Resources) != 2 {
		t.Fatalf("converted %d definitions and %d replicas; warnings %v", len(res.ResourceDefinitions), len(res.Resources), res.Warnings)
	}

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	objs := make([]ctrlclient.Object, 0, 1+len(res.Resources))
	objs = append(objs, &res.ResourceDefinitions[0])
	for i := range res.Resources {
		objs = append(objs, &res.Resources[i])
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	st := k8sstore.New(cli)
	ctx := t.Context()

	addAux := func(props map[string]string) map[string]string {
		if props == nil {
			props = map[string]string{}
		}

		props["Aux/example"] = "value"

		return props
	}

	if err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "pvc-s", func(rd *apiv1.ResourceDefinition) error {
		rd.Props = addAux(rd.Props)

		return nil
	}); err != nil {
		t.Fatalf("update the definition: %v", err)
	}

	if err := st.Resources().PatchResourceSpec(ctx, "pvc-s", "node-a", func(r *apiv1.Resource) error {
		r.Props = addAux(r.Props)

		return nil
	}); err != nil {
		t.Fatalf("update the replica: %v", err)
	}

	// The dispatcher takes the meta pool from these two Props maps.
	var rd crdv1alpha1.ResourceDefinition
	if err := cli.Get(ctx, ctrlclient.ObjectKeyFromObject(&res.ResourceDefinitions[0]), &rd); err != nil {
		t.Fatalf("read the definition back: %v", err)
	}

	if v, ok := rd.Spec.Props[storPoolNameDrbdMeta]; ok {
		t.Errorf("the definition carries %s %q after an update", storPoolNameDrbdMeta, v)
	}

	for i := range res.Resources {
		var r crdv1alpha1.Resource
		if err := cli.Get(ctx, ctrlclient.ObjectKeyFromObject(&res.Resources[i]), &r); err != nil {
			t.Fatalf("read replica %s back: %v", res.Resources[i].Name, err)
		}

		if v, ok := r.Spec.Props[storPoolNameDrbdMeta]; ok {
			t.Errorf("replica %s carries %s %q after an update", r.Name, storPoolNameDrbdMeta, v)
		}
	}
}
