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

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// The kernel, the storage provider and the intent know a resource by its
// definition's CR name: pvc-mix, or a hash for a name outside RFC 1123.
// The store names a replica's Resource CR, and a Snapshot CR, from the
// spellings the definition and the node were registered under, and for a
// hashed definition those do not hash back from the CR name. So a CR a
// satellite reaches from a kernel name is named through the registered
// spellings, the way the store named it at create.

// replicaCRName names the Resource CR of the replica of rdName on node.
func replicaCRName(ctx context.Context, reader client.Reader, rdName, node string) (string, error) {
	rd, err := registeredRDName(ctx, reader, rdName)
	if err != nil {
		return "", err
	}

	var crd blockstoriov1alpha1.Node

	err = reader.Get(ctx, client.ObjectKey{Name: naming.Key(node)}, &crd)

	switch {
	case err == nil:
		node = k8s.OriginalName(&crd.ObjectMeta)
	case !apierrors.IsNotFound(err):
		return "", errors.Wrapf(err, "get Node %s", node)
	}

	return k8s.ResourceCRName(rd, node), nil
}

// snapshotCRName names the Snapshot CR snap of rdName.
func snapshotCRName(ctx context.Context, reader client.Reader, rdName, snap string) (string, error) {
	rd, err := registeredRDName(ctx, reader, rdName)
	if err != nil {
		return "", err
	}

	return k8s.SnapshotCRName(rd, snap), nil
}

// registeredRDName is the spelling rdName's definition was registered
// under. A definition that is not there leaves rdName as it is: whatever
// CR the caller reaches for is not there either.
func registeredRDName(ctx context.Context, reader client.Reader, rdName string) (string, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := reader.Get(ctx, client.ObjectKey{Name: naming.Key(rdName)}, &rd)
	if apierrors.IsNotFound(err) {
		return rdName, nil
	}

	if err != nil {
		return "", errors.Wrapf(err, "get ResourceDefinition %s", rdName)
	}

	return k8s.OriginalName(&rd.ObjectMeta), nil
}
