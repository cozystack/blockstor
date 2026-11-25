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
	"fmt"
	"regexp"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// addressReuseResources matches the resource names drbdadm gives when it
// refuses a configuration whose address another .res on the node uses
// (drbdadm_postparse.c): "<file>:<line>: in resource <a>" and
// "<af>:<addr>:<port> is also used <file>:<line> (resource <b>)".
var addressReuseResources = regexp.MustCompile(`in resource (\S+)\s+\S+ is also used \S+ \(resource ([^)\s]+)\)`)

// heldAddressOwner is the resource whose .res still holds the address
// the apply of rdName was refused over, "" when message is not that
// refusal.
func heldAddressOwner(message, rdName string) string {
	match := addressReuseResources.FindStringSubmatch(message)
	if match == nil {
		return ""
	}

	for _, name := range match[1:] {
		if !naming.Same(name, rdName) {
			return name
		}
	}

	return ""
}

// waitForAddressHandover turns the refusal of an apply over an address
// whose previous owner on this node is gone or being deleted into a wait.
// The controller hands a port out once no Resource on the node holds it,
// and a replica whose Resource went away can keep its .res for a moment
// longer, while its teardown runs or retries: drbdadm then refuses the
// new .res that uses the same port, and the next pass, once the old .res
// is removed, goes through. Reported as a failed apply, that read as a
// fault on every such handover. An owner that is still a live replica
// here is a real conflict and stays a failure, and a wait that lasts is
// reported after a minute, as every wait for the controller is
// (trackControllerWait).
func (r *ResourceReconciler) waitForAddressHandover(
	ctx context.Context, res *blockstoriov1alpha1.Resource, results []*intent.ResourceApplyResult, logger logr.Logger,
) {
	for _, result := range results {
		if result.GetOk() || result.GetWaitingForController() {
			continue
		}

		owner := heldAddressOwner(result.GetMessage(), res.Spec.ResourceDefinitionName)
		if owner == "" || r.ownerLivesHere(ctx, owner, res.Spec.NodeName, logger) {
			continue
		}

		result.WaitingForController = true
		result.Message = fmt.Sprintf("waiting for the teardown of %s, whose .res still holds the address: %s",
			owner, result.GetMessage())
	}
}

// ownerLivesHere reports whether node holds a replica of rdName that is
// not being deleted. A read that fails counts as one: the refusal then
// stays the failure it was.
func (r *ResourceReconciler) ownerLivesHere(ctx context.Context, rdName, node string, logger logr.Logger) bool {
	reader := r.peerReader()

	name, err := replicaCRName(ctx, reader, rdName, node)
	if err != nil {
		logger.V(1).Info("cannot tell whether the address's previous owner is still here", "owner", rdName, "error", err.Error())

		return true
	}

	var owner blockstoriov1alpha1.Resource

	err = reader.Get(ctx, client.ObjectKey{Name: name}, &owner)
	if apierrors.IsNotFound(err) {
		return false
	}

	if err != nil {
		logger.V(1).Info("cannot tell whether the address's previous owner is still here", "owner", rdName, "error", err.Error())

		return true
	}

	return owner.DeletionTimestamp.IsZero()
}
