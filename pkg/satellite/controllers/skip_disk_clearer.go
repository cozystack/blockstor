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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// SSA unstructured field-name constants used by both writeSkipDiskProp
// (stamp) and SkipDiskClearer (release). Same string spellings the
// CRD's OpenAPI schema uses on disk; pulled out as constants so the
// goconst lint check passes when both call sites share the literals.
const (
	specFieldResourceDefinitionName = "resourceDefinitionName"
	specFieldNodeName               = "nodeName"
	specFieldProps                  = "props"
)

// SkipDiskClearer implements satellite.SkipDiskClearer. Releases the
// observer's SSA claim on Spec.Props["DrbdOptions/SkipDisk"] when the
// satellite reconciler detects the kernel re-emerged healthy after a
// defensive SkipDisk stamp (Bug 278: Talos kernel upgrade reattach).
//
// Why this isn't "auto-recovery beyond upstream": SkipDisk on a healthy
// slot is an artifact OF our defensive stamping (the observer's
// writeSkipDiskProp under observerSkipDiskFieldOwner). Removing our
// own stamp is symmetric with stamping it — not new behavior.
// Operator-set SkipDisk (via `linstor r prop set ... SkipDisk=True`)
// remains intact: the operator's apply uses the controller's
// FieldOwner ("blockstor-controller"), so SSA's per-key merge keeps
// the operator's claim alive when this clearer releases only its own
// owner's claim.
//
// One instance per satellite — the agent wires this in after the
// controller-runtime manager is built (cached client lives there).
type SkipDiskClearer struct {
	// Client is the controller-runtime cached client. Reads + writes
	// flow through the same client the rest of the controllers use
	// so the SSA release lands on the same apiserver round-trip the
	// observer's stamp uses.
	Client client.Client

	// NodeName is this satellite's identity. Used to build the
	// per-node Resource CRD name (`<rd>.<node>`) the apiserver
	// holds Spec.Props under.
	NodeName string
}

// ClearSkipDisk SSA-applies a Spec.Props document WITHOUT the
// SkipDisk key under the observer's own FieldOwner
// (`observerSkipDiskFieldOwner`). SSA's per-key map merge releases
// that owner's claim on `DrbdOptions/SkipDisk`; when no other writer
// claims the key, the apiserver removes it from Spec.Props.
//
// Idempotent — repeat calls converge on the "owner releases the key"
// state (the second apply is a no-op at the apiserver level because
// the claim is already gone). NotFound on the Resource CRD is
// swallowed: the convergence-pending case mirrors writeSkipDiskProp's
// silence policy.
//
// ForceOwnership is intentionally NOT set. We're releasing OUR claim,
// not claiming the field — forcing ownership would re-claim the empty
// state and prevent the apiserver from deleting the key when nobody
// else owns it. The observer's stamp path uses ForceOwnership because
// it's claiming the key against the controller's resolved bag; the
// clear path uses the same owner with the SAME apply shape minus the
// SkipDisk key, which is the standard SSA way to release a claim.
func (s *SkipDiskClearer) ClearSkipDisk(ctx context.Context, resourceName string) error {
	if resourceName == "" {
		return nil
	}

	name := k8s.Name(resourceName + "." + s.NodeName)

	// Read the existing Resource to carry the immutable required
	// scalars (`resourceDefinitionName`, `nodeName`) on the SSA apply
	// — same rationale as writeSkipDiskProp: kubebuilder marks both
	// `+required` with no `omitempty`, so a typed apply without them
	// fails SSA validation. NotFound is the convergence-pending case:
	// the Resource CRD hasn't materialised yet, so there's nothing to
	// clear; silently no-op.
	var existing blockstoriov1alpha1.Resource

	err := s.Client.Get(ctx, client.ObjectKey{Name: name}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return errors.Wrapf(err, "get Resource %s", name)
	}

	// Unstructured so the serialised SSA apply object carries an
	// EMPTY `spec.props` map under the observer's owner. SSA's
	// per-key merge sees the owner's previous apply claimed
	// SkipDisk, and the new apply omits it — claim is released. If
	// no other writer owns the key, the apiserver deletes it from
	// Spec.Props. Other keys claimed by other writers (the
	// controller's resolved bag, etc.) stay untouched because
	// per-key merge isolates each writer's claims.
	apply := &unstructured.Unstructured{}
	apply.SetGroupVersionKind(blockstoriov1alpha1.GroupVersion.WithKind(resourceKind))
	apply.SetName(name)
	apply.Object["spec"] = map[string]any{
		specFieldResourceDefinitionName: existing.Spec.ResourceDefinitionName,
		specFieldNodeName:               existing.Spec.NodeName,
		specFieldProps:                  map[string]any{},
	}

	err = s.Client.Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
		client.FieldOwner(observerSkipDiskFieldOwner))
	if err != nil {
		return errors.Wrapf(err, "ssa release Resource.Spec.Props SkipDisk %s", name)
	}

	return nil
}
