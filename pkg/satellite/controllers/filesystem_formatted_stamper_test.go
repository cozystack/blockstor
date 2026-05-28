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
	"encoding/json"
	"testing"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestFilesystemFormattedAndObservedSharedSSAShape pins the key
// invariant for the observe-existing-fs path: StampFilesystemObserved
// MUST produce a Server-Side Apply patch whose shape is byte-identical
// to StampFilesystemFormatted EXCEPT for the Reason and Message fields
// of the single FilesystemFormatted Condition entry.
//
// Why this matters: both writers share the same FieldOwner. SSA's
// listMap merge on `type=FilesystemFormatted` lets two callers under
// one owner cleanly overwrite each other's slot — but ONLY if neither
// writer accidentally claims an adjacent listMap (`.status.volumes`,
// `.status.conditions[type=KernelLoaded]`, etc). If StampFilesystemObserved
// drifts from StampFilesystemFormatted's shape, the apiserver may
// reassign ownership of unrelated Status sub-trees and silently null
// out fields the observer / volume-status writers own — the exact
// regression that wedged PR #32's predecessor (`.status.volumes` was
// reset to null and every e2e lane failed with `volumes":null`).
func TestFilesystemFormattedAndObservedSharedSSAShape(t *testing.T) {
	formattedPatch := captureStamperPatch(t, "fs-rd.n1", func(s *FilesystemFormattedStamper, ctx context.Context, name string) error {
		return s.StampFilesystemFormatted(ctx, name)
	})

	observedPatch := captureStamperPatch(t, "fs-rd.n1", func(s *FilesystemFormattedStamper, ctx context.Context, name string) error {
		return s.StampFilesystemObserved(ctx, name)
	})

	if formattedPatch.fieldOwner != observedPatch.fieldOwner {
		t.Fatalf("FieldOwner MUST match across both stamp paths;\n  formatted: %q\n  observed:  %q",
			formattedPatch.fieldOwner, observedPatch.fieldOwner)
	}

	if formattedPatch.fieldOwner != filesystemFormattedFieldOwner {
		t.Fatalf("formatted FieldOwner drifted from package constant;\n  want: %q\n  got:  %q",
			filesystemFormattedFieldOwner, formattedPatch.fieldOwner)
	}

	// Strip the timestamp + Reason + Message fields BEFORE comparing
	// the marshalled patch bytes. LastTransitionTime is metav1.Now()
	// (different between the two captures by construction); Reason /
	// Message are the very fields we INTEND to differ. Everything
	// else — Kind, APIVersion, ObjectMeta.Name, Status.Conditions
	// list shape, Condition Type/Status — MUST be byte-identical.
	fStripped := stripConditionVariableFields(t, formattedPatch.body)
	oStripped := stripConditionVariableFields(t, observedPatch.body)

	if string(fStripped) != string(oStripped) {
		t.Fatalf("SSA patch shape diverged outside Reason/Message/LastTransitionTime — this risks the .status.volumes ownership regression PR #32 hit\nformatted (stripped):\n%s\n\nobserved (stripped):\n%s",
			fStripped, oStripped)
	}

	// Sanity: the unstripped bytes MUST differ — otherwise our stamp
	// methods are identical and the audit-trail distinction is gone.
	if string(formattedPatch.body) == string(observedPatch.body) {
		t.Fatalf("Reason/Message MUST differ between StampFilesystemFormatted and StampFilesystemObserved; both produced identical patch bytes")
	}

	if formattedPatch.reason() != "MkfsSucceeded" {
		t.Errorf("formatted Reason: want MkfsSucceeded; got %q", formattedPatch.reason())
	}

	if observedPatch.reason() != "FilesystemObserved" {
		t.Errorf("observed Reason: want FilesystemObserved; got %q", observedPatch.reason())
	}
}

// capturedPatch holds the raw SSA patch bytes + the FieldOwner string
// the controller-runtime client recorded on a single
// `Status().Patch(..., client.Apply, client.FieldOwner(...))` call.
type capturedPatch struct {
	fieldOwner string
	body       []byte
}

// reason pulls the single Condition's Reason field out of the
// captured patch body (best-effort: returns "" on parse error so
// the test's primary assertions still surface useful diffs).
func (p capturedPatch) reason() string {
	var doc struct {
		Status struct {
			Conditions []struct {
				Reason string `json:"reason"`
			} `json:"conditions"`
		} `json:"status"`
	}

	if err := json.Unmarshal(p.body, &doc); err != nil {
		return ""
	}

	if len(doc.Status.Conditions) == 0 {
		return ""
	}

	return doc.Status.Conditions[0].Reason
}

// captureStamperPatch builds a fake controller-runtime client with a
// SubResourcePatch interceptor, invokes the supplied stamp function,
// and returns the SSA patch the stamper emitted.
func captureStamperPatch(
	t *testing.T,
	resourceName string,
	call func(*FilesystemFormattedStamper, context.Context, string) error,
) capturedPatch {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	var got capturedPatch

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(_ context.Context, _ client.Client, _ string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				body, err := patch.Data(obj)
				if err != nil {
					return errors.Wrap(err, "fake SubResourcePatch: marshal patch body")
				}

				got.body = body

				patchOpts := &client.SubResourcePatchOptions{}
				patchOpts.ApplyOptions(opts)

				if patchOpts.FieldManager != "" {
					got.fieldOwner = patchOpts.FieldManager
				}

				return nil
			},
		}).
		Build()

	stamper := &FilesystemFormattedStamper{Client: cli}

	if err := call(stamper, t.Context(), resourceName); err != nil {
		t.Fatalf("stamp call: %v", err)
	}

	if got.body == nil {
		t.Fatalf("interceptor was never invoked — stamp path may be wired wrong")
	}

	return got
}

// stripConditionVariableFields removes the three Condition fields the
// shape comparison MUST ignore: `reason` and `message` (intentionally
// different between the two stamp methods) and `lastTransitionTime`
// (metav1.Now() ticks between captures). Everything else — Kind,
// APIVersion, ObjectMeta.Name, Conditions list-element keys for
// type / status — stays for byte comparison.
func stripConditionVariableFields(t *testing.T, raw []byte) []byte {
	t.Helper()

	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}

	status, _ := doc["status"].(map[string]any)
	if status == nil {
		return raw
	}

	conditions, _ := status["conditions"].([]any)

	for _, entry := range conditions {
		cond, _ := entry.(map[string]any)
		if cond == nil {
			continue
		}

		delete(cond, "reason")
		delete(cond, "message")
		delete(cond, "lastTransitionTime")
	}

	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal stripped patch: %v", err)
	}

	return out
}
