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
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// dropPlaintextPassphraseProps removes every property that would carry a
// LUKS passphrase from the resolved effective-props bag, and names the
// ones that held a value in a log line, never the values. Each LUKS volume
// has its own password, stored wrapped on its replica (luks_keys.go); a
// passphrase in a property is never a key to anything, and a property is
// plaintext on its CR.
func dropPlaintextPassphraseProps(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition, effectiveProps map[string]string) {
	ignored := passphrase.DropPlaintextProps(effectiveProps)

	if rd == nil || len(ignored) == 0 || !layerStackHasLUKS(rd.Spec.LayerStack) {
		return
	}

	log.FromContext(ctx).Info("ignoring LUKS passphrase properties; every volume has its own password",
		"resourceDefinition", rd.Name, "properties", ignored)
}

// layerStackHasLUKS reports whether the RD's layer stack names LUKS
// (case-insensitive, mirroring pkg/satellite's needsLUKS).
func layerStackHasLUKS(stack []string) bool {
	for _, layer := range stack {
		if strings.EqualFold(layer, "LUKS") {
			return true
		}
	}

	return false
}
