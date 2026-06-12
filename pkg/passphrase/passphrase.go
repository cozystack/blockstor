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

// Package passphrase centralises access to the cluster master
// passphrase Secret — the value `linstor encryption
// create-passphrase` stamps (POST /v1/encryption/passphrase, served
// by pkg/rest/encryption.go) and the LUKS layer consumes.
//
// Bug 023: before this package, the Secret was written by the REST
// layer but never read outside it — the LUKS RD-create gate and the
// satellite-facing dispatch chain only consulted the legacy
// plaintext controller property `DrbdOptions/EncryptPassphrase`, so
// the upstream-standard `encryption create-passphrase` →
// `rd create --layer-list ...,luks,...` flow dead-ended with a hint
// telling operators to put a PLAINTEXT passphrase into a controller
// prop. Both the REST gate (pkg/rest) and the satellite-side
// dispatch (pkg/satellite/controllers) now resolve the passphrase
// through this single implementation.
package passphrase

import (
	"context"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// DefaultSecretName is the Secret the controller falls back to when
// ControllerConfig.Spec.PassphraseSecretRef is unset. Operators
// override via the ControllerConfig CRD.
const DefaultSecretName = "blockstor-cluster-passphrase"

// SecretKey is the data key inside the Secret carrying the cluster
// passphrase. Matches the upstream-LINSTOR-on-k8s convention so
// existing Secret YAML manifests continue to work.
const SecretKey = "passphrase"

// PropKeyCanonical is upstream LINSTOR's cluster-scope master-key
// property name (`linstor controller set-property
// DrbdOptions/EncryptPassphrase <pass>`). Bug 023: this LEGACY path
// stores the passphrase in PLAINTEXT on the ControllerConfig CRD —
// kept working for backward compatibility, but the Secret written
// by `encryption create-passphrase` is the primary mechanism.
const PropKeyCanonical = "DrbdOptions/EncryptPassphrase"

// PropKeyLegacy is the early-Phase-9 per-RD alias the dispatcher
// still alias-reads (Bug 265 history, pkg/dispatcher). Deprecated.
const PropKeyLegacy = "DrbdOptions/Encryption/passphrase"

// SecretName resolves the passphrase Secret's name from the
// singleton ControllerConfig, falling back to DefaultSecretName when
// the ControllerConfig is absent or doesn't pin a reference. The
// fallback lets operators get a working cluster without first
// applying a ControllerConfig CRD.
func SecretName(ctx context.Context, reader client.Reader) (string, error) {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := reader.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return DefaultSecretName, nil
		}

		return "", errors.Wrap(err, "get ControllerConfig")
	}

	if ctrlConfig.Spec.PassphraseSecretRef != nil && ctrlConfig.Spec.PassphraseSecretRef.Name != "" {
		return ctrlConfig.Spec.PassphraseSecretRef.Name, nil
	}

	return DefaultSecretName, nil
}

// Read returns the cluster master passphrase from the encryption
// Secret in the given namespace. Empty string (not an error) means
// "not yet set" — the Secret is missing or carries an empty value;
// that's the explicit signal the create/enter handshake relies on.
func Read(ctx context.Context, reader client.Reader, namespace string) (string, error) {
	name, err := SecretName(ctx, reader)
	if err != nil {
		return "", err
	}

	var sec corev1.Secret

	err = reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &sec)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}

		return "", errors.Wrap(err, "get passphrase Secret")
	}

	return string(sec.Data[SecretKey]), nil
}
