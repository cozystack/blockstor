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

package controller

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/cozystack/blockstor/pkg/passphrase"
)

// MasterKeyPeriod is how often the controller looks at the passphrase and
// keyring Secrets. They are read by name, not watched: Secret RBAC is a
// namespaced get.
const MasterKeyPeriod = 30 * time.Second

// MasterKeyRunnable keeps the keyring in step with the passphrase Secret.
// On a cluster whose ControllerConfig names a passphrase Secret and that
// has no keyring yet, it creates one, the way LINSTOR creates its master
// key from MASTER_PASSPHRASE on first start.
//
// Leader-elected: two controllers would each create a master key, and
// although only one keyring Secret can be created, there is no reason to
// race for it.
//
// +kubebuilder:rbac:groups="",namespace=blockstor-system,resources=secrets,verbs=get;create
type MasterKeyRunnable struct {
	// Client writes the keyring Secret.
	Client client.Client

	// Reader reads the Secrets, the ControllerConfig and the replicas
	// past the cache. Production wires mgr.GetAPIReader().
	Reader client.Reader

	// Namespace is blockstor's own namespace, where both Secrets live.
	Namespace string

	// Period overrides MasterKeyPeriod (tests).
	Period time.Duration

	// Now overrides time.Now (tests).
	Now func() time.Time
}

// NeedLeaderElection returns true: only one controller writes the keyring.
func (*MasterKeyRunnable) NeedLeaderElection() bool { return true }

// Start runs one pass at once and then one per period until ctx ends.
func (r *MasterKeyRunnable) Start(ctx context.Context) error {
	period := r.Period
	if period == 0 {
		period = MasterKeyPeriod
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		r.Pass(ctx)

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Pass runs one bootstrap attempt and logs what came of it.
func (r *MasterKeyRunnable) Pass(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("master-key")

	now := time.Now
	if r.Now != nil {
		now = r.Now
	}

	created, err := passphrase.Bootstrap(ctx, r.Client,
		passphrase.Secrets{Reader: r.Reader, Namespace: r.Namespace}, now())
	if err != nil {
		logger.Error(err, "keyring bootstrap")

		return
	}

	if created {
		logger.Info("created the keyring from the passphrase Secret",
			"secret", passphrase.KeyringSecretName, "namespace", r.Namespace)
	}
}

// RegisterWithManager adds the runnable to mgr.
func (r *MasterKeyRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(r)
	if err != nil {
		return errors.Wrap(err, "add MasterKeyRunnable")
	}

	return nil
}
