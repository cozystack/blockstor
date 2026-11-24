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
// keyring Secrets. With Reader a passphrase.WatchedReader a pass reads
// them from its watches, so an idle pass sends no request.
const MasterKeyPeriod = 30 * time.Second

// MasterKeyRunnable keeps the keyring in step with the passphrase Secret.
// On a cluster whose ControllerConfig names a passphrase Secret and that
// has no keyring yet, it creates one, the way LINSTOR creates its master
// key from MASTER_PASSPHRASE on first start. Once the Secret opens a slot
// that is not superseded, it removes the slots modify-passphrase marked
// superseded.
//
// Leader-elected: two controllers would each create a master key, and
// although only one keyring Secret can be created, there is no reason to
// race for it.
//
// Its Secret access is the hand-written, resourceNames-limited Role in
// config/rbac/secrets_role.yaml, not a kubebuilder marker: a marker
// cannot name the Secrets, so it would grant every Secret in the
// namespace.
type MasterKeyRunnable struct {
	// Client writes the keyring Secret.
	Client client.Client

	// Reader reads the Secrets and the ControllerConfig a pass looks
	// at. Production wires a passphrase.WatchedReader, so a pass that
	// finds nothing to do sends no request.
	Reader client.Reader

	// Direct reads the keyring before a write to it, and the replicas
	// before a keyring is created: Reader trails the API server.
	// Production wires mgr.GetAPIReader(). Nil is Reader.
	Direct client.Reader

	// Namespace is blockstor's own namespace, where both Secrets live.
	Namespace string

	// Period overrides MasterKeyPeriod (tests).
	Period time.Duration

	// Now overrides time.Now (tests).
	Now func() time.Time

	// unlocker is kept across passes: while a superseded slot waits for
	// the Secret to move, each pass would otherwise run Argon2id again.
	unlocker *passphrase.Unlocker
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

	secrets := passphrase.Secrets{Reader: r.Reader, Namespace: r.Namespace, Direct: r.Direct}

	created, err := passphrase.Bootstrap(ctx, r.Client, secrets, now())
	if err != nil {
		logger.Error(err, "keyring bootstrap")

		return
	}

	if created {
		logger.Info("created the keyring from the passphrase Secret",
			"secret", passphrase.KeyringSecretName, "namespace", r.Namespace)
	}

	if r.unlocker == nil {
		r.unlocker = &passphrase.Unlocker{Secrets: secrets}
	}

	ops := &passphrase.Ops{Client: r.Client, Secrets: secrets, Now: r.Now, Unlocker: r.unlocker}

	removed, err := ops.CleanupSuperseded(ctx)
	if err != nil {
		logger.Error(err, "remove superseded keyring slots")

		return
	}

	if removed {
		logger.Info("removed the superseded keyring slots: the passphrase Secret opens its new slot")
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
