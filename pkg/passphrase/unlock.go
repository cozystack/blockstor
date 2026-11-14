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

package passphrase

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/keyring"
)

const (
	// KeyringSecretName is the Secret holding the master key's slots.
	// blockstor owns it and is its only writer.
	KeyringSecretName = "blockstor-master-key"

	// KeyringSecretType marks the keyring Secret.
	KeyringSecretType corev1.SecretType = "blockstor.org/master-key" //nolint:gosec // a Secret type, not a credential

	// KeyringDataKey is the keyring Secret's one data key.
	KeyringDataKey = "keyring.json"

	// UnlockSecretName is the Secret blockstor generates and unlocks
	// from when ControllerConfig names no passphrase Secret.
	UnlockSecretName = "blockstor-unlock"

	// DefaultPassphraseKey is the data key of a passphrase Secret, as
	// piraeus gives it to LINSTOR.
	DefaultPassphraseKey = "MASTER_PASSPHRASE"

	// SlotNameSecret names the slot enrolled from the passphrase Secret.
	SlotNameSecret = "secret"
)

// State is the cluster's encryption state, as `linstor encryption
// status` reports it.
type State string

const (
	// StateUnset is a cluster with no keyring.
	StateUnset State = "unset"

	// StateLocked is a keyring that the passphrase Secret does not open:
	// the Secret is missing, empty or carries another passphrase.
	StateLocked State = "locked"

	// StateUnlocked is a keyring the passphrase Secret opens.
	StateUnlocked State = "unlocked"
)

// ErrKeyringLost is a passphrase Secret with no keyring while replicas
// already carry wrapped passwords: a fresh keyring would wrap under a new
// master key and leave every one of them unreadable, so none is created.
var ErrKeyringLost = errors.New(
	"the keyring Secret is missing while replicas or snapshots carry LUKS passwords wrapped under it; " +
		"restore " + KeyringSecretName + " from a backup instead of creating a new one")

// Ref is where the master passphrase is read from.
type Ref struct {
	// Name and Key address the Secret and its data key.
	Name, Key string

	// Generated is the blockstor-owned unlock Secret, used when the
	// ControllerConfig names no passphrase Secret.
	Generated bool
}

// ResolveRef reads the passphrase Secret reference from the ControllerConfig
// singleton. With none, it is blockstor's own generated unlock Secret.
func ResolveRef(ctx context.Context, reader client.Reader) (Ref, error) {
	generated := Ref{Name: UnlockSecretName, Key: DefaultPassphraseKey, Generated: true}

	var cfg blockstoriov1alpha1.ControllerConfig

	err := reader.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &cfg)
	if apierrors.IsNotFound(err) {
		return generated, nil
	}

	if err != nil {
		return Ref{}, errors.Wrap(err, "get ControllerConfig")
	}

	ref := cfg.Spec.PassphraseSecretRef
	if ref == nil || ref.Name == "" {
		return generated, nil
	}

	key := ref.Key
	if key == "" {
		key = DefaultPassphraseKey
	}

	return Ref{Name: ref.Name, Key: key, Generated: false}, nil
}

// Secrets reads the passphrase Secret and the keyring Secret. Reader
// should be the manager's direct reader: Secret RBAC is a namespaced get,
// which a cache cannot list and watch with.
type Secrets struct {
	Reader    client.Reader
	Namespace string
}

// Keyring returns the keyring and its Secret's resourceVersion, or a nil
// keyring when the Secret does not exist.
func (s Secrets) Keyring(ctx context.Context) (*keyring.Keyring, string, error) {
	var sec corev1.Secret

	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: KeyringSecretName}, &sec)
	if apierrors.IsNotFound(err) {
		return nil, "", nil
	}

	if err != nil {
		return nil, "", errors.Wrap(err, "get the keyring Secret")
	}

	ring, err := keyring.Parse(sec.Data[KeyringDataKey])
	if err != nil {
		return nil, "", errors.Wrapf(err, "secret %s/%s", s.Namespace, KeyringSecretName)
	}

	return ring, sec.ResourceVersion, nil
}

// Passphrase returns the passphrase ref addresses and its Secret's
// resourceVersion. A missing Secret or an empty value is a nil passphrase,
// not an error. The bytes are verbatim: a trailing newline is part of it.
func (s Secrets) Passphrase(ctx context.Context, ref Ref) ([]byte, string, error) {
	var sec corev1.Secret

	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: ref.Name}, &sec)
	if apierrors.IsNotFound(err) {
		return nil, "", nil
	}

	if err != nil {
		return nil, "", errors.Wrapf(err, "get the passphrase Secret %s", ref.Name)
	}

	value := sec.Data[ref.Key]
	if len(value) == 0 {
		return nil, sec.ResourceVersion, nil
	}

	return bytes.Clone(value), sec.ResourceVersion, nil
}

// Status is what Unlock found.
type Status struct {
	State State

	// Ref is where the passphrase was read from.
	Ref Ref

	// Ring is the keyring, nil when State is StateUnset.
	Ring *keyring.Keyring

	// RingVersion is the keyring Secret's resourceVersion.
	RingVersion string

	// Key is the unlocked master key and SlotID the slot that opened
	// it, when State is StateUnlocked. Key is shared with every other
	// caller of the same Unlocker; it must not be destroyed.
	Key    *keyring.MasterKey
	SlotID string
}

// Unlocker opens the master key from the passphrase Secret and remembers
// the outcome, open or not, for as long as neither Secret changes: every
// failed attempt costs an Argon2id run per slot, and reconciles ask often.
type Unlocker struct {
	Secrets Secrets

	mu     sync.Mutex
	cached *cachedStatus

	// attempts counts the passphrases tried against the keyring, for
	// the tests that pin the cache.
	attempts int
}

type cachedStatus struct {
	ref              Ref
	passVer, ringVer string
	status           Status
}

// Unlock reports the encryption state and, when the passphrase Secret
// opens the keyring, the master key.
func (u *Unlocker) Unlock(ctx context.Context) (Status, error) {
	ref, err := ResolveRef(ctx, u.Secrets.Reader)
	if err != nil {
		return Status{}, err
	}

	ring, ringVer, err := u.Secrets.Keyring(ctx)
	if err != nil {
		return Status{}, err
	}

	if ring == nil {
		return Status{State: StateUnset, Ref: ref}, nil
	}

	pass, passVer, err := u.Secrets.Passphrase(ctx, ref)
	if err != nil {
		return Status{}, err
	}

	defer clear(pass)

	u.mu.Lock()
	defer u.mu.Unlock()

	if c := u.cached; c != nil && c.ref == ref && c.passVer == passVer && c.ringVer == ringVer {
		return c.status, nil
	}

	status := Status{State: StateLocked, Ref: ref, Ring: ring, RingVersion: ringVer, Key: nil, SlotID: ""}

	if pass != nil {
		u.attempts++

		key, slotID, unlockErr := ring.Unlock(pass)

		switch {
		case unlockErr == nil:
			status.State, status.Key, status.SlotID = StateUnlocked, key, slotID
		case !errors.Is(unlockErr, keyring.ErrWrongPassphrase):
			return Status{}, errors.Wrapf(unlockErr, "secret %s/%s", u.Secrets.Namespace, KeyringSecretName)
		}
	}

	// The key a previous status held may still be in use by a caller
	// that read it a moment ago, so it is dropped, not destroyed.
	u.cached = &cachedStatus{ref: ref, passVer: passVer, ringVer: ringVer, status: status}

	return status, nil
}

// Bootstrap creates the keyring from the passphrase Secret the
// ControllerConfig names, the way LINSTOR creates its master key from
// MASTER_PASSPHRASE on first start: a fresh master key with one slot,
// `secret`, for the Secret's passphrase. It does nothing when a keyring
// exists, when no Secret is named or the named one carries nothing, and
// it refuses with ErrKeyringLost when a replica already carries a wrapped
// password. A keyring another controller created first is not an error.
//
// It reports whether it created the keyring.
func Bootstrap(ctx context.Context, writer client.Client, secrets Secrets, now time.Time) (bool, error) {
	ref, err := ResolveRef(ctx, secrets.Reader)
	if err != nil || ref.Generated {
		return false, err
	}

	ring, _, err := secrets.Keyring(ctx)
	if err != nil || ring != nil {
		return false, err
	}

	pass, _, err := secrets.Passphrase(ctx, ref)
	if err != nil || pass == nil {
		return false, err
	}

	defer clear(pass)

	err = refuseLostKeyring(ctx, secrets.Reader)
	if err != nil {
		return false, err
	}

	ring, key, err := keyring.New(pass, SlotNameSecret, keyring.OriginSecret, now)
	if err != nil {
		return false, errors.Wrap(err, "create the keyring")
	}

	key.Destroy()

	return CreateKeyring(ctx, writer, secrets.Namespace, ring)
}

// CreateKeyring writes a new keyring Secret and reports whether it did:
// false, with no error, when one already exists.
func CreateKeyring(ctx context.Context, writer client.Client, namespace string, ring *keyring.Keyring) (bool, error) {
	data, err := ring.Marshal()
	if err != nil {
		return false, err //nolint:wrapcheck // keyring errors name what is malformed
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: KeyringSecretName, Namespace: namespace},
		Type:       KeyringSecretType,
		Data:       map[string][]byte{KeyringDataKey: data},
	}

	err = writer.Create(ctx, sec)
	if apierrors.IsAlreadyExists(err) {
		return false, nil
	}

	if err != nil {
		return false, errors.Wrap(err, "create the keyring Secret")
	}

	return true, nil
}

// refuseLostKeyring is ErrKeyringLost when a replica or a snapshot carries
// a wrapped password: with no keyring, the key it is wrapped under is gone, and a
// new master key would leave it unreadable for good.
func refuseLostKeyring(ctx context.Context, reader client.Reader) error {
	carrier, err := AnyLUKSKeyCarrier(ctx, reader)
	if err != nil {
		return err
	}

	if carrier != "" {
		return errors.Wrapf(ErrKeyringLost, "%s", carrier)
	}

	return nil
}

// AnyLUKSKeyCarrier names a replica or a snapshot that carries a wrapped
// LUKS password, or returns "" when none does. A snapshot counts: its
// passwords are wrapped under the same master key, and a new key would
// leave it unrestorable.
func AnyLUKSKeyCarrier(ctx context.Context, reader client.Reader) (string, error) {
	var replicas blockstoriov1alpha1.ResourceList

	err := reader.List(ctx, &replicas)
	if err != nil {
		return "", errors.Wrap(err, "list replicas")
	}

	for i := range replicas.Items {
		for _, vol := range replicas.Items[i].Spec.Volumes {
			if vol.LUKSKey != nil {
				return "replica " + replicas.Items[i].Name, nil
			}
		}
	}

	var snapshots blockstoriov1alpha1.SnapshotList

	err = reader.List(ctx, &snapshots)
	if err != nil {
		return "", errors.Wrap(err, "list snapshots")
	}

	for i := range snapshots.Items {
		if len(snapshots.Items[i].Spec.LUKSKeys) > 0 {
			return "snapshot " + snapshots.Items[i].Name, nil
		}
	}

	return "", nil
}
