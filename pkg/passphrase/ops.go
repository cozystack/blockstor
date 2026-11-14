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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cozystack/blockstor/pkg/keyring"
)

// Slot names the operations give.
const (
	// SlotNameOperator is the slot create-passphrase enrols the
	// operator's passphrase in.
	SlotNameOperator = "operator"

	// SlotNameGenerated is the slot of the generated unlock Secret.
	SlotNameGenerated = "generated"

	// generatedPassBytes is the entropy of the generated unlock
	// passphrase.
	generatedPassBytes = 32

	kibPerMiB = 1024
)

var (
	// ErrKeyringExists is a create on a cluster that has a keyring.
	ErrKeyringExists = errors.New("the cluster already has a master passphrase")

	// ErrNoKeyring is an operation that needs a keyring on a cluster
	// without one.
	ErrNoKeyring = errors.New("the cluster has no master passphrase; create one first")

	// ErrSecretManaged is a create on a cluster whose ControllerConfig
	// names a passphrase Secret: the keyring comes from that Secret.
	ErrSecretManaged = errors.New("the keyring is created from the passphrase Secret the ControllerConfig names")

	// ErrUnlockSlot is a change that would leave the cluster unable to
	// unlock from its passphrase Secret.
	ErrUnlockSlot = errors.New("the slot is the one the cluster unlocks with")
)

// Ops runs the passphrase operations of `linstor encryption` and
// `blockstor encryption` against the keyring Secret. Passphrases given to
// them live only in keyring slots; the operator's passphrase Secret is
// never written.
type Ops struct {
	// Client writes the keyring Secret and the generated unlock Secret.
	Client client.Client

	// Secrets reads both, past any cache.
	Secrets Secrets

	// Now overrides time.Now (tests).
	Now func() time.Time
}

// CreateResult is what Create did.
type CreateResult struct {
	// AlreadySet is a keyring that the passphrase already opens: the
	// create is a no-op, as upstream answers a repeat.
	AlreadySet bool
}

// Create sets up the cluster's first master passphrase. With no passphrase
// Secret named, it creates the master key with a slot for passphrase and
// a slot for a generated passphrase, which it stores in the unlock Secret
// it owns, so the cluster unlocks unattended without the operator's
// passphrase resting anywhere in clear. With a Secret named, the keyring
// is created from that Secret and the create is refused.
func (o *Ops) Create(ctx context.Context, pass []byte) (CreateResult, error) {
	ring, _, err := o.Secrets.Keyring(ctx)
	if err != nil {
		return CreateResult{}, err
	}

	if ring != nil {
		_, opensErr := ring.Opens(pass)
		if opensErr == nil {
			return CreateResult{AlreadySet: true}, nil
		}

		return CreateResult{}, ErrKeyringExists
	}

	ref, err := ResolveRef(ctx, o.Secrets.Reader)
	if err != nil {
		return CreateResult{}, err
	}

	if !ref.Generated {
		return CreateResult{}, errors.Wrapf(ErrSecretManaged,
			"put the passphrase into Secret %s/%s, key %s", o.Secrets.Namespace, ref.Name, ref.Key)
	}

	ring, master, err := keyring.New(pass, SlotNameOperator, keyring.OriginAPI, o.now())
	if err != nil {
		return CreateResult{}, errors.Wrap(err, "create the keyring")
	}

	defer master.Destroy()

	generated, err := generateUnlockPassphrase()
	if err != nil {
		return CreateResult{}, err
	}

	defer clear(generated)

	_, err = ring.AddSlot(master, generated, SlotNameGenerated, keyring.OriginGenerated, o.now())
	if err != nil {
		return CreateResult{}, errors.Wrap(err, "add the generated slot")
	}

	created, err := CreateKeyring(ctx, o.Client, o.Secrets.Namespace, ring)
	if err != nil {
		return CreateResult{}, err
	}

	if !created {
		return CreateResult{}, ErrKeyringExists
	}

	// Written after the keyring: a crash in between leaves a locked
	// cluster that enter-passphrase repairs, never an unlock Secret for a
	// keyring that does not exist.
	return CreateResult{}, o.writeUnlockSecret(ctx, generated)
}

// EnterResult is what Enter found.
type EnterResult struct {
	// SlotID is the slot the passphrase opens.
	SlotID string

	// Status is the cluster's state after the call.
	Status Status

	// Repaired is a cluster on the generated unlock Secret that was
	// locked and that Enter gave a fresh generated slot and Secret.
	Repaired bool
}

// Enter verifies passphrase against every slot. On a cluster that runs on
// the generated unlock Secret and is locked, it also replaces the
// generated slot and Secret, which is the one way back from a deleted
// unlock Secret. Under a named passphrase Secret it only verifies; the
// Secret is the operator's to fix.
func (o *Ops) Enter(ctx context.Context, pass []byte) (EnterResult, error) {
	status, err := (&Unlocker{Secrets: o.Secrets}).Unlock(ctx)
	if err != nil {
		return EnterResult{}, err
	}

	if status.State == StateUnset {
		return EnterResult{}, ErrNoKeyring
	}

	master, slotID, err := status.Ring.Unlock(pass)
	if err != nil {
		return EnterResult{}, err //nolint:wrapcheck // keyring.ErrWrongPassphrase, said as it is
	}

	defer master.Destroy()

	if status.State == StateUnlocked || !status.Ref.Generated {
		return EnterResult{SlotID: slotID, Status: status}, nil
	}

	generated, err := generateUnlockPassphrase()
	if err != nil {
		return EnterResult{}, err
	}

	defer clear(generated)

	err = o.mutate(ctx, func(ring *keyring.Keyring) error {
		for {
			idx := ring.SlotIndex(SlotNameGenerated)
			if idx < 0 {
				break
			}

			ring.Slots = slices.Delete(ring.Slots, idx, idx+1)
		}

		_, addErr := ring.AddSlot(master, generated, SlotNameGenerated, keyring.OriginGenerated, o.now())

		return addErr //nolint:wrapcheck // keyring errors name the slot
	})
	if err != nil {
		return EnterResult{}, err
	}

	err = o.writeUnlockSecret(ctx, generated)
	if err != nil {
		return EnterResult{}, err
	}

	status, err = (&Unlocker{Secrets: o.Secrets}).Unlock(ctx)
	if err != nil {
		return EnterResult{}, err
	}

	return EnterResult{SlotID: slotID, Status: status, Repaired: true}, nil
}

// ModifyResult is what Modify did.
type ModifyResult struct {
	// Unchanged is a new passphrase equal to the old one.
	Unchanged bool

	// SecretToUpdate names the passphrase Secret that still holds the
	// old passphrase: the old slot stays, superseded, until the Secret
	// holds the new one.
	SecretToUpdate string
}

// Modify replaces the slot old opens with one for passphrase new, in one
// keyring update. When old is what the named passphrase Secret holds, the
// slot for old is kept, marked superseded, so the cluster still unlocks;
// the controller removes it once the Secret opens another slot.
func (o *Ops) Modify(ctx context.Context, oldPass, newPass []byte) (ModifyResult, error) {
	if bytes.Equal(oldPass, newPass) {
		_, err := o.verify(ctx, oldPass)

		return ModifyResult{Unchanged: err == nil}, err
	}

	status, err := (&Unlocker{Secrets: o.Secrets}).Unlock(ctx)
	if err != nil {
		return ModifyResult{}, err
	}

	if status.State == StateUnset {
		return ModifyResult{}, ErrNoKeyring
	}

	var result ModifyResult

	err = o.mutate(ctx, func(ring *keyring.Keyring) error {
		master, slotID, unlockErr := ring.Unlock(oldPass)
		if unlockErr != nil {
			return unlockErr //nolint:wrapcheck // keyring.ErrWrongPassphrase, said as it is
		}

		defer master.Destroy()

		old := ring.Slots[ring.SlotIndex(slotID)]
		isUnlock := status.State == StateUnlocked && slotID == status.SlotID

		switch {
		case isUnlock && status.Ref.Generated:
			return errors.Wrapf(ErrUnlockSlot, "slot %s is the generated unlock passphrase; it is not changed by hand", old.Name)
		case isUnlock:
			result.SecretToUpdate = status.Ref.Name

			supersedeErr := ring.Supersede(slotID)
			if supersedeErr != nil {
				return supersedeErr //nolint:wrapcheck // keyring errors name the slot
			}

			_, addErr := ring.AddSlot(master, newPass, freshName(ring, old.Name), old.Origin, o.now())

			return addErr //nolint:wrapcheck // keyring errors name the slot
		default:
			removeErr := ring.RemoveSlot(slotID, "")
			if removeErr != nil && !errors.Is(removeErr, keyring.ErrLastSlot) {
				return removeErr //nolint:wrapcheck // keyring errors name the slot
			}

			if removeErr != nil {
				// The only slot: add the new one first, then drop it.
				_, addErr := ring.AddSlot(master, newPass, freshName(ring, old.Name), old.Origin, o.now())
				if addErr != nil {
					return addErr //nolint:wrapcheck // keyring errors name the slot
				}

				return ring.RemoveSlot(slotID, "")
			}

			_, addErr := ring.AddSlot(master, newPass, old.Name, old.Origin, o.now())

			return addErr //nolint:wrapcheck // keyring errors name the slot
		}
	})

	return result, err
}

// Add adds a slot for newPass, named name, authorised by auth, which must
// open an existing slot.
func (o *Ops) Add(ctx context.Context, auth, newPass []byte, name string) (string, error) {
	var slotID string

	err := o.mutate(ctx, func(ring *keyring.Keyring) error {
		master, _, err := ring.Unlock(auth)
		if err != nil {
			return err //nolint:wrapcheck // keyring.ErrWrongPassphrase, said as it is
		}

		defer master.Destroy()

		slotID, err = ring.AddSlot(master, newPass, name, keyring.OriginAPI, o.now())

		return err //nolint:wrapcheck // keyring errors name the slot
	})

	return slotID, err
}

// Remove removes the slot ref names, by ID or name. It refuses the last
// slot and the slot the cluster unlocks with.
func (o *Ops) Remove(ctx context.Context, ref string) error {
	status, err := (&Unlocker{Secrets: o.Secrets}).Unlock(ctx)
	if err != nil {
		return err
	}

	if status.State == StateUnset {
		return ErrNoKeyring
	}

	return o.mutate(ctx, func(ring *keyring.Keyring) error {
		err := ring.RemoveSlot(ref, status.SlotID)
		if errors.Is(err, keyring.ErrProtectedSlot) {
			return errors.Wrapf(ErrUnlockSlot, "%q is opened by the passphrase Secret %s", ref, status.Ref.Name)
		}

		return err //nolint:wrapcheck // keyring errors name the slot
	})
}

// SlotInfo describes one slot, without key material.
type SlotInfo struct {
	ID, Name, Origin string
	CreatedAt        time.Time
	Superseded       bool
	KDF              string
	Unlocks          bool
}

// List describes the slots and marks the one the cluster unlocks with.
func (o *Ops) List(ctx context.Context) ([]SlotInfo, Status, error) {
	status, err := (&Unlocker{Secrets: o.Secrets}).Unlock(ctx)
	if err != nil || status.State == StateUnset {
		return nil, status, err
	}

	out := make([]SlotInfo, 0, len(status.Ring.Slots))

	for i := range status.Ring.Slots {
		slot := &status.Ring.Slots[i]
		out = append(out, SlotInfo{
			ID:         slot.ID,
			Name:       slot.Name,
			Origin:     slot.Origin,
			CreatedAt:  slot.CreatedAt,
			Superseded: slot.Superseded,
			KDF:        kdfSummary(&slot.KDF),
			Unlocks:    slot.ID == status.SlotID,
		})
	}

	return out, status, nil
}

// CleanupSuperseded removes the superseded slots other than the one the
// passphrase Secret opens, and reports whether it removed any: once the
// Secret holds the new passphrase, the old one's slot goes.
func (o *Ops) CleanupSuperseded(ctx context.Context) (bool, error) {
	status, err := (&Unlocker{Secrets: o.Secrets}).Unlock(ctx)
	if err != nil || status.State != StateUnlocked {
		return false, err
	}

	removed := false

	err = o.mutate(ctx, func(ring *keyring.Keyring) error {
		removed = ring.RemoveSuperseded(status.SlotID)
		if !removed {
			return errNothingToWrite
		}

		return nil
	})
	if errors.Is(err, errNothingToWrite) {
		return false, nil
	}

	return removed, err
}

var errNothingToWrite = errors.New("nothing to write")

// verify reports the slot pass opens.
func (o *Ops) verify(ctx context.Context, pass []byte) (string, error) {
	ring, _, err := o.Secrets.Keyring(ctx)
	if err != nil {
		return "", err
	}

	if ring == nil {
		return "", ErrNoKeyring
	}

	return ring.Opens(pass) //nolint:wrapcheck // keyring.ErrWrongPassphrase, said as it is
}

// mutate reads the keyring Secret, runs change on the keyring and writes it
// back under the resourceVersion it read, again on a conflict.
func (o *Ops) mutate(ctx context.Context, change func(*keyring.Keyring) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error { //nolint:wrapcheck // change's errors and the update's are wrapped inside
		var sec corev1.Secret

		err := o.Secrets.Reader.Get(ctx, client.ObjectKey{Namespace: o.Secrets.Namespace, Name: KeyringSecretName}, &sec)
		if apierrors.IsNotFound(err) {
			return ErrNoKeyring
		}

		if err != nil {
			return errors.Wrap(err, "get the keyring Secret")
		}

		ring, err := keyring.Parse(sec.Data[KeyringDataKey])
		if err != nil {
			return errors.Wrapf(err, "secret %s/%s", o.Secrets.Namespace, KeyringSecretName)
		}

		err = change(ring)
		if err != nil {
			return err
		}

		data, err := ring.Marshal()
		if err != nil {
			return err //nolint:wrapcheck // keyring errors name what is malformed
		}

		sec.Data[KeyringDataKey] = data

		err = o.Client.Update(ctx, &sec)
		if apierrors.IsConflict(err) {
			return err //nolint:wrapcheck // RetryOnConflict needs the conflict as it is
		}

		return errors.Wrap(err, "update the keyring Secret")
	})
}

// writeUnlockSecret creates or replaces the generated unlock Secret.
func (o *Ops) writeUnlockSecret(ctx context.Context, generated []byte) error {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: UnlockSecretName, Namespace: o.Secrets.Namespace},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{DefaultPassphraseKey: bytes.Clone(generated)},
	}

	err := o.Client.Create(ctx, sec)
	if !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "create the unlock Secret")
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error { //nolint:wrapcheck // the update's error is wrapped inside
		var cur corev1.Secret

		getErr := o.Secrets.Reader.Get(ctx, client.ObjectKeyFromObject(sec), &cur)
		if getErr != nil {
			return errors.Wrap(getErr, "get the unlock Secret")
		}

		cur.Data = sec.Data

		updateErr := o.Client.Update(ctx, &cur)
		if apierrors.IsConflict(updateErr) {
			return updateErr //nolint:wrapcheck // RetryOnConflict needs the conflict as it is
		}

		return errors.Wrap(updateErr, "update the unlock Secret")
	})
}

func (o *Ops) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}

	return time.Now()
}

// generateUnlockPassphrase is 32 random bytes as unpadded base64url.
func generateUnlockPassphrase() ([]byte, error) {
	var raw [generatedPassBytes]byte

	_, err := io.ReadFull(rand.Reader, raw[:])
	if err != nil {
		return nil, errors.Wrap(err, "read random unlock passphrase")
	}

	defer clear(raw[:])

	out := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(out, raw[:])

	return out, nil
}

// freshName is base, or base-2, base-3, ... whichever no slot has.
func freshName(ring *keyring.Keyring, base string) string {
	name := base

	for n := 2; ring.SlotIndex(name) >= 0; n++ {
		name = base + "-" + strconv.Itoa(n)
	}

	return name
}

func kdfSummary(kdf *keyring.KDF) string {
	return fmt.Sprintf("%s t=%d m=%dMiB p=%d", kdf.Type, kdf.Time, kdf.MemoryKiB/kibPerMiB, kdf.Parallelism)
}
