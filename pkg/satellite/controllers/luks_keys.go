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
	"bytes"
	"context"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/keyring"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/passphrase"
	"github.com/cozystack/blockstor/pkg/satellite"
)

// errLUKSKeyRace is a replica that gained a password between the read
// that found none and the write that would have set one. The next
// reconcile reads the stored one.
var errLUKSKeyRace = errors.New("the replica gained a LUKS password concurrently; retrying with it")

// LUKSKeys is the satellite's satellite.LUKSKeys: it unlocks the master
// key from the passphrase Secret, unwraps the passwords stored on this
// node's replicas, and stores new ones.
//
// Every read goes past the cache. A password this satellite stored a
// moment ago must be the one it formats with, and a cache that trails
// the write would hand back none and have a second one generated.
type LUKSKeys struct {
	// Client writes the replica's Resource.
	Client client.Client

	// Reader reads the Resource, the Snapshot and the Secrets past the
	// cache.
	Reader client.Reader

	// Unlocker opens the master key; it is shared by every reconcile.
	Unlocker *passphrase.Unlocker
}

var _ satellite.LUKSKeys = (*LUKSKeys)(nil)

// Stored implements satellite.LUKSKeys.
func (k *LUKSKeys) Stored(ctx context.Context, vol satellite.LUKSVolume) ([]byte, error) {
	res, err := k.replica(ctx, vol)
	if err != nil {
		return nil, err
	}

	stored := luksKeyOf(res, vol.Volume)
	if stored == nil {
		return nil, errors.Wrapf(luks.ErrNoLUKSKey, "replica %s volume %d", res.Name, vol.Volume)
	}

	master, err := k.masterKey(ctx)
	if err != nil {
		return nil, err
	}

	return unwrap(master, stored, res.Name, vol.Volume)
}

// Generate implements satellite.LUKSKeys.
func (k *LUKSKeys) Generate(ctx context.Context, vol satellite.LUKSVolume) ([]byte, error) {
	master, err := k.masterKey(ctx)
	if err != nil {
		return nil, err
	}

	password, err := keyring.GeneratePassword()
	if err != nil {
		return nil, errors.Wrap(err, "generate a volume password")
	}

	wrapped, err := master.WrapVolumePassword(password)
	if err != nil {
		clear(password)

		return nil, errors.Wrap(err, "wrap the volume password")
	}

	err = k.store(ctx, vol, &blockstoriov1alpha1.LUKSKey{
		KeyID: wrapped.KeyID, Wrapped: wrapped.Wrapped, Origin: blockstoriov1alpha1.LUKSKeyOriginGenerated,
	})
	if err != nil {
		clear(password)

		return nil, err
	}

	return password, nil
}

// SnapshotCandidates implements satellite.LUKSKeys.
func (k *LUKSKeys) SnapshotCandidates(ctx context.Context, vol satellite.LUKSVolume) ([]satellite.LUKSCandidate, error) {
	srcRD, snapName, ok := strings.Cut(vol.SourceSnapshot, ":")
	if !ok || srcRD == "" || snapName == "" {
		return nil, errors.Wrapf(luks.ErrLUKSKeyMissing, "restore source %q is not <definition>:<snapshot>", vol.SourceSnapshot)
	}

	name, err := snapshotCRName(ctx, k.Reader, srcRD, snapName)
	if err != nil {
		return nil, err
	}

	var snap blockstoriov1alpha1.Snapshot

	err = k.Reader.Get(ctx, client.ObjectKey{Name: name}, &snap)
	if err != nil {
		return nil, errors.Wrapf(err, "get snapshot %s for its LUKS passwords", name)
	}

	master, err := k.masterKey(ctx)
	if err != nil {
		return nil, err
	}

	entries := snapshotEntriesFor(&snap, vol)

	var (
		out  []satellite.LUKSCandidate
		errs error
	)

	for i := range entries {
		stored := entries[i].LUKSKey

		password, unwrapErr := unwrap(master, &stored, name, vol.Volume)
		if unwrapErr != nil {
			errs = errors.CombineErrors(errs, unwrapErr)

			continue
		}

		out = append(out, satellite.LUKSCandidate{
			Node:     entries[i].NodeName,
			Password: password,
			Adopt:    func(ctx context.Context) error { return k.store(ctx, vol, stored.DeepCopy()) },
		})
	}

	if len(out) == 0 && errs != nil {
		return nil, errors.Wrapf(errs, "snapshot %s", name)
	}

	return out, nil
}

// snapshotEntriesFor is the snapshot's recorded passwords for the volume,
// this node's first: a volume restored from its own node's snapshot is
// the common case.
func snapshotEntriesFor(snap *blockstoriov1alpha1.Snapshot, vol satellite.LUKSVolume) []blockstoriov1alpha1.SnapshotLUKSKey {
	entries := make([]blockstoriov1alpha1.SnapshotLUKSKey, 0, len(snap.Spec.LUKSKeys))

	for _, entry := range snap.Spec.LUKSKeys {
		if entry.VolumeNumber == vol.Volume {
			entries = append(entries, entry)
		}
	}

	self := naming.Key(vol.Node)

	slices.SortStableFunc(entries, func(a, b blockstoriov1alpha1.SnapshotLUKSKey) int {
		return boolOrder(naming.Key(a.NodeName) != self, naming.Key(b.NodeName) != self)
	})

	return entries
}

// boolOrder sorts false before true.
func boolOrder(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	default:
		return -1
	}
}

func (k *LUKSKeys) masterKey(ctx context.Context) (*keyring.MasterKey, error) {
	status, err := k.Unlocker.Unlock(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unlock the master key")
	}

	if status.State != passphrase.StateUnlocked {
		return nil, errors.Wrapf(luks.ErrMasterKeyLocked, "encryption is %s; the passphrase Secret %s does not open %s",
			status.State, status.Ref.Name, passphrase.KeyringSecretName)
	}

	return status.Key, nil
}

func (k *LUKSKeys) replica(ctx context.Context, vol satellite.LUKSVolume) (*blockstoriov1alpha1.Resource, error) {
	name, err := replicaCRName(ctx, k.Reader, vol.Resource, vol.Node)
	if err != nil {
		return nil, err
	}

	var res blockstoriov1alpha1.Resource

	err = k.Reader.Get(ctx, client.ObjectKey{Name: name}, &res)
	if err != nil {
		return nil, errors.Wrapf(err, "get replica %s", name)
	}

	return &res, nil
}

// store sets the volume's wrapped password on the replica, under the
// resourceVersion it read, and reads it back: the caller formats or opens
// with the password only once it is known to be stored.
func (k *LUKSKeys) store(ctx context.Context, vol satellite.LUKSVolume, key *blockstoriov1alpha1.LUKSKey) error {
	res, err := k.replica(ctx, vol)
	if err != nil {
		return err
	}

	if luksKeyOf(res, vol.Volume) != nil {
		return errors.Wrapf(errLUKSKeyRace, "replica %s volume %d", res.Name, vol.Volume)
	}

	idx := slices.IndexFunc(res.Spec.Volumes, func(v blockstoriov1alpha1.ResourceVolumeSpec) bool {
		return v.VolumeNumber == vol.Volume
	})
	if idx < 0 {
		res.Spec.Volumes = append(res.Spec.Volumes, blockstoriov1alpha1.ResourceVolumeSpec{VolumeNumber: vol.Volume})
		idx = len(res.Spec.Volumes) - 1
	}

	res.Spec.Volumes[idx].LUKSKey = key

	err = k.Client.Update(ctx, res)
	if err != nil {
		return errors.Wrapf(err, "store the LUKS password of replica %s volume %d", res.Name, vol.Volume)
	}

	back, err := k.replica(ctx, vol)
	if err != nil {
		return err
	}

	got := luksKeyOf(back, vol.Volume)
	if got == nil || got.KeyID != key.KeyID || !bytes.Equal(got.Wrapped, key.Wrapped) {
		return errors.Wrapf(errLUKSKeyRace, "replica %s volume %d reads back another password", res.Name, vol.Volume)
	}

	return nil
}

func luksKeyOf(res *blockstoriov1alpha1.Resource, vol int32) *blockstoriov1alpha1.LUKSKey {
	for i := range res.Spec.Volumes {
		if res.Spec.Volumes[i].VolumeNumber == vol {
			return res.Spec.Volumes[i].LUKSKey
		}
	}

	return nil
}

func unwrap(master *keyring.MasterKey, stored *blockstoriov1alpha1.LUKSKey, owner string, vol int32) ([]byte, error) {
	password, err := master.UnwrapVolumePassword(keyring.WrappedKey{KeyID: stored.KeyID, Wrapped: stored.Wrapped})
	if errors.Is(err, keyring.ErrKeyIDMismatch) {
		return nil, errors.Wrapf(errors.WithSecondaryError(luks.ErrMasterKeyMismatch, err), "%s volume %d", owner, vol)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "%s volume %d", owner, vol)
	}

	return password, nil
}
