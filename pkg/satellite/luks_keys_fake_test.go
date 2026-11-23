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

package satellite_test

import (
	"bytes"
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
)

// fakeLUKSKeys is an in-memory satellite.LUKSKeys. Every volume reads
// back the password stored for it; a generated one is "generated-<n>".
type fakeLUKSKeys struct {
	mu sync.Mutex

	// stored maps a volume to its password; a volume without one has
	// none.
	stored map[luksVol][]byte

	// all, when set, is every volume's stored password.
	all []byte

	// err, when set, is what Stored and Generate fail with.
	err error

	// generateErr, when set, is what Generate fails with: the store of
	// the new password did not go through.
	generateErr error

	// candidates are what SnapshotCandidates returns.
	candidates map[string][]byte

	// events records the calls in order: "stored", "generate",
	// "candidates", "adopt <node>".
	events []string
}

var errStoreRefused = errors.New("the API server refused the update")

type luksVol struct {
	resource string
	volume   int32
}

var _ satellite.LUKSKeys = (*fakeLUKSKeys)(nil)

// storedLUKSKey is a provider whose every volume carries password.
func storedLUKSKey(password string) *fakeLUKSKeys {
	return &fakeLUKSKeys{all: []byte(password)}
}

func (f *fakeLUKSKeys) Stored(_ context.Context, vol satellite.LUKSVolume) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.events = append(f.events, "stored")

	if f.err != nil {
		return nil, f.err
	}

	if f.all != nil {
		return bytes.Clone(f.all), nil
	}

	if pw, ok := f.stored[luksVol{vol.Resource, vol.Volume}]; ok {
		return bytes.Clone(pw), nil
	}

	return nil, errors.Wrap(luks.ErrNoLUKSKey, "fake")
}

func (f *fakeLUKSKeys) Generate(_ context.Context, vol satellite.LUKSVolume) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.events = append(f.events, "generate")

	if f.err != nil {
		return nil, f.err
	}

	if f.generateErr != nil {
		return nil, f.generateErr
	}

	if f.stored == nil {
		f.stored = map[luksVol][]byte{}
	}

	pw := []byte("generated-" + string(rune('0'+len(f.stored))))
	f.stored[luksVol{vol.Resource, vol.Volume}] = pw

	return bytes.Clone(pw), nil
}

func (f *fakeLUKSKeys) SnapshotCandidates(_ context.Context, vol satellite.LUKSVolume) ([]satellite.LUKSCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.events = append(f.events, "candidates")

	out := make([]satellite.LUKSCandidate, 0, len(f.candidates))

	for _, node := range []string{vol.Node, "peer-a", "peer-b"} {
		pw, ok := f.candidates[node]
		if !ok {
			continue
		}

		out = append(out, satellite.LUKSCandidate{
			Node:     node,
			Password: bytes.Clone(pw),
			Adopt: func(context.Context) error {
				f.mu.Lock()
				defer f.mu.Unlock()

				f.events = append(f.events, "adopt "+node)

				if f.stored == nil {
					f.stored = map[luksVol][]byte{}
				}

				f.stored[luksVol{vol.Resource, vol.Volume}] = bytes.Clone(pw)

				return nil
			},
		})
	}

	return out, nil
}

func (f *fakeLUKSKeys) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.events...)
}
