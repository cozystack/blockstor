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

package store

import (
	"encoding/json"

	"github.com/cockroachdb/errors"
)

// cloneForPatch returns a copy a mutator may edit freely, so a mutator that
// fails changes nothing.
//
// The in-memory stores hand their patch mutators a struct copy, which reads
// as safe and is not: a struct copy is shallow, so every map and slice in it
// still addresses the stored object's memory. A mutator that writes a
// property and then returns an error leaves that property written, even
// though the store never assigns the struct back. The rollback is only
// apparent.
//
// That matters beyond tidiness. The patch mutator is where refusals live —
// an immutable backing-store key, an attach that would wipe a claimed disk —
// and a refusal that has already applied half its edit is not a refusal.
//
// The round trip through JSON is not the fastest way to deep-copy, but every
// type here is a JSON-tagged wire type, so it is correct for all of them
// without seven hand-written copiers to keep in step with seven structs. The
// in-memory store backs tests and local runs, not a hot path.
func cloneForPatch[T any](in T) (T, error) {
	var out T

	raw, err := json.Marshal(in)
	if err != nil {
		return out, errors.Wrap(err, "clone for patch")
	}

	err = json.Unmarshal(raw, &out)
	if err != nil {
		return out, errors.Wrap(err, "clone for patch")
	}

	return out, nil
}
