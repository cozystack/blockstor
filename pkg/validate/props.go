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

package validate

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// ErrImmutableProp refuses an edit to a property that names where a storage
// pool's data physically lives.
var ErrImmutableProp = errors.New("property is immutable after pool creation")

// Storage-pool properties that identify the backing store. Changing one does
// not migrate anything: the pool keeps its name and its replicas keep
// reporting UpToDate, while the driver is pointed at a different volume group,
// zpool or directory — or at one that does not exist. The data is not moved,
// it is simply no longer where the pool says it is.
const (
	PropStorPoolName = "StorDriver/StorPoolName"
	PropLvmVG        = "StorDriver/LvmVg"
	PropThinPool     = "StorDriver/ThinPool"
	PropZPool        = "StorDriver/ZPool"
	PropZPoolThin    = "StorDriver/ZPoolThin"
	PropFileDir      = "StorDriver/FileDir"
)

// ImmutableStoragePoolProps lists the properties no writer may change or
// remove once the pool exists.
func ImmutableStoragePoolProps() []string {
	return []string{
		PropStorPoolName,
		PropLvmVG,
		PropThinPool,
		PropZPool,
		PropZPoolThin,
		PropFileDir,
	}
}

// StoragePoolPropNamed refuses an operation that NAMES a backing-identity
// key, whatever value it carries.
//
// StoragePoolPropEdit catches a key whose value moved. It cannot catch an
// operator setting one to the value it already has, because no state changed
// — and the REST door refuses exactly that, since it inspects the requested
// operation rather than the result. Two doors answering the same question
// differently is the drift this package exists to remove, so the CLI asks
// both questions: this one about the operation, the other about the result.
func StoragePoolPropNamed(key string) error {
	if !slices.Contains(ImmutableStoragePoolProps(), key) {
		return nil
	}

	return fmt.Errorf("%w: %s", ErrImmutableProp, key)
}

// StoragePoolPropEdit compares a property bag before and after an edit and
// refuses the edit if it touched a backing-identity key.
//
// Comparing the two states rather than inspecting the requested operation is
// deliberate: set, delete and delete-namespace all reach the bag by different
// routes, and a guard written per operation misses whichever route is added
// next. A key that changed value, appeared, or vanished is refused the same
// way, whatever produced it.
func StoragePoolPropEdit(before, after map[string]string) error {
	touched := make([]string, 0, len(ImmutableStoragePoolProps()))

	for _, key := range ImmutableStoragePoolProps() {
		was, hadBefore := before[key]
		now, hasAfter := after[key]

		if hadBefore != hasAfter || was != now {
			touched = append(touched, key)
		}
	}

	if len(touched) == 0 {
		return nil
	}

	sort.Strings(touched)

	return fmt.Errorf("%w: %s", ErrImmutableProp, strings.Join(touched, ", "))
}
