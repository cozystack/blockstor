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

import "reflect"

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
// The copy is made by walking the value rather than by round-tripping
// through JSON. A JSON copy is shorter to write and silently drops every
// field tagged `json:"-"`: PoolMissing on a storage pool is one, so a patch
// of an unrelated property would have cleared the flag the placer and the
// Faulty column read. Walking the value carries every field, exported or not
// to JSON, and replaces only what the mutator could reach through a shared
// reference.
func cloneForPatch[T any](in T) T {
	out := deepCopyValue(reflect.ValueOf(&in).Elem())

	return out.Interface().(T) //nolint:forcetypeassert // same type in, same type out
}

// deepCopyValue returns a value equal to v whose maps, slices and pointers
// are fresh, recursively. Everything else is copied by assignment, which is
// what the caller wants: an int or a string cannot be edited through a
// shared reference.
func deepCopyValue(val reflect.Value) reflect.Value {
	//nolint:exhaustive // the default arm is the point: value kinds copy by assignment
	switch val.Kind() {
	case reflect.Map:
		if val.IsNil() {
			return val
		}

		out := reflect.MakeMapWithSize(val.Type(), val.Len())
		for _, key := range val.MapKeys() {
			out.SetMapIndex(deepCopyValue(key), deepCopyValue(val.MapIndex(key)))
		}

		return out

	case reflect.Slice:
		if val.IsNil() {
			return val
		}

		out := reflect.MakeSlice(val.Type(), val.Len(), val.Cap())
		for i := range val.Len() {
			out.Index(i).Set(deepCopyValue(val.Index(i)))
		}

		return out

	case reflect.Pointer:
		if val.IsNil() {
			return val
		}

		out := reflect.New(val.Type().Elem())
		out.Elem().Set(deepCopyValue(val.Elem()))

		return out

	case reflect.Struct:
		out := reflect.New(val.Type()).Elem()
		for i := range val.NumField() {
			// An unexported field cannot be set through reflection. None
			// of the wire types carry one; if that changes, the field is
			// left at its zero value rather than silently aliasing, which
			// a test on the new type will catch.
			if !out.Field(i).CanSet() {
				continue
			}

			out.Field(i).Set(deepCopyValue(val.Field(i)))
		}

		return out

	case reflect.Interface:
		if val.IsNil() {
			return val
		}

		out := reflect.New(val.Type()).Elem()
		out.Set(deepCopyValue(val.Elem()))

		return out

	default:
		return val
	}
}
