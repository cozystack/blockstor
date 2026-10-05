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

import "maps"

// RollbackAbandonedProp marks a resource definition whose compensation started
// and did not report finishing, with the step it stopped at. The replay gate of
// the clone path refuses over it.
const RollbackAbandonedProp = "BlockstorRollbackAbandoned"

// restoreFromSnapshotMarker is the `<source>:<snapshot>` a restored or cloned
// definition carries; see TravellingProps for why it never travels.
const restoreFromSnapshotMarker = "BlockstorRestoreFromSnapshot"

// TravellingProps copies a props bag for use on another object, leaving out
// the props that describe the object they were read from and nothing derived
// from it. Nil stays nil.
//
// A definition's props are copied forward wholesale: onto every snapshot taken
// of it, and from a snapshot, or from the source when the snapshot has none,
// onto every definition restored or cloned from it. A mark that records
// something about THIS definition, read on a copy, says it about a definition
// it was never true of: a healthy clone of a marked source was refused on the
// replay linstor-csi sends, and the correction said to delete it.
//
// The restore marker is the other member of the class: it records where THIS
// definition's data came from, and the dispatcher, the placer and autoplace
// all read it off whatever definition carries it. A volume-less clone of a
// restored definition kept the source's marker, so the first volume added to
// it later was restored from somebody else's snapshot, on that snapshot's
// nodes. Every restore door stamps its own marker after the copy, and nothing
// reads the marker off a snapshot.
func TravellingProps(props map[string]string) map[string]string {
	out := maps.Clone(props)
	delete(out, RollbackAbandonedProp)
	delete(out, restoreFromSnapshotMarker)

	return out
}
