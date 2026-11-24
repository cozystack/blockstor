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

import "context"

// The objects a satellite owns on-disk state for carry a finalizer that only
// that satellite removes, after it has deleted the state. `node lost` is the
// one caller that may not wait for it: the node is gone, its satellite will
// never run again, and a plain delete would leave every one of its replicas
// and pools terminating forever. LINSTOR's `node lost` drops the node's
// objects from its database and leaves the storage on the node; these are
// that operation for a store whose objects have finalizers. A store without
// finalizers (the in-memory one) has nothing to release and does not
// implement them: a plain delete is the whole operation there.

// ReplicaAbandoner removes a replica without its satellite deleting its
// storage. The replica is marked first, so a satellite that is in fact still
// running tears down DRBD and leaves the storage alone, then deleted, then
// released from its satellite's finalizer.
type ReplicaAbandoner interface {
	AbandonReplica(ctx context.Context, rdName, node string) error
}

// PoolAbandoner removes a storage pool without its satellite: it is deleted,
// then released from its satellite's finalizer. Deleting a pool never touches
// the disk.
type PoolAbandoner interface {
	AbandonPool(ctx context.Context, node, pool string) error
}

// SnapshotNodeReleaser removes a node from every Snapshot it is listed on,
// leaving that node's copies on its disk: its hold is dropped, it leaves the
// list, and a Snapshot it was the last copy of is deleted. A Snapshot that
// still carries the old shared finalizer loses it too once every other node
// it lists holds its own, since the released node was the last one that had
// not.
type SnapshotNodeReleaser interface {
	ReleaseSnapshotsOnNode(ctx context.Context, node string) error
}
