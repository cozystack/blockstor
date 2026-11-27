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
	"context"
	"errors"
	"fmt"
	"slices"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// DeleteWhenEmptyProp marks a definition a client asked to delete once
// nothing is left on it (`delete_empty_resource_definition` on a truncate or
// a snapshot delete, REST 1.29) and that only the clone path's own snapshots
// kept. LINSTOR clones below its snapshot definitions, so the same request
// deletes the definition there at once; here the internal snapshot is a
// snapshot of the definition and has to outlive the clone, and nobody sends
// the request again once it is gone. The mark keeps the request until the
// definition is empty, and the definition controller finishes it.
const DeleteWhenEmptyProp = "Blockstor/DeleteWhenEmpty"

// DefinitionContents is what is left on a definition that a delete of it
// once it is empty has to know.
type DefinitionContents struct {
	// Empty: no replica that is not being deleted, and no snapshot.
	Empty bool

	// OnlyCloneSnapshots: not empty, and all that is left are snapshots
	// the clone path took for itself.
	OnlyCloneSnapshots bool
}

// ReadDefinitionContents reads, past the cache, what is left on rdName. A
// replica being deleted is gone for this question: its satellite only has
// the teardown left, which the definition delete waits on the same way.
func ReadDefinitionContents(ctx context.Context, st Store, rdName string) (DefinitionContents, error) {
	snaps, err := st.Snapshots().ListByDefinitionUncached(ctx, rdName)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return DefinitionContents{}, fmt.Errorf("list snapshots of %s: %w", rdName, err)
	}

	replicas, err := LiveReplicas(ctx, st, rdName)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return DefinitionContents{}, fmt.Errorf("list replicas of %s: %w", rdName, err)
	}

	live := slices.ContainsFunc(replicas, func(res apiv1.Resource) bool {
		return !slices.Contains(res.Flags, apiv1.ResourceFlagDelete)
	})

	switch {
	case !live && len(snaps) == 0:
		return DefinitionContents{Empty: true}, nil
	case live:
		return DefinitionContents{}, nil
	default:
		return DefinitionContents{OnlyCloneSnapshots: !slices.ContainsFunc(snaps, func(snap apiv1.Snapshot) bool {
			return snap.Props[CloneSnapshotOwnerProp] == ""
		})}, nil
	}
}

// SetDeleteWhenEmpty puts DeleteWhenEmptyProp on rdName, or takes it off.
func SetDeleteWhenEmpty(ctx context.Context, st Store, rdName string, set bool) error {
	//nolint:wrapcheck // the callers tell ErrNotFound from the rest
	return st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(rd *apiv1.ResourceDefinition) error {
			if !set {
				delete(rd.Props, DeleteWhenEmptyProp)

				return nil
			}

			if rd.Props == nil {
				rd.Props = map[string]string{}
			}

			rd.Props[DeleteWhenEmptyProp] = "true"

			return nil
		})
}

// ClaimEmptyDefinition marks rdName DELETE when it is empty, so the caller
// can delete it: claimed reports that it may. The contents read are
// returned either way.
//
// LINSTOR checks and deletes under one lock, so a replica or a snapshot
// created meanwhile keeps the definition. Here the definition is first
// marked DELETE, which a snapshot create checks after its own write and
// rolls back on, and then checked again past the cache: a snapshot that
// lands before the mark is seen by the second check, which takes the mark
// off and keeps the definition, and one that lands after it rolls itself
// back.
func ClaimEmptyDefinition(ctx context.Context, st Store, rdName string) (DefinitionContents, bool, error) {
	contents, err := ReadDefinitionContents(ctx, st, rdName)
	if err != nil || !contents.Empty {
		return contents, false, err
	}

	err = MarkDefinitionDeleting(ctx, st, rdName)
	if err != nil {
		return contents, false, err
	}

	contents, err = ReadDefinitionContents(ctx, st, rdName)
	if err == nil && contents.Empty {
		return contents, true, nil
	}

	unmarkErr := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName,
		func(rd *apiv1.ResourceDefinition) error {
			rd.Flags = slices.DeleteFunc(rd.Flags, func(flag string) bool { return flag == apiv1.ResourceFlagDelete })

			return nil
		})
	if errors.Is(unmarkErr, ErrNotFound) {
		unmarkErr = nil
	}

	if err != nil || unmarkErr != nil {
		return contents, false, fmt.Errorf("keep %s: %w", rdName, errors.Join(err, unmarkErr))
	}

	return contents, false, nil
}

// DeleteClaimedDefinition deletes a definition ClaimEmptyDefinition claimed,
// and then the internal snapshot its own clone left on its source, which
// nothing else reaps (see ReapClonedSnapshot). It has no replica and no
// snapshot, so the definition is all there is to delete. A definition
// already gone is not an error.
func DeleteClaimedDefinition(ctx context.Context, st Store, rdName string) error {
	clonedFrom, err := OwnedCloneSnapshot(ctx, st, rdName)
	if err != nil {
		return err
	}

	err = st.ResourceDefinitions().Delete(ctx, rdName)
	if errors.Is(err, ErrNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("delete %s: %w", rdName, err)
	}

	err = ReapClonedSnapshot(ctx, st, clonedFrom)
	if err != nil {
		return fmt.Errorf("%s deleted, internal clone snapshot kept: %w", rdName, err)
	}

	return nil
}
