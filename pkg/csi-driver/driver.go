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

// Package csidriver is the thin shim layer that maps incoming CSI
// gRPC requests (csi.proto) onto blockstor's REST surface via
// golinstor's lapi client.
//
// Today the upstream linstor-csi binary out-of-tree owns the
// container-storage-interface/spec wiring (csi.proto stubs, gRPC
// server bootstrap, csi-sanity acceptance). We do NOT vendor the
// CSI proto stubs here on purpose:
//
//   - csi.proto types (csi.CreateSnapshotRequest, etc.) live in
//     `github.com/container-storage-interface/spec/lib/go/csi`,
//     which we do not import yet — blockstor is the backend, not
//     the CSI sidecar.
//   - We still need to pin the BEHAVIOURAL CONTRACT each CSI RPC
//     expects from blockstor's REST surface, so a refactor of the
//     REST layer cannot silently break linstor-csi.
//
// This package defines a minimal mirror of the two CSI request
// shapes that scenarios 11.W08 and 11.W09 exercise:
//
//   - CreateSnapshot      → POST /v1/resource-definitions/{rd}/snapshots
//   - CreateVolume with
//     ContentSource.Snapshot →
//     POST /v1/resource-definitions/{rd}/snapshot-restore-resource
//
// The Driver type drives both via golinstor's lapi client, exactly
// like linstor-csi does in production. Tests in this package boot a
// real in-memory REST server (`pkg/rest`) and assert that the
// resulting wire calls behave as the CSI sidecar requires (idempotent
// retries, ContentSource.SnapshotId propagation, size validation).
package csidriver

import (
	"context"
	"errors"
	"fmt"
	"math"

	lapi "github.com/LINBIT/golinstor/client"
)

// Sentinel validation errors. err113 mandates static errors so
// callers (and tests) can match with errors.Is rather than
// string-matching a Sprintf payload.
var (
	// ErrNilRequest is returned when a CSI RPC arrives with a nil
	// proto message. csi-sanity's marshal layer can produce this
	// shape in fuzz mode.
	ErrNilRequest = errors.New("csi driver: nil request")

	// ErrMissingSourceVolume is csi-sanity's
	// "CreateSnapshot should fail when the source volume is not
	// specified" path.
	ErrMissingSourceVolume = errors.New("csi driver: SourceVolumeID is required")

	// ErrMissingName covers both
	// "CreateSnapshot should fail when the name field is missing"
	// and the equivalent CreateVolume case.
	ErrMissingName = errors.New("csi driver: Name is required")

	// ErrMissingContentSource is the guard rail for this shim:
	// only the snapshot-restore branch is modelled here.
	ErrMissingContentSource = errors.New("csi driver: ContentSource is required (snapshot-restore branch)")

	// ErrMalformedContentSource is the wave1 contract: linstor-csi
	// encodes snapshot ids as `<rd>/<snap>` — both halves must be
	// non-empty.
	ErrMalformedContentSource = errors.New("csi driver: ContentSource.{SourceRD,SnapshotName} required")

	// ErrCapacityBelowSnapshot is the CSI-spec size guard: a new
	// volume restored from a snapshot must be at least as large as
	// the snapshot.
	ErrCapacityBelowSnapshot = errors.New("csi driver: requested capacity below snapshot size (CSI spec: clone must be >= source snapshot)")
)

// Driver is the thin CSI-to-blockstor adapter. It owns one
// golinstor REST client. The real linstor-csi binary wraps this
// same client in a `csi.ControllerServer` impl; we expose the
// behaviour-bearing methods directly so unit tests don't need a
// full gRPC roundtrip.
type Driver struct {
	Client *lapi.Client
}

// CreateSnapshotRequest mirrors the load-bearing fields of
// csi.CreateSnapshotRequest. The CSI sidecar fills:
//
//   - SourceVolumeID — blockstor RD name (CSI volume_id == RD name)
//   - Name           — caller-chosen snapshot name (idempotency key)
//
// `Parameters` is reserved for VolumeSnapshotClass parameters;
// linstor-csi forwards them as RD/snap props but blockstor does
// not interpret them here.
type CreateSnapshotRequest struct {
	SourceVolumeID string
	Name           string
	Parameters     map[string]string
}

// CreateSnapshotResponse mirrors csi.CreateSnapshotResponse.Snapshot.
// `ReadyToUse` is what csi-snapshotter watches to mark the
// VolumeSnapshot object `readyToUse=true`. blockstor's REST
// CreateSnapshot returns success before the satellite actually
// took the snapshot, so this shim reports `ReadyToUse=false` and
// relies on a follow-up ListSnapshots / GetSnapshot poll, exactly
// as upstream linstor-csi does.
type CreateSnapshotResponse struct {
	SnapshotID     string
	SourceVolumeID string
	SizeBytes      int64
	ReadyToUse     bool
}

// VolumeContentSourceSnapshot mirrors csi.VolumeContentSource_SnapshotSource.
// The SnapshotID encodes both the source RD and the snapshot name
// in linstor-csi's `rd/snap` form. We accept it pre-parsed for
// clarity in tests; the real driver does the split.
type VolumeContentSourceSnapshot struct {
	SourceRD     string
	SnapshotName string
}

// CreateVolumeRequest mirrors the load-bearing fields of
// csi.CreateVolumeRequest. Only the snapshot-restore path is
// modelled here — the empty-volume create path is wave1 work.
type CreateVolumeRequest struct {
	Name             string
	CapacityRangeMin int64
	CapacityRangeMax int64
	ContentSource    *VolumeContentSourceSnapshot
	Parameters       map[string]string
}

// CreateVolumeResponse mirrors csi.CreateVolumeResponse.Volume.
type CreateVolumeResponse struct {
	VolumeID      string
	CapacityBytes int64
	ContentSource *VolumeContentSourceSnapshot
}

// CreateSnapshot implements csi.ControllerServer.CreateSnapshot
// against blockstor's REST surface. The CSI spec mandates
// idempotent retries: the same (SourceVolumeID, Name) tuple must
// return the same snapshot id without erroring. blockstor's
// snapshot handler already enforces this (see
// pkg/rest/snapshots.go: returns 200 + maskInfo on re-create).
func (d *Driver) CreateSnapshot(ctx context.Context, req *CreateSnapshotRequest) (*CreateSnapshotResponse, error) {
	if req == nil {
		return nil, ErrNilRequest
	}

	if req.SourceVolumeID == "" {
		return nil, ErrMissingSourceVolume
	}

	if req.Name == "" {
		return nil, ErrMissingName
	}

	snap := lapi.Snapshot{
		Name:         req.Name,
		ResourceName: req.SourceVolumeID,
	}

	err := d.Client.Resources.CreateSnapshot(ctx, snap)
	if err != nil {
		return nil, fmt.Errorf("CreateSnapshot %s/%s: %w", req.SourceVolumeID, req.Name, err)
	}

	// Snapshot id is the CSI-side handle: linstor-csi encodes it as
	// `<rd>/<snap>` and decodes back on the restore call.
	return &CreateSnapshotResponse{
		SnapshotID:     req.SourceVolumeID + "/" + req.Name,
		SourceVolumeID: req.SourceVolumeID,
		// blockstor's REST shim returns synchronously before the
		// satellite reconcile fires. csi-snapshotter polls
		// ListSnapshots until ReadyToUse flips true (see 8.W03).
		ReadyToUse: false,
	}, nil
}

// CreateVolume implements the snapshot-restore branch of
// csi.ControllerServer.CreateVolume. Maps onto blockstor's
// `snapshot-restore-resource` endpoint (wave2-08 8.W03).
//
// The CSI spec requires the new volume size to be at least the
// snapshot size — csi-snapshotter validates the upper bound,
// but the driver is responsible for rejecting a too-small
// CapacityRangeMin. We surface that as an early error so a
// regression in the REST layer does not let a too-small PVC
// silently truncate the clone.
func (d *Driver) CreateVolume(ctx context.Context, req *CreateVolumeRequest) (*CreateVolumeResponse, error) {
	if req == nil {
		return nil, ErrNilRequest
	}

	if req.Name == "" {
		return nil, ErrMissingName
	}

	if req.ContentSource == nil {
		return nil, ErrMissingContentSource
	}

	if req.ContentSource.SourceRD == "" || req.ContentSource.SnapshotName == "" {
		return nil, ErrMalformedContentSource
	}

	// Size guard: CSI driver layer's job per spec.
	// We fetch the snapshot and compare its size against
	// CapacityRangeMin before calling restore so blockstor never
	// sees a request that would violate the size contract.
	srcRD := req.ContentSource.SourceRD
	snapName := req.ContentSource.SnapshotName

	snap, err := d.Client.Resources.GetSnapshot(ctx, srcRD, snapName)
	if err != nil {
		return nil, fmt.Errorf("CreateVolume: GetSnapshot %s/%s: %w", srcRD, snapName, err)
	}

	snapSizeBytes := snapshotSizeBytes(&snap)

	if req.CapacityRangeMin > 0 && req.CapacityRangeMin < snapSizeBytes {
		return nil, fmt.Errorf(
			"%w: requested=%d snapshot=%d",
			ErrCapacityBelowSnapshot, req.CapacityRangeMin, snapSizeBytes)
	}

	err = d.Client.Resources.RestoreSnapshot(ctx, srcRD, snapName, lapi.SnapshotRestore{
		ToResource: req.Name,
	})
	if err != nil {
		return nil, fmt.Errorf("CreateVolume: RestoreSnapshot %s/%s → %s: %w", srcRD, snapName, req.Name, err)
	}

	return &CreateVolumeResponse{
		VolumeID:      req.Name,
		CapacityBytes: snapSizeBytes,
		ContentSource: req.ContentSource,
	}, nil
}

// bytesPerKiB is the unit conversion factor between LINSTOR's wire
// size encoding (KiB on `SizeKib` fields) and the CSI spec's
// byte-denominated CapacityBytes. Named to keep `mnd` happy and
// readable at every call site.
const bytesPerKiB int64 = 1024

// snapshotSizeBytes returns the snapshot's first-volume size in
// bytes. blockstor encodes sizes as KiB (`SizeKib`); the CSI spec
// works in bytes. A snapshot with no VolumeDefinitions is treated
// as size 0 so the size check above degrades to a no-op rather
// than panicking on a malformed reply.
//
// golinstor's wire type for `SizeKib` is uint64 (it cannot represent
// negative sizes); blockstor's own DTO + CSI's CapacityBytes are
// int64. We clamp at math.MaxInt64 — a real snapshot that overflows
// 8 EiB does not exist on any storage backend blockstor supports,
// so the clamp is defensive against a malformed/fuzz response.
func snapshotSizeBytes(snap *lapi.Snapshot) int64 {
	if snap == nil || len(snap.VolumeDefinitions) == 0 {
		return 0
	}

	kib := snap.VolumeDefinitions[0].SizeKib
	if kib > uint64(math.MaxInt64)/uint64(bytesPerKiB) {
		return math.MaxInt64
	}

	return int64(kib) * bytesPerKiB
}
