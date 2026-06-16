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

package drbd_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// AttachedVolumes is the kernel-truth backing for the BUG-048 resize-deadlock
// gate: it returns the LOCAL volumes that are present-and-attached (have a
// valid DRBD-9 metadata block — disk-state UpToDate / Inconsistent /
// Consistent / Outdated). A volume absent from the device list, or Diskless /
// mid-transition, is excluded so the reconciler still runs the metadata pass
// for a genuine late-add but never for a converged/resizing RD.

func attachedVolumesAdm(t *testing.T, json string) *drbd.Adm {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Responses["drbdsetup status pvc-av --json"] = storage.FakeResponse{Stdout: []byte(json)}

	return drbd.NewAdm(fx)
}

func assertAttachedSet(t *testing.T, got map[int32]struct{}, want ...int32) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("attached set size = %d, want %d (got %v, want %v)", len(got), len(want), got, want)
	}

	for _, v := range want {
		if _, ok := got[v]; !ok {
			t.Errorf("expected volume %d in attached set %v", v, got)
		}
	}
}

// Every metadata-bearing disk-state counts as attached. Diskless and
// transient states do NOT — those are volumes that still need the metadata
// pass (a genuine late-add or a re-attach).
func TestAttachedVolumes_OnlyMetadataBearingStatesCount(t *testing.T) {
	adm := attachedVolumesAdm(t, `[{
	  "name":"pvc-av","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"Inconsistent"},
	    {"volume":2,"disk-state":"Consistent"},
	    {"volume":3,"disk-state":"Outdated"},
	    {"volume":4,"disk-state":"Diskless"},
	    {"volume":5,"disk-state":"Negotiating"},
	    {"volume":6,"disk-state":"DUnknown"}
	  ]
	}]`)

	// 0..3 are attached (metadata-bearing); 4 (Diskless), 5 (Negotiating),
	// 6 (DUnknown) are not.
	assertAttachedSet(t, adm.AttachedVolumes(t.Context(), "pvc-av"), 0, 1, 2, 3)
}

// A volume entirely absent from the kernel device list (the genuine late-add
// window — `vd c` added it to the desired set but the kernel has not brought
// it up yet) is simply not in the set, so the reconciler sees it as
// unattached and runs the metadata pass for it.
func TestAttachedVolumes_AbsentVolumeNotInSet(t *testing.T) {
	adm := attachedVolumesAdm(t, `[{
	  "name":"pvc-av","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"}
	  ]
	}]`)

	got := adm.AttachedVolumes(t.Context(), "pvc-av")
	assertAttachedSet(t, got, 0)

	if _, ok := got[1]; ok {
		t.Errorf("volume 1 is absent from the device list and must NOT be reported attached; got %v", got)
	}
}

// Conservative toward FIRING (fail-toward-correctness): on a probe failure,
// empty output, or an unloaded slot the set is empty, so the reconciler treats
// every desired volume as possibly-unattached and runs the idempotent metadata
// pass — the pre-#164 behaviour. The deadlock-relevant skip only happens once
// the kernel positively reports volumes attached.
func TestAttachedVolumes_ConservativeEmptyOnNoKernelSlot(t *testing.T) {
	fx := storage.NewFakeExec()
	// No registered response → FakeExec returns empty stdout, nil error
	// (the "kernel slot present but status empty" / blank-output shape).
	adm := drbd.NewAdm(fx)

	if got := adm.AttachedVolumes(t.Context(), "pvc-missing"); len(got) != 0 {
		t.Errorf("expected empty attached set on no kernel data; got %v", got)
	}
}
