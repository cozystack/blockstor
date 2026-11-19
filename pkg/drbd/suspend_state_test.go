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
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

const suspendStateCmd = "drbdsetup status pvc-1 --json"

func TestSuspendStateReadsTheKernelFlagRoleAndMinors(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect(suspendStateCmd, storage.FakeResponse{Stdout: []byte(
		`[{"name":"pvc-1","role":"Primary","suspended":true,"suspended-user":true,` +
			`"devices":[{"volume":0,"minor":1000},{"volume":1,"minor":1001}]}]`)})

	state, err := drbd.NewAdm(fx).SuspendState(t.Context(), "pvc-1")
	if err != nil {
		t.Fatal(err)
	}

	if !state.Loaded || !state.SuspendedUser || state.Role != "Primary" ||
		state.Minors[0] != 1000 || state.Minors[1] != 1001 {
		t.Errorf("state %+v", state)
	}
}

// An answer without the flag is unknown, never "not suspended": a
// resume must not be reported on a reading that says nothing.
func TestSuspendStateWithoutTheFlagIsUnknown(t *testing.T) {
	t.Parallel()

	for _, out := range []string{"", "[]", `[{"name":"pvc-1","role":"Secondary"}]`, "garbage"} {
		fx := storage.NewFakeExec()
		fx.Expect(suspendStateCmd, storage.FakeResponse{Stdout: []byte(out)})

		_, err := drbd.NewAdm(fx).SuspendState(t.Context(), "pvc-1")
		if !errors.Is(err, drbd.ErrSuspendStateUnknown) {
			t.Errorf("output %q: err %v, want ErrSuspendStateUnknown", out, err)
		}
	}
}

// A resource the kernel does not hold has nothing suspended.
func TestSuspendStateOfAnUnloadedResource(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect(suspendStateCmd, storage.FakeResponse{
		Stdout: []byte("pvc-1: No such resource\n"),
		Err:    storage.FakeExitError{Code: 10},
	})

	state, err := drbd.NewAdm(fx).SuspendState(t.Context(), "pvc-1")
	if err != nil || state.Loaded {
		t.Errorf("state %+v err %v, want unloaded", state, err)
	}
}

// One call reads every resource, so the watchdog's sweep for suspends
// nothing accounts for costs one exec however many resources there are.
func TestSuspendStatesReadsEveryResource(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --json", storage.FakeResponse{Stdout: []byte(
		`[{"name":"pvc-1","role":"Primary","suspended-user":true,"devices":[{"volume":0,"minor":1000}]},` +
			`{"name":"pvc-2","role":"Secondary","suspended-user":false,"devices":[{"volume":0,"minor":1001}]},` +
			`{"name":"pvc-3","role":"Secondary"}]`)})

	states, err := drbd.NewAdm(fx).SuspendStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if len(states) != 2 || !states["pvc-1"].SuspendedUser || states["pvc-1"].Minors[0] != 1000 ||
		states["pvc-2"].SuspendedUser {
		t.Errorf("states %+v", states)
	}
}

func TestSuspendStatesWithNoResources(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --json", storage.FakeResponse{
		Stdout: []byte("# No currently configured DRBD found.\n"),
		Err:    storage.FakeExitError{Code: 20},
	})

	states, err := drbd.NewAdm(fx).SuspendStates(t.Context())
	if err != nil || len(states) != 0 {
		t.Errorf("states %+v err %v, want none", states, err)
	}
}
