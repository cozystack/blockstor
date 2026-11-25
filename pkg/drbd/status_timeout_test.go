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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// errKilled is what a drbdsetup killed by its context's end returns.
var errKilled = errors.New("signal: killed")

// wedgedStatusExec answers every command but `drbdsetup status`, which
// blocks until its context ends, as a netlink dump that reaches a DRBD
// resource whose receivers deadlocked on its bitmap lock does.
type wedgedStatusExec struct{}

func (wedgedStatusExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return wedgedStatusExec{}.RunWithStdin(ctx, nil, name, args...)
}

func (wedgedStatusExec) RunWithStdin(ctx context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	if name != "drbdsetup" || len(args) == 0 || args[0] != "status" {
		return nil, nil
	}

	<-ctx.Done()

	return nil, errKilled
}

// TestStatusOfAWedgedKernelFailsInsteadOfHanging: every query the
// satellite makes through `drbdsetup status`, of one resource or of all,
// gives up after the status timeout with ErrStatusTimeout. On a node
// whose DRBD netlink dumps stopped answering, the satellite ran no
// resource pass for half an hour; bounded, the pass fails, says why, and
// the next one runs.
func TestStatusOfAWedgedKernelFailsInsteadOfHanging(t *testing.T) {
	t.Parallel()

	adm := drbd.NewAdm(wedgedStatusExec{})
	adm.SetStatusTimeout(20 * time.Millisecond)

	for name, query := range map[string]func(context.Context) error{
		"every resource": func(ctx context.Context) error {
			_, err := adm.StatusResources(ctx)

			return fmt.Errorf("every resource: %w", err)
		},
		"suspend states": func(ctx context.Context) error {
			_, err := adm.SuspendStates(ctx)

			return fmt.Errorf("suspend states: %w", err)
		},
		"one resource": func(ctx context.Context) error {
			_, err := adm.SuspendState(ctx, "pvc-a")

			return fmt.Errorf("one resource: %w", err)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			done := make(chan error, 1)

			go func() { done <- query(t.Context()) }()

			select {
			case err := <-done:
				if !errors.Is(err, drbd.ErrStatusTimeout) {
					t.Errorf("got %v, want ErrStatusTimeout", err)
				}

				if err != nil && !strings.Contains(err.Error(), "drbdsetup status") {
					t.Errorf("error %q does not name the command", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("drbdsetup status still waiting on a wedged kernel after 5s")
			}
		})
	}
}

// TestStatusKeepsTheCallersDeadline: a query cut off by its caller's own
// deadline, shorter than the status timeout, is not reported as a wedged
// kernel.
func TestStatusKeepsTheCallersDeadline(t *testing.T) {
	t.Parallel()

	adm := drbd.NewAdm(wedgedStatusExec{})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err := adm.StatusResources(ctx)
	if err == nil || errors.Is(err, drbd.ErrStatusTimeout) {
		t.Errorf("got %v, want the caller's deadline", err)
	}
}
