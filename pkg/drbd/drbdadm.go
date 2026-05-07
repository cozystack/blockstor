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

package drbd

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Adm is a thin wrapper around the `drbdadm` CLI. It exists so the
// satellite reconciler can be unit-tested without a real DRBD kernel
// module present: production injects storage.RealExec, tests inject
// storage.FakeExec and assert the exact command lines.
type Adm struct {
	exec storage.Exec
}

// NewAdm constructs an Adm with the given Exec.
func NewAdm(ex storage.Exec) *Adm {
	return &Adm{exec: ex}
}

// Up activates the resource: `drbdadm up <res>`. Idempotent on the DRBD
// side (already-up resources return 0 with a noisy warning); we don't
// try to suppress that here.
func (a *Adm) Up(ctx context.Context, resource string) error {
	return a.run(ctx, "up", resource)
}

// Down deactivates the resource: `drbdadm down <res>`. Counterpart to Up.
func (a *Adm) Down(ctx context.Context, resource string) error {
	return a.run(ctx, "down", resource)
}

// Adjust reconciles kernel state to the on-disk .res file. Called after
// the ConfFileBuilder writes a new file and we need DRBD to pick up
// changes (added/removed peers, new options).
func (a *Adm) Adjust(ctx context.Context, resource string) error {
	return a.run(ctx, "adjust", resource)
}

// CreateMD initialises on-disk metadata for the resource. We always use
// --force: a freshly-allocated LV may carry leftover signature bytes
// from its previous tenant, and DRBD bails without --force.
func (a *Adm) CreateMD(ctx context.Context, resource string) error {
	return a.run(ctx, "create-md", "--force", resource)
}

// Primary flips the resource to Primary role so it can be opened
// read-write (mounted, exported via NBD, etc.).
func (a *Adm) Primary(ctx context.Context, resource string) error {
	return a.run(ctx, "primary", resource)
}

// Secondary flips the resource back to Secondary role. Used after the
// consumer unmounts and before another peer takes Primary.
func (a *Adm) Secondary(ctx context.Context, resource string) error {
	return a.run(ctx, "secondary", resource)
}

// run is the single shell-out site so every drbdadm error gets
// uniform context (subcommand + resource) for log triage.
func (a *Adm) run(ctx context.Context, args ...string) error {
	_, err := a.exec.Run(ctx, "drbdadm", args...)
	if err != nil {
		return errors.Wrapf(err, "drbdadm %s", args[0])
	}

	return nil
}
