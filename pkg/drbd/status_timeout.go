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

package drbd

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

// DefaultStatusTimeout bounds one `drbdsetup status` call. A status of
// every resource on a node with a thousand of them answers in well under
// a second; one that has not answered in this long is not going to.
const DefaultStatusTimeout = 30 * time.Second

// ErrStatusTimeout reports a `drbdsetup status` the kernel did not answer
// within the Adm's status timeout. DRBD serves status from netlink dumps
// that take the locks of the resources they walk, and two receivers of
// one resource can deadlock on its bitmap lock: every dump that reaches
// that resource then waits for good, a dump of all resources included,
// while every other resource keeps serving I/O. Unbounded, one such
// resource stalls whatever asked, the apply of unrelated resources and
// the sweep among them.
var ErrStatusTimeout = errors.New("drbdsetup status did not answer: the kernel's DRBD state may be wedged")

// SetStatusTimeout changes how long one `drbdsetup status` call may take;
// zero or less restores DefaultStatusTimeout.
func (a *Adm) SetStatusTimeout(d time.Duration) {
	a.statusTimeout = d
}

// status runs `drbdsetup status <args>`, bounded by the status timeout.
// A call cut off by the bound fails with ErrStatusTimeout; one the
// caller's own context ended keeps that error.
func (a *Adm) status(ctx context.Context, args ...string) ([]byte, error) {
	timeout := a.statusTimeout
	if timeout <= 0 {
		timeout = DefaultStatusTimeout
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	out, err := a.exec.Run(callCtx, "drbdsetup", append([]string{"status"}, args...)...)
	if err != nil && ctx.Err() == nil && errors.Is(callCtx.Err(), context.DeadlineExceeded) {
		command := strings.TrimSpace("drbdsetup status " + strings.Join(args, " "))

		return out, errors.Wrapf(ErrStatusTimeout, "%s: no answer within %s", command, timeout)
	}

	return out, err //nolint:wrapcheck // callers wrap with their own context
}
