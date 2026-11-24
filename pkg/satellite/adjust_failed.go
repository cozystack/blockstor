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

package satellite

import (
	"context"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// adjustFailedSuffix names the marker file beside a resource's .res
// that says its last adjust failed.
const adjustFailedSuffix = ".adjust-failed"

// adjustFailedFileMode is the mode of that marker, an empty file.
const adjustFailedFileMode = 0o600

// adjustFailedBefore reports whether the last adjust of the resource
// failed and no full adjust has succeeded since.
//
// shouldSkipNetOnAdjust reads a StandAlone connection that has peer
// devices as an operator's disconnect, to be kept. A failed adjust
// leaves exactly that behind: drbdadm creates the connections and their
// peer devices before the step that failed, a new-path refused with
// "(103) Remote address(port) already in use" say, and never reaches
// connect. The retry then ran with --skip-net, succeeded without
// touching the network, and the replica stayed StandAlone for good,
// even after the conflict was gone. LINSTOR never passes --skip-net:
// every retry of a failed resource is a full adjust. So after a failed
// adjust this satellite does the same until one goes through.
func (r *Reconciler) adjustFailedBefore(name string) bool {
	r.mu.Lock()
	failed := r.adjustFailed[name]
	r.mu.Unlock()

	if failed || r.cfg.StateDir == "" {
		return failed
	}

	_, err := os.Stat(r.adjustFailedPath(name))

	return err == nil
}

// recordAdjustOutcome keeps the failed-adjust record in step with the
// adjust that just ran. Only an adjust that configured the network sets
// or clears it. One run with --skip-net left the connections as they
// were, so it cannot have half built them, and recording its failure
// would turn the retry into a full adjust that connects a peer the
// operator disconnected, the first step of a split-brain recovery.
func (r *Reconciler) recordAdjustOutcome(ctx context.Context, name string, skipNet bool, adjustErr error) {
	switch {
	case skipNet:
	case adjustErr != nil:
		r.setAdjustFailed(ctx, name)
	default:
		r.forgetAdjustFailed(name)
	}
}

// upRecorded runs `drbdadm up` and keeps the failed-adjust record in
// step with it. Up configures the network the way a full adjust does,
// new-peer and new-path before connect, so a failure leaves the same
// StandAlone connections with peer devices behind, and the next pass,
// an adjust because the slot is loaded now, must not read them as an
// operator's disconnect.
func (r *Reconciler) upRecorded(ctx context.Context, name string) error {
	err := r.cfg.Adm.Up(ctx, name)
	r.recordAdjustOutcome(ctx, name, false, err)

	return errors.WithStack(err)
}

func (r *Reconciler) setAdjustFailed(ctx context.Context, name string) {
	r.mu.Lock()
	if r.adjustFailed == nil {
		r.adjustFailed = map[string]bool{}
	}

	r.adjustFailed[name] = true
	r.mu.Unlock()

	if r.cfg.StateDir == "" {
		return
	}

	// Best effort: without the file only a satellite restart before the
	// next full adjust loses the record, which is the old behaviour.
	err := os.WriteFile(r.adjustFailedPath(name), nil, adjustFailedFileMode)
	if err != nil {
		log.FromContext(ctx).Info("cannot record the failed adjust on disk", "resource", name, "error", err.Error())
	}
}

func (r *Reconciler) forgetAdjustFailed(name string) {
	r.mu.Lock()
	delete(r.adjustFailed, name)
	r.mu.Unlock()

	if r.cfg.StateDir == "" {
		return
	}

	_ = os.Remove(r.adjustFailedPath(name))
}

func (r *Reconciler) adjustFailedPath(name string) string {
	return filepath.Join(r.cfg.StateDir, name+adjustFailedSuffix)
}
