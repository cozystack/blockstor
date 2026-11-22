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

package controllers

import (
	"context"
	"strings"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/storage"
)

// driftCheckInterval is how often the satellite asks drbdadm which of
// the node's resources the kernel no longer runs as their .res says.
//
// A converged replica gets no pass of its own: its next one comes from
// an event, its own Resource, a peer, its definition or a kernel state
// change the observer reports. What no event reports is the kernel
// drifting from the configuration, a peer path or an option changed
// behind the satellite's back. LINSTOR adjusts only the resources
// `drbdadm sh-list-adjustable` lists (DrbdLayer.prepare), and so does
// this check: one drbdadm run for the whole node, which parses the
// .res files once and lists the resources an adjust would change.
const driftCheckInterval = 30 * time.Second

// driftCheck runs drbdadm sh-list-adjustable every interval and sends
// a pass for each local replica it lists.
type driftCheck struct {
	reader   client.Reader
	exec     storage.Exec
	node     string
	out      chan<- event.GenericEvent
	interval time.Duration

	// failing is set while drbdadm fails, so the failure is logged
	// once rather than every interval.
	failing bool
}

// run checks every interval until ctx ends.
func (d *driftCheck) run(logger logr.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		interval := d.interval
		if interval <= 0 {
			interval = driftCheckInterval
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				d.check(ctx, logger)
			}
		}
	}
}

// check sends a pass for every local replica drbdadm lists as
// adjustable. When drbdadm cannot tell (an older drbd-utils without
// sh-list-adjustable, a .res it cannot parse), every local replica
// gets one, the way LINSTOR adjusts every resource then.
func (d *driftCheck) check(ctx context.Context, logger logr.Logger) {
	out, err := d.exec.Run(ctx, "drbdadm", "sh-list-adjustable", "all")
	if err != nil {
		if ctx.Err() != nil {
			return
		}

		if !d.failing {
			logger.Error(err, "drbdadm sh-list-adjustable failed; passing every local replica until it answers")
		}

		d.failing = true

		d.sendAll(ctx, logger)

		return
	}

	if d.failing {
		logger.Info("drbdadm sh-list-adjustable answers again")
	}

	d.failing = false

	listed := map[string]bool{}

	for line := range strings.Lines(string(out)) {
		if name := strings.TrimSpace(line); name != "" {
			listed[naming.Key(name)] = true
		}
	}

	if len(listed) == 0 {
		return
	}

	d.send(ctx, logger, func(res *blockstoriov1alpha1.Resource) bool {
		return listed[naming.Key(res.Spec.ResourceDefinitionName)]
	})
}

func (d *driftCheck) sendAll(ctx context.Context, logger logr.Logger) {
	d.send(ctx, logger, func(*blockstoriov1alpha1.Resource) bool { return true })
}

// send sends a pass for every local replica pick selects.
func (d *driftCheck) send(ctx context.Context, logger logr.Logger, pick func(*blockstoriov1alpha1.Resource) bool) {
	var list blockstoriov1alpha1.ResourceList

	err := d.reader.List(ctx, &list)
	if err != nil {
		logger.Error(err, "list local replicas for the drift check")

		return
	}

	for i := range list.Items {
		res := &list.Items[i]
		if !naming.Same(res.Spec.NodeName, d.node) || !pick(res) {
			continue
		}

		select {
		case d.out <- event.GenericEvent{Object: &blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: res.Name}}}:
		case <-ctx.Done():
			return
		}
	}
}
