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
	"sync"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// passReportInterval is how often the satellite logs how many resource
// passes it ran, by reason.
const passReportInterval = time.Minute

// The reasons a resource pass runs for, as passCounter counts them.
const (
	passResource   = "resource"   // the replica's own Resource changed
	passPeer       = "peer"       // another node's replica of the same definition changed
	passDefinition = "definition" // the ResourceDefinition changed
	passConfig     = "config"     // a group, the controller config, a node or a pool the replica renders changed
	passKernel     = "kernel"     // the observer saw a kernel state change
	passDrift      = "drift"      // drbdadm lists the replica as adjustable
	passRetry      = "retry"      // the previous pass asked to be run again
)

// passCounter counts the resource passes by why they ran, so the share
// of each kind of event can be read off the satellite log. A pass's
// reason is the source of the last event that came in for its replica
// since its previous pass began, or a retry the previous pass asked for
// when none did. The events are counted where they enter the queue
// (marker and the map functions), the only place that tells them apart
// from a requeue.
type passCounter struct {
	mu sync.Mutex

	// source is the reason of the last event that came in for a
	// replica whose pass has not begun yet.
	source map[string]string

	// passes counts the passes since the last report, by reason.
	passes map[string]uint64
}

func newPassCounter() *passCounter {
	return &passCounter{source: map[string]string{}, passes: map[string]uint64{}}
}

// changed records an event from source for name.
func (c *passCounter) changed(name, source string) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.source[name] = source
}

// begin starts a pass of name and counts it under its reason.
func (c *passCounter) begin(name string) string {
	if c == nil {
		return passRetry
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	reason, ok := c.source[name]
	if !ok {
		reason = passRetry
	}

	delete(c.source, name)
	c.passes[reason]++

	return reason
}

// report returns the pass counts since the last report as key/value
// pairs for the log, and starts the counts over. It returns nothing
// when no pass ran.
func (c *passCounter) report() []any {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.passes) == 0 {
		return nil
	}

	fields := make([]any, 0, 2*len(c.passes))

	for _, reason := range []string{passResource, passPeer, passDefinition, passConfig, passKernel, passDrift, passRetry} {
		fields = append(fields, reason, c.passes[reason])
	}

	clear(c.passes)

	return fields
}

// reportEvery logs the pass counts every interval until ctx ends.
func (c *passCounter) reportEvery(interval time.Duration, logger logr.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if kv := c.report(); kv != nil {
					logger.Info("resource passes", kv...)
				}
			}
		}
	}
}

// counted wraps inner so that every pass is counted under its reason.
func (c *passCounter) counted(inner reconcile.Reconciler) reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		c.begin(req.Name)

		return inner.Reconcile(ctx, req)
	})
}

// marker is a predicate that lets every event through and records it
// as coming from source. It goes last in a chain, so only events that
// reach the queue count.
func (c *passCounter) marker(source string) predicate.Predicate {
	record := func(obj client.Object) bool {
		c.changed(obj.GetName(), source)

		return true
	}

	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return record(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return record(e.ObjectNew) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return record(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return record(e.Object) },
	}
}

// mapFunc records an event from source for every replica fn enqueues.
func (c *passCounter) mapFunc(source string, fn handler.MapFunc) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		reqs := fn(ctx, obj)
		for _, req := range reqs {
			c.changed(req.Name, source)
		}

		return reqs
	}
}
