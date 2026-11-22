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

package passphrase

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// WatchSyncTimeout is how long a watch gets to list its object. One that
// has not by then, because the API server is slow or out of reach, is
// stopped, and its object is read through the API from then on, as it was
// before there were watches. A watch the API server refuses, under a Role
// that grants get but not list and watch on that Secret, stops at once.
const WatchSyncTimeout = 30 * time.Second

// WatchedReader serves the reads the encryption state is derived from, the
// ControllerConfig and the Secrets in Namespace, from one watch per object,
// so a caller that derives the state over and over (the controller's pass,
// every resource view) sends no request while nothing changes.
//
// Each watch is limited to its object by name: a Role that grants get,
// list and watch with resourceNames allows exactly that, and nothing else
// in the namespace becomes readable. The watch on an object starts at its
// first read, so a passphrase Secret the ControllerConfig is switched to
// gets one too.
//
// Until a watch has listed its object, and for good once it fails to or
// is refused, reads go to Direct. Every other read, a List included, goes
// to Direct.
//
// A watch trails the API server: a read-modify-write must read through
// Direct (Secrets.Direct), or its update conflicts until the watch
// catches up.
type WatchedReader struct {
	// Config, Options and Direct come from the manager: its rest config,
	// its HTTP client, scheme and REST mapper, and its API reader.
	Config  *rest.Config
	Options cache.Options
	Direct  client.Reader

	// Namespace is where the watched Secrets live.
	Namespace string

	// SyncTimeout overrides WatchSyncTimeout (tests).
	SyncTimeout time.Duration

	mu      sync.Mutex
	ctx     context.Context //nolint:containedctx // the watches live as long as the Runnable
	watches map[watchKey]*objectWatch
}

type watchKey struct {
	secret bool
	name   string
}

type objectWatch struct {
	cache cache.Cache

	mu     sync.Mutex
	cancel context.CancelFunc
	synced bool
	failed bool
}

func (o *objectWatch) state() (bool, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.synced, o.failed
}

// markSynced records that the watch has listed its object, unless it has
// failed already: a failed watch is stopped and serves nothing again.
func (o *objectWatch) markSynced() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.failed {
		o.synced = true
	}
}

// fail stops the watch and sends its reads to Direct, and reports whether
// this call is the one that failed it.
func (o *objectWatch) fail() bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.failed {
		return false
	}

	o.synced, o.failed = false, true

	if o.cancel != nil {
		o.cancel()
	}

	return true
}

// NeedLeaderElection returns false: every replica that derives the state
// reads it.
func (*WatchedReader) NeedLeaderElection() bool { return false }

// Start lets reads start watches, which stop when ctx ends.
func (w *WatchedReader) Start(ctx context.Context) error {
	w.mu.Lock()
	w.ctx = ctx
	w.mu.Unlock()

	<-ctx.Done()

	return nil
}

// Get reads a watched object from its watch, everything else from Direct.
func (w *WatchedReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	owatch := w.watchFor(ctx, key, obj)
	if owatch != nil {
		if synced, _ := owatch.state(); synced {
			return owatch.cache.Get(ctx, key, obj, opts...) //nolint:wrapcheck // the reader's error, as Direct's is
		}
	}

	return w.Direct.Get(ctx, key, obj, opts...) //nolint:wrapcheck // the reader's error, as it is
}

// List reads from Direct: only single objects are watched.
func (w *WatchedReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return w.Direct.List(ctx, list, opts...) //nolint:wrapcheck // the reader's error, as it is
}

// watchFor returns the watch on the object key names, starting it at the
// first read, or nil for an object that is not watched or a reader not
// started yet.
func (w *WatchedReader) watchFor(ctx context.Context, key client.ObjectKey, obj client.Object) *objectWatch {
	var wkey watchKey

	switch obj.(type) {
	case *corev1.Secret:
		if key.Namespace != w.Namespace {
			return nil
		}

		wkey = watchKey{secret: true, name: key.Name}
	case *blockstoriov1alpha1.ControllerConfig:
		wkey = watchKey{secret: false, name: key.Name}
	default:
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.ctx == nil {
		return nil
	}

	if owatch, ok := w.watches[wkey]; ok {
		return owatch
	}

	owatch := &objectWatch{}

	if w.watches == nil {
		w.watches = map[watchKey]*objectWatch{}
	}

	w.watches[wkey] = owatch

	// The watch outlives the read that starts it: it runs under Start's
	// context, not ctx.
	err := w.startWatch(owatch, wkey) //nolint:contextcheck // see above
	if err != nil {
		log.FromContext(ctx).WithName("passphrase").Error(err, "watch; reading through the API instead",
			"secret", wkey.secret, "name", wkey.name)
		owatch.fail()
	}

	return owatch
}

// startWatch starts a cache that holds the one object wkey names and settles
// owatch once that cache has listed it or has run out of time to.
func (w *WatchedReader) startWatch(owatch *objectWatch, wkey watchKey) error {
	var obj client.Object = &blockstoriov1alpha1.ControllerConfig{}

	byObject := cache.ByObject{Field: fields.OneTermEqualSelector("metadata.name", wkey.name)}

	if wkey.secret {
		obj = &corev1.Secret{}
		byObject.Namespaces = map[string]cache.Config{w.Namespace: {}}
	}

	opts := w.Options
	opts.ByObject = map[client.Object]cache.ByObject{obj: byObject}
	opts.DefaultNamespaces = nil
	opts.ReaderFailOnMissingInformer = true
	opts.DefaultWatchErrorHandler = w.watchErrorHandler(owatch, wkey)

	objCache, err := cache.New(w.Config, opts)
	if err != nil {
		return errors.Wrap(err, "new cache")
	}

	ctx, cancel := context.WithCancel(w.ctx)

	owatch.mu.Lock()
	owatch.cancel = cancel
	owatch.mu.Unlock()

	_, err = objCache.GetInformer(ctx, obj, cache.BlockUntilSynced(false))
	if err != nil {
		cancel()

		return errors.Wrap(err, "informer")
	}

	owatch.cache = objCache

	go func() {
		_ = objCache.Start(ctx)
	}()

	go w.awaitSync(ctx, owatch, wkey)

	return nil
}

// awaitSync marks owatch synced once its cache has listed the object, and
// fails it once the cache has run out of time to.
func (w *WatchedReader) awaitSync(ctx context.Context, owatch *objectWatch, wkey watchKey) {
	timeout := w.SyncTimeout
	if timeout == 0 {
		timeout = WatchSyncTimeout
	}

	syncCtx, syncCancel := context.WithTimeout(ctx, timeout)
	defer syncCancel()

	if owatch.cache.WaitForCacheSync(syncCtx) {
		owatch.markSynced()

		return
	}

	if owatch.fail() && w.ctx.Err() == nil {
		log.FromContext(ctx).WithName("passphrase").Info(
			"the watch did not list its object in time; reading it through the API instead",
			"secret", wkey.secret, "name", wkey.name, "timeout", timeout)
	}
}

// watchErrorHandler is the watch's error handler. A watch that is refused,
// whether it has listed its object or not, fails: its reflector would
// retry for as long as the refusal lasts while the cache went on serving
// the object it last listed, however stale. Direct reads the object
// again, and reports the refusal, if it is one, to the caller.
func (w *WatchedReader) watchErrorHandler(owatch *objectWatch, wkey watchKey) toolscache.WatchErrorHandlerWithContext {
	return func(ctx context.Context, r *toolscache.Reflector, err error) {
		if !apierrors.IsForbidden(err) && !apierrors.IsUnauthorized(err) {
			toolscache.DefaultWatchErrorHandler(ctx, r, err)

			return
		}

		if owatch.fail() {
			log.FromContext(ctx).WithName("passphrase").Info(
				"the watch was refused; reading its object through the API instead",
				"secret", wkey.secret, "name", wkey.name, "error", err.Error())
		}
	}
}

// AddWatchedReader adds to mgr a WatchedReader over namespace, reading past
// its watches through mgr's API reader.
func AddWatchedReader(mgr manager.Manager, namespace string) (*WatchedReader, error) {
	watched := &WatchedReader{
		Config: mgr.GetConfig(),
		Options: cache.Options{
			HTTPClient: mgr.GetHTTPClient(),
			Scheme:     mgr.GetScheme(),
			Mapper:     mgr.GetRESTMapper(),
		},
		Direct:    mgr.GetAPIReader(),
		Namespace: namespace,
	}

	err := mgr.Add(watched)
	if err != nil {
		return nil, errors.Wrap(err, "add the passphrase watches")
	}

	return watched, nil
}
