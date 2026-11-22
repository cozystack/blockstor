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

	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

// UnlockAttempts is how many passphrases u has tried against a keyring.
func UnlockAttempts(u *Unlocker) int {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.attempts
}

// Started reports whether w's Start has run, so reads start watches.
func Started(w *WatchedReader) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.ctx != nil
}

// SecretWatchFailed reports whether the watch on the Secret name has failed.
func SecretWatchFailed(w *WatchedReader, name string) bool {
	w.mu.Lock()
	owatch := w.watches[watchKey{secret: true, name: name}]
	w.mu.Unlock()

	if owatch == nil {
		return false
	}

	_, failed := owatch.state()

	return failed
}

// ReportToSyncedWatch hands err to the error handler of a watch that has
// listed its object, and reports whether the watch failed and whether it
// was stopped.
func ReportToSyncedWatch(err error) (bool, bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	owatch := &objectWatch{cancel: cancel, synced: true}
	key := watchKey{secret: true, name: KeyringSecretName}
	reflector := toolscache.NewReflector(nil, &corev1.Secret{}, toolscache.NewStore(toolscache.MetaNamespaceKeyFunc), 0)

	(&WatchedReader{}).watchErrorHandler(owatch, key)(ctx, reflector, err)

	_, failed := owatch.state()

	return failed, ctx.Err() != nil
}
