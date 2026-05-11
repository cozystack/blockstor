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

package controllers_test

import (
	ctrl "sigs.k8s.io/controller-runtime"
)

// requeueRequested reports whether the reconciler asked
// controller-runtime to retry — either the legacy `Requeue=true`
// flag or a positive `RequeueAfter`. Wraps `result.Requeue` so the
// production assertions stay readable while the deprecation of the
// bool field is invisible at the call sites. Once the reconcilers
// migrate to `RequeueAfter` everywhere, this helper folds into a
// plain `result.RequeueAfter > 0` check.
func requeueRequested(result ctrl.Result) bool {
	return result.Requeue || result.RequeueAfter > 0
}
