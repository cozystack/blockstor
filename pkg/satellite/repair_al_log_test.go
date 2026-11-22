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
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/storage"
)

// DRBD clears the activity log's clean flag on every detach, so the
// repair runs after a clean `drbdsetup down` as well. Its log line used
// to say the log was left by an unclean shutdown, which sent operators
// looking for a crash that never happened.
func TestRepairedActivityLogIsNotReportedAsACrash(t *testing.T) {
	rec, dr, _, _, fx := uncleanFixture(t)
	dr.Volumes[0].ObservedGI = uncleanReportedGI

	fx.Expect(uncleanApplyAL, storage.FakeResponse{})
	fx.Expect(uncleanDumpMD, storage.FakeResponse{Stdout: dumpWithHistory("9F00AA11BB22CC33", uncleanReportedGI)})

	var lines []string

	logger := funcr.New(func(prefix, args string) { lines = append(lines, args) }, funcr.Options{})

	keep, err := rec.repairUncleanMetadata(log.IntoContext(t.Context(), logger), dr, dr.Volumes[0])
	if err != nil || !keep {
		t.Fatalf("repair: keep=%v err=%v", keep, err)
	}

	all := strings.Join(lines, "\n")
	if strings.Contains(all, "unclean shutdown") || !strings.Contains(all, "every detach") {
		t.Fatalf("the repair log reads as a crash: %s", all)
	}
}
