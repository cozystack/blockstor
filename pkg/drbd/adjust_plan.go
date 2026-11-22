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
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// AdjustPlan runs `drbdadm -d adjust <resource>`: the drbdsetup and
// drbdmeta commands a full adjust would run, one per line, without
// running them.
func (a *Adm) AdjustPlan(ctx context.Context, resource string) ([]string, error) {
	out, err := a.exec.Run(ctx, "drbdadm", "-d", "adjust", resource)
	if err != nil {
		return nil, errors.Wrapf(err, "drbdadm -d adjust %s", resource)
	}

	var plan []string

	for line := range strings.Lines(string(out)) {
		if line = strings.TrimSpace(line); line != "" {
			plan = append(plan, line)
		}
	}

	return plan, nil
}

// PlanReconfiguresPeer reports whether an adjust plan changes the
// configuration of the connection to peerNodeID of resource rather
// than only connecting it: a new-peer, a path, net options or the
// options of one of its peer devices.
//
// A connection an operator disconnected keeps every option it had, so
// the plan for it is the connect and its wait and nothing else. A
// connection the kernel dropped because it was set up for a peer that
// has changed since, a diskless peer that turned diskful say, also
// needs its options reset before the connect, and connecting it with
// the stale ones fails the handshake the same way again.
func PlanReconfiguresPeer(plan []string, resource string, peerNodeID int32) bool {
	peer := strconv.Itoa(int(peerNodeID))

	for _, line := range plan {
		verb, args, ok := drbdsetupCommand(line)
		if !ok || len(args) < 2 || args[0] != resource || args[1] != peer {
			continue
		}

		if verb == "connect" || strings.HasPrefix(verb, "wait-connect") {
			continue
		}

		return true
	}

	return false
}

// drbdsetupCommand splits one line of an adjust plan into the drbdsetup
// verb and its arguments. Lines of any other tool are not drbdsetup
// commands.
func drbdsetupCommand(line string) (string, []string, bool) {
	fields := strings.Fields(line)
	for i, field := range fields {
		if path.Base(field) != "drbdsetup" {
			continue
		}

		if i+1 >= len(fields) {
			return "", nil, false
		}

		return fields[i+1], fields[i+2:], true
	}

	return "", nil, false
}
