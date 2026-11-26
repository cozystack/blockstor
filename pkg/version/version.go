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

// Package version exposes the build identity of blockstor and the LINSTOR
// REST contract version it implements. The contract version is reported in
// /v1/controller/version so that golinstor clients can negotiate.
package version

// Project names the binary. It is a const because the binary's own name
// never changes; Version / GitCommit are vars so the container build can
// stamp them via `-ldflags -X` (Bug 169).
const Project = "blockstor"

// LogKey is the key every blockstor binary logs Version under when it
// starts, so the release a process runs is in its first log lines.
const LogKey = "blockstor_version"

// Version / GitCommit identify the blockstor build. Defaults are
// development-friendly placeholders; the production Dockerfile rewrites
// them via `go build -ldflags "-X .../version.Version=<tag> -X
// .../version.GitCommit=<sha>"`. `-ldflags -X` only works against
// package-level string vars, never consts.
//
//nolint:gochecknoglobals // ldflags-injected build identity, see comment above
var (
	Version   = "0.0.0-dev"
	GitCommit = "unknown"
)

// LINSTOR REST contract identity reported in /v1/controller/version.
// We mimic a recent upstream Java LINSTOR so strict golinstor clients
// accept our responses.
//
// LinstorVersion / RestAPIVersion are bound to upstream LINSTOR's
// release cadence and only change with code; consts express the
// invariant. LinstorGitHash and LinstorBuildTime carry the BLOCKSTOR
// commit + image build time, which differ per image — vars (Bug 169)
// so the Dockerfile can stamp them via `-ldflags -X`.
const (
	// LinstorVersion is the LINSTOR release whose REST contract blockstor
	// serves; see RestAPIVersion for the one place it differs.
	LinstorVersion = "1.35.0"
	// RestAPIVersion opens python-linstor's client-side
	// `_require_version()` gates, and linstor-csi from v1.13 refuses to
	// start below 1.29 (MinimumRestApiMinor in its highlevelclient).
	// History: 1.23.0 until Bug 222 raised it to 1.27.0, which stayed
	// until truncate and delete_empty_resource_definition (1.29.0) were
	// served. make-available's auto_manage_dual_primary and
	// unmake-available, which linstor-csi v1.13 also needs, are in
	// LINSTOR 1.35.0's spec but in no CHANGELOG-OPENAPI.md entry.
	//
	// It is the highest version whose additions are all either served or
	// refused with an explicit 501 because they have no meaning here
	// (database export, API tokens, drbd-reactorctl, DRBD client
	// replicas). 1.29.1, which LINSTOR 1.35.0 serves, only adds clone
	// volume_sizes; blockstor refuses that one but could serve it, so the
	// version stops at 1.29.0 and python-linstor keeps the flag gated.
	//
	// The spec is not vendored here any more: it went with the switch to
	// golinstor's types, and `make rest-types` still names the regen
	// script that went with it. 1.29.0 was checked against LINSTOR
	// v1.35.0: docs/rest_v1_openapi.yaml, whose info.version is 1.29.1,
	// and CHANGELOG-OPENAPI.md at the root.
	//
	// When bumping this string a future contributor MUST:
	//   1. confirm every endpoint and field added up to the new version
	//      in CHANGELOG-OPENAPI.md and the rest_v1_openapi.yaml of the
	//      LINSTOR release that serves it is either wired or refused
	//      with an explicit 501 envelope;
	//   2. update the compatibility matrix in docs/linstor-csi-compatibility.md
	//      and pkg/rest/version_bug_222_test.go.
	RestAPIVersion = "1.29.0"
)

// LinstorGitHash is the blockstor source commit SHA. Default
// "blockstor" matches the dev-build sentinel; the production image
// rewrites it via `-ldflags -X .../version.LinstorGitHash=$(git
// rev-parse HEAD)` so /v1/controller/version reports a real commit
// (Bug 169). Operators correlate wire bugs to commits via this field.
//
// LinstorBuildTime is the image build timestamp, RFC3339 with `+00:00`
// offset to match upstream LINSTOR's Java-formatted shape. Default
// "2026-01-01T00:00:00+00:00" is the dev sentinel; production
// rewrites via `-ldflags -X .../version.LinstorBuildTime=$(date -u
// +%FT%TZ)`. Parses cleanly as time.RFC3339 so contract tests don't
// gate on the exact value.
//
//nolint:gochecknoglobals // ldflags-injected build identity, see comment above
var (
	LinstorGitHash   = "blockstor"
	LinstorBuildTime = "2026-01-01T00:00:00+00:00"
)
