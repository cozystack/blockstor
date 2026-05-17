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

// Build-time identity of this binary. Project is a const because the
// binary's own name never changes; Version / GitCommit are vars so the
// container build can stamp them via `-ldflags -X` (Bug 169).
const Project = "blockstor"

// Version / GitCommit identify the blockstor build. Defaults are
// development-friendly placeholders; the production Dockerfile rewrites
// them via `go build -ldflags "-X .../version.Version=<tag> -X
// .../version.GitCommit=<sha>"`. `-ldflags -X` only works against
// package-level string vars, never consts.
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
	LinstorVersion = "1.33.2"
	// RestAPIVersion: bumped from "1.23.0" → "1.27.0" (Bug 222). The
	// upstream Java LINSTOR contract advanced through 1.24, 1.25, 1.26
	// and 1.27 while we still advertised 1.23 — and python-linstor's
	// `_require_version()` gates every CLI flag added in that window
	// (e.g. `--storage-pool-list`, `--diskless-on-remaining` second
	// form, several rg-modify keys) client-side on this string. Until
	// we report a version >= the gate, the CLI refuses to even send
	// the request and blockstor looks like it's missing features it
	// actually serves.
	//
	// Bug 238 (OpenAPI spec version drift): the vendored OpenAPI spec
	// at `third_party/linstor-openapi/rest_v1_openapi.yaml` carries
	// `info.version: 1.28.0` — strictly newer than what we serve
	// here. The served version is deliberately the LOWER of (vendored
	// spec, supported endpoints): the 1.28.0 spec includes endpoints
	// blockstor has not yet wired (snapshot-clone data plane, full
	// rebalance, etc.), so advertising 1.28.0 would open additional
	// python-linstor `_require_version` gates whose CLI calls then
	// either 404 or silently produce wrong results.
	//
	// When bumping this string a future contributor MUST:
	//   1. confirm every endpoint added between RestAPIVersion and
	//      `third_party/linstor-openapi/rest_v1_openapi.yaml`
	//      info.version is either wired or refused with an explicit
	//      501 envelope;
	//   2. update `pkg/version/bug_238_test.go` to reflect the new
	//      alignment (or document the new divergence).
	RestAPIVersion = "1.27.0"
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
var (
	LinstorGitHash   = "blockstor"
	LinstorBuildTime = "2026-01-01T00:00:00+00:00"
)
