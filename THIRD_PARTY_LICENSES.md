# Third-party licenses

Blockstor is distributed under the [Apache License, Version 2.0](LICENSE).
It depends on the following third-party Go modules. All licenses listed
below belong to the Apache-2.0–compatible set
(`Apache-2.0`, `BSD-2-Clause`, `BSD-3-Clause`, `MIT`, `MPL-2.0`, `ISC`);
see `.github/workflows/license-check.yml` for the CI-enforced allowlist.

The authoritative, machine-checked dependency-license list is generated
on every pull request by that workflow. The summary below covers the
direct (non-indirect) dependencies as of v0.1.1.

## Direct dependencies

| Module | Version | License |
|---|---|---|
| `github.com/LINBIT/golinstor` | v0.60.0 | Apache-2.0 |
| `github.com/cockroachdb/errors` | v1.13.0 | Apache-2.0 |
| `github.com/fsnotify/fsnotify` | v1.9.0 | BSD-3-Clause |
| `github.com/go-logr/logr` | v1.4.3 | Apache-2.0 |
| `github.com/google/uuid` | v1.6.0 | BSD-3-Clause |
| `github.com/onsi/ginkgo/v2` | v2.27.2 | MIT |
| `github.com/onsi/gomega` | v1.38.2 | MIT |
| `github.com/prometheus/client_golang` | v1.23.2 | Apache-2.0 |
| `golang.org/x/sys` | v0.39.0 | BSD-3-Clause |
| `k8s.io/api` | v0.35.0 | Apache-2.0 |
| `k8s.io/apimachinery` | v0.35.0 | Apache-2.0 |
| `k8s.io/client-go` | v0.35.0 | Apache-2.0 |
| `sigs.k8s.io/controller-runtime` | v0.23.3 | Apache-2.0 |
| `sigs.k8s.io/yaml` | v1.6.0 | Apache-2.0 + BSD-3-Clause |

## Policy

- No GPL / AGPL / LGPL / SSPL / commercial-restricted code may enter the
  runtime dependency graph.
- This includes any code generated from a GPL specification (e.g. an
  OpenAPI document carrying a GPL license declaration).
- For API interoperability with LINSTOR, blockstor uses
  [LINBIT/golinstor](https://github.com/LINBIT/golinstor), which is
  published by LINBIT under Apache-2.0 and is the upstream-blessed
  source of truth for the LINSTOR REST API wire shape.

## Trademarks

LINSTOR, LINBIT, and DRBD are trademarks or registered trademarks of
LINBIT. Blockstor is an independent project and is not affiliated with,
endorsed by, or sponsored by LINBIT.
