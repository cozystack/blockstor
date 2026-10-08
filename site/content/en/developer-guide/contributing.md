---
title: "Contributing"
linkTitle: "Contributing"
weight: 50
description: "Licensing rules, and what a change needs before it merges."
---

## Licensing: Apache 2.0, clean room

Blockstor is Apache 2.0. Do not copy structure or code from GPL sources, in particular `linstor-server` and `drbd-utils`. You may read them to understand behaviour, never to reproduce how they are written.

Hard rules:

- No GPL-licensed spec files in the tree. LINSTOR's `rest_v1_openapi.yaml` declares GPLv3, and code generated from it is a derivative work.
- No code generated from a GPL source by any tool.
- No Go dependency under GPL, AGPL, LGPL or SSPL. CI allows only Apache-2.0, BSD-2-Clause, BSD-3-Clause, MIT, MPL-2.0 and ISC, through [`license-check.yml`](https://github.com/cozystack/blockstor/blob/main/.github/workflows/license-check.yml).
- Every Go source file carries an `SPDX-License-Identifier: Apache-2.0` header.

Apache-2.0 projects that are safe to study and depend on: [`golinstor`](https://github.com/LINBIT/golinstor), the authoritative description of the REST wire shape; [`piraeus-operator`](https://github.com/piraeusdatastore/piraeus-operator); [`linstor-csi`](https://github.com/piraeusdatastore/linstor-csi); [`drbd-reactor`](https://github.com/LINBIT/drbd-reactor); and the public DRBD 9 documentation and man pages.

When in doubt, work clean room: one person reads the GPL source and writes a behavioural spec, and another implements it without seeing the original.

## Before you open a pull request

- `go test ./...` passes and `make lint` is clean.
- A bug fix comes with a test at the lowest tier that catches it. A bug in the operator CLI also gets a cell in `tests/e2e/cli-matrix/`.
- A new CLI verb or a change to a wire shape is checked with the parity refresh, and an intended difference from upstream gets a row in [`docs/cli-parity-known-deltas.md`](https://github.com/cozystack/blockstor/blob/main/docs/cli-parity-known-deltas.md).
- User-facing behaviour is documented on this site, under `site/content/en/docs/`.
- Commits follow Conventional Commits and are signed off (`git commit --signoff`).
