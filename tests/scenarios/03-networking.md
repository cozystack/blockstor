# Group 3 — Networking

`NetInterface` CRUD, `PrefNic` (node + storage-pool levels with the
Diskless caveat), multi-path DRBD (`resource-connection path
create`), `StltCon` (controller-satellite active interface),
end-to-end replication over a dedicated network.

Some networking tests need a **multi-NIC stand** — `stand/setup-host.sh
--extra-nic` provisions a second virbr bridge so workers come up
with `default` (control plane) + `repl` (10.245.0.0/24) interfaces.

[Group index in README.md](README.md).

---

## NetInterface CRUD

### 3.1 NetInterface create / list / modify / delete — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Managing network interface cards" (lines 2120-2169); ug9-features 1.x; REST surface

**Unit:** `pkg/rest/nodes_test.go` exercises `POST/PUT/DELETE /v1/nodes/{node}/net-interfaces[/{name}]`. POST returns 201 + ApiCallRc, GET returns the list, DELETE is idempotent.
**E2E:** `linstor node interface create worker-1 nic_10G 192.168.43.10` → `linstor node interface list worker-1` shows two rows (default + nic_10G).

### 3.2 NetInterface name validation — S

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Managing network interface cards" (lines 2139-2143)

Regex `(?i)^[_a-z][a-z0-9_-]{2,31}`. Min 3 / max 32 chars. Test invalid names (start with digit, contain dot, etc.) → 400.

### 3.3 `default` interface auto-created at node register — S

- **Priority:** P0  **Target:** integration  **Complexity:** L

When satellite first dials Hello, its IP is registered as the `default` NetInterface. Test: envtest Hello → Node CRD has `NetInterfaces[0].Name=default` populated.

---

## PrefNic — preferred replication interface

### 3.4 `PrefNic` on node selects interface for DRBD traffic — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 §"Managing network interface cards" (lines 2145-2160); ug9-features 1.1; advanced-config #1

**Why:** Separates DRBD replication from control-plane traffic. UG recommends node-level over pool-level — applies to all storage pools on the node including the Diskless one.

**Unit:** `pkg/dispatcher/dispatcher.go:peerAddressWithPrefNic` returns the `repl` interface address when `PrefNic=repl` is set on the peer Node.
**E2E:** 2-NIC stand; set `PrefNic=repl` on each node; spawn an RD; `ssh worker-1 'cat /var/lib/linstor.d/<rd>.res | grep address'` shows 10.245.x addresses, not the default 10.244.x. Verify bytes actually flow over the `repl` NIC via `iftop` during a 1 GiB write.

**Failure modes:**
- StorageClass `PrefNic` param ignored → control plane saturates
- Prop accepted but `.res` still has default IP → dispatcher / conffile-builder gap

### 3.5 `PrefNic` on storage-pool with Diskless caveat — P

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Managing network interface cards" (lines 2152-2160); ug9-features 1.2

UG warns: pool-level PrefNic means Diskless / Tiebreaker resources STILL use `default` unless you also PrefNic the Diskless pool. Test pins which behaviour blockstor implements (matches upstream → document; auto-inherits → verify).

**E2E:** set PrefNic only on `zfs-thin` pool; spawn a 3-replica RD getting a Diskless tiebreaker on worker-3; audit worker-3's `.res` for which IP it uses. Document the result.

### 3.6 StorageClass `property.linstor.csi.linbit.com/PrefNic` propagates — S

- **Priority:** P1  **Target:** e2e  **Complexity:** L
- **Source:** linstor-kubernetes.adoc §2186-2199; advanced-config #1

The CSI parameter sets a RG prop → propagates to spawned RDs → ends up in `.res`. Test: StorageClass with the property → PVC → resource on the `repl` network.

---

## Multi-path DRBD

### 3.7 `resource-connection path create` for redundant networking — T

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** H (implement first)
- **Source:** UG9 §"Creating multiple DRBD paths with LINSTOR" (lines 2187-2256); ug9-features 1.4

**Status:** Not implemented. `grep ResourceConnectionPath` → zero matches. `pkg/drbd/conffile.go` emits a single `path { ... }` block per connection.

**What "done" means:**
- REST: `POST /v1/resource-definitions/{rd}/resource-connections/{nodeA}/{nodeB}/paths` accepts path1, path2, ...
- `ConfFileBuilder` emits multiple `path { host A address X; host B address Y; }` blocks
- DRBD honours `TcpPortAutoRange` per path (warning per UG: path-specific ports are dynamically allocated, not the ones from `node interface create`)

**Unit (after implement):** conffile renderer test with two NICs per node, two paths → both appear in the .res.
**E2E:** 2 NICs per node, 2 paths per resource-connection. Block path1 via iptables → DRBD switches to path2 within `ping-timeout` (~5s). TCP transport uses one path at a time; RDMA balances — pin both behaviours per UG.

**Workaround until implemented:** Linux bond on top of redundant uplinks below DRBD. Document in `docs/networking.md`.

### 3.8 Multi-path default-path coexistence — T

- **Priority:** P2  **Target:** e2e  **Complexity:** L (after 3.7)
- **Source:** UG9 §"How adding a new DRBD path affects the default path" (lines 2233-2255)

When you add an explicit path, the implicit `default` path is no longer used **unless you also explicitly add it as path3**. Test pins this so operators understand why their `default` NIC went quiet.

---

## Controller-satellite (StltCon) traffic

### 3.9 `StltCon` interface marker for controller-satellite traffic — P

- **Priority:** P1  **Target:** unit  **Complexity:** L
- **Source:** UG9 §"Managing network interface cards" (lines 2161-2172); ug9-features 1.3

**Why:** `linstor node interface modify worker-1 satconn_1G --active` sets `StltConn/0/Active=true` so satellite dials controller via that NIC. For blockstor (gRPC satellite → apiserver) maps to "which IP does the satellite use to dial the apiserver".

**Status:** Contract-test normaliser (`tests/contract/normalize.go`) already includes the `StltConn/0/*` keys — the wire surface exists. Pin that we emit the actual values, not just empty keys.

**Unit:** `pkg/rest/net_interface_test.go` — POST sets `Active=true`, GET returns it.

### 3.10 Satellite respects `--controller-bind-address` from active interface — T

- **Priority:** P2  **Target:** e2e  **Complexity:** M
- **Source:** UG9 §"Managing network interface cards" (lines 2167-2169)

After `node interface modify ... --active`, satellite re-dials the controller via the new IP. Today blockstor's satellite reads the controller endpoint from a CLI flag at startup — doesn't honour runtime changes. Document the gap; test asserts the satellite re-dials on Spec change.

**Status (Outcome B — deferred):** Phase 10.6 retired the satellite→controller gRPC wire entirely; the satellite now talks only to the kube-apiserver via `ctrl.GetConfig()`. There is no `--controller-bind-address` flag and `NodeNetInterface` has no `Active` field — `IsActive` on the REST wire is synthesised as `i == 0` (pure presentation). Implementing live re-dial (Outcome A) would require resurrecting a custom controller wire that the Phase 10.6 design intentionally removed. Pinned by `pkg/satellite/stream/redial_spec_test.go` (`TestSatelliteFlagsLackControllerBindAddress`, `TestNodeCRDHasNoActiveField`) — both fail if anyone starts implementing Outcome A, forcing a positive re-dial test to replace them. Tracked as Bug 49 in `docs/known-issues.md`.

Note on `--controller-namespace` (Bug 023): the satellite binary grew this flag to name the NAMESPACE the cluster master passphrase Secret (`linstor encryption create-passphrase`) lives in — defaulting to `$POD_NAMESPACE` then `blockstor-system`. It is read through the kube-apiserver like every other satellite input; it carries no controller ADDRESS and does not change the Outcome B posture above.

---

## End-to-end on dedicated replication network

### 3.11 Multi-NIC stand provisions cleanly — S (operator step)

- **Priority:** P1  **Target:** e2e  **Complexity:** M (stand harness)
- **Source:** advanced-config #1

**Why:** Every test in this group needs a 2-NIC stand. Pin the harness step itself.

**E2E:** `stand/setup-host.sh --extra-nic` creates `virbr-repl` bridge with subnet 10.245.0.0/24; `make up NAME=X EXTRA_NIC=1` brings up workers with eth1 attached. Validation: `kubectl get nodes -o wide` shows both addresses; ping between worker-1 and worker-2 over both subnets succeeds.

### 3.12 Replication-only traffic measurable on `repl` NIC — S, missing test

- **Priority:** P0  **Target:** e2e  **Complexity:** M
- **Source:** advanced-config #1 expected behaviour

**Why:** Final integration check — proves PrefNic actually moves bytes.

**E2E:** PrefNic=repl, mount a PVC on worker-1, write 5 GiB. `iftop -i eth1` on worker-2 shows ~5 GiB inbound; `iftop -i eth0` shows ~0. Inverse without PrefNic shows the opposite.

---

## Implementation-order recommendation

1. 3.1, 3.2, 3.3 — NetInterface CRUD foundation (existing surface, just tests)
2. 3.9 — StltCon prop wire (existing keys, pin values)
3. 3.4 — PrefNic on node (P0; verify the dispatcher path)
4. 3.11, 3.12 — multi-NIC stand harness + verify replication moves
5. 3.5, 3.6 — pool-level PrefNic + StorageClass parameter propagation
6. 3.7 — multi-path (P1, needs implementation)
7. 3.8 — multi-path default-path edge case
8. 3.10 — satellite re-dial on interface modify (P2)

## Group summary

| Tag | Count |
|-----|-------|
| P0 unit | 3 |
| P0 e2e | 2 |
| P1 unit | 2 |
| P1 e2e | 3 |
| P2 e2e | 1 |
| T (implement first) | 3 |

**Hardware requirement:** 4 of these 12 tests need a 2-NIC stand.
Land them in a single CI lane that brings up `EXTRA_NIC=1`.
