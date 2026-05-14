# Wave 2 — Group 3 — Networking (Day2 ops)

NetInterface CRUD, `PrefNic` at node scope, multi-path DRBD
(`resource-connection path`), TCP port-range controller knob, and
the K8s host-network ↔ container-network DRBD switch flow.

Pairs with wave1's `03-networking.md` — same surface, additional
Day2 operations.

[Group index in README.md](README.md).

---

### 3.W01 `node interface create <node> <name> <ip>` — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 §"Managing network interface cards" (lines 2119-2185) via tests/scenarios/day2-node-interface-create.md

Cross-listed with wave1 3.1. POST `/v1/nodes/{node}/net-interfaces` creates by IP; LINSTOR name is arbitrary, unrelated to Linux kernel NIC name.

**Unit:** `pkg/rest/nodes_test.go` — POST → 201 + ApiCallRc; name validation per UG line 2139-2143 (regex, 3-32 chars).
**E2E:** create `nic_10G 192.168.43.231` → `node interface list` shows two rows.

### 3.W02 `node interface modify ... --active` switches StltCon — P

- **Priority:** P2  **Target:** unit  **Complexity:** M
- **Source:** UG9 §"Managing network interface cards" (lines 2161-2185) via tests/scenarios/day2-node-interface-modify-active.md

Cross-listed with wave1 3.10. Phase 10.6 retired controller-satellite wire — `Active` field is presentation-only. Document the deferred state; assert `IsActive` synthesised as `i == 0`.

**Unit:** `pkg/rest/net_interface_test.go::TestNetInterfaceModifyActiveIsPresentationOnly` — PUT with `is_active=true` on the second interface MUST NOT promote it; the only switch path is reorder (DELETE + recreate). `TestNetInterfaceModifyActiveBodyRoundTrips` pins that the wire decoder still accepts the field so upstream golinstor's `linstor n interface modify` doesn't 400.

### 3.W03 `node set-property PrefNic <nic>` steers DRBD replication — S

- **Priority:** P0  **Target:** unit + e2e  **Complexity:** M
- **Source:** UG9 §"Managing network interface cards" (lines 2120-2185) via tests/scenarios/day2-node-prefnic.md

Cross-listed with wave1 3.4. Node-level `PrefNic` is safer than pool-level — applies to all SPs on the node, including the diskless. Existing connections need `.res` regenerate; happens on next RD modify or automatic via dispatcher.

**E2E:** `node set-property worker-1 PrefNic nic_10G` → next spawn's `.res` references `nic_10G` IP for worker-1.

### 3.W04 `resource-connection path create` multi-path — T

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** H (implement first)
- **Source:** UG9 §"Creating multiple DRBD paths with LINSTOR" (lines 2186-2255) via tests/scenarios/day2-resource-connection-multipath.md

Cross-listed with wave1 3.7. Up to N explicit paths between alpha-bravo plus the implicit `default` (only honoured if explicitly re-added as path3). Ports dynamic via `TcpPortAutoRange` regardless of interface-create port.

**Unit (after implement):** conffile renderer emits N `path { ... }` blocks per connection.
**E2E:** 2 NICs per node, 2 paths; iptables-drop path1 → DRBD fails over to path2 within ping-timeout. TCP transport: one path at a time; RDMA: balanced.

### 3.W05 Controller `TcpPortAutoRange` constrains dynamic port range — S

- **Priority:** P1  **Target:** unit + e2e  **Complexity:** L
- **Source:** UG9 NOTE block at lines 2224-2231 via tests/scenarios/day2-controller-tcp-port-range.md

`linstor controller set-property TcpPortAutoRange 7900-7999` → new RDs draw ports from new range; existing RDs keep their previously-assigned ports (no auto-renumber). Shrinking below current resource count eventually exhausts.

**Unit:** allocator unit test — set range, allocate N ports, expect all within range; assert allocator surfaces exhaustion as actionable error rather than panic.
**E2E:** flip prop, spawn a new RD, `rd lp` shows port in range.

### 3.W06 `LinstorSatelliteConfiguration` `hostNetwork: true` switch — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"Configuring DRBD replication to use the host network" (lines 2870-2890) via tests/scenarios/day2-k8s-drbd-host-network.md

Piraeus Operator path — satellite pods restart with hostNetwork, LINSTOR reconfigures peer IPs in each `.res`. **Host-network is the default for blockstor-on-cozystack stands** (most operators want firewall-on-host control + survival across CNI restarts).

**E2E:** apply CR; wait pods recreated; `cat /var/lib/linstor.d/<rd>.res` on satellite shows host IPs, not pod IPs; `drbdadm status Connected`.

### 3.W07 Switch DRBD replication back to container network — P

- **Priority:** P2  **Target:** unit + e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"Configuring DRBD replication to use the container network" (lines 2891-2946) via tests/scenarios/day2-k8s-drbd-container-network-switch.md

Inverse of 3.W06. Two recipes: drbdadm-based (suspend-io → disconnect → del-path → delete CR) keeps Pods running; reboot-based requires planned downtime. Test pins both paths.

**Unit:** `pkg/dispatcher/dispatcher_test.go::TestDispatcherSwitchesHostAndContainerNetwork` — flipping Node.Spec.NetInterfaces between host-network state (`k8s-internal` + `default` carrying host InternalIP) and container-network state (`default` carrying pod IP) re-renders `peer.<n>.address` in DrbdOptions without cache poisoning. Round-trip subtest pins state stability across repeated flips.
**E2E:** start from 3.W06 stand; run drbdadm-based recipe; `.res` flips to pod IPs; `drbdadm status Connected` after each node finishes.

---

## Group summary

| Tag | Count |
|-----|------:|
| P0 unit | 1 |
| P0 e2e | 2 |
| P1 unit | 1 |
| P1 e2e | 2 |
| P2 e2e | 2 |
| T (implement first) | 1 |
