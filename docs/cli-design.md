# `blockstor` CLI — design

A native CLI that speaks the Kubernetes API directly and reproduces the command surface operators already know, so the upstream python client can be dropped as a runtime dependency.

## Why not go through the REST apiserver

The store layer is already a reusable library: `k8s.New(c ctrlclient.Client)` (`pkg/store/k8s/k8s.go`) builds every sub-store from a plain controller-runtime client, and the whole wire↔CRD transcode — property split into typed `DRBDOptions`, name normalisation, the composite `<rd>.<node>` naming, DRBD state projection from CRD Status — lives in that package rather than in the REST handlers. A CLI that constructs a client from the operator's kubeconfig therefore gets the same DTOs the apiserver would return, without an HTTP hop and without duplicating a line of translation logic.

Reading CRDs directly is also *more* correct than reading through the apiserver. The REST layer runs N replicas behind a ClusterIP and reads from controller-runtime informer caches, so it carries retry machinery (`pkg/rest/cache_retry.go`) purely to absorb cross-replica cache lag. A CLI using a non-cached client reads from the single Kubernetes API and gets read-your-writes consistency for free — that class of race cannot occur.

## Command surface

Commands mirror the upstream noun-verb grammar so muscle memory and existing runbooks keep working:

```
blockstor node list                 blockstor n l
blockstor storage-pool list         blockstor sp l
blockstor resource-definition create pvc-x    blockstor rd c pvc-x
blockstor resource toggle-disk n1 pvc-x       blockstor r td n1 pvc-x
blockstor volume-definition set-size pvc-x 0 10G   blockstor vd s pvc-x 0 10G
```

Every noun and verb carries the same short alias the upstream client accepts, and the alias table is data, not a pile of `if`s — it is the single source both the command tree and its tests read. Adding a command means adding a row.

The scope is "everything this project actually uses": the command/flag combinations exercised by `tests/e2e/cli-matrix/`, the operator-harness replay workflows, and the stand/hack scripts. Commands the upstream client offers that blockstor does not implement stay out and are listed in the parity docs rather than stubbed.

## Output

**One intermediate representation: `metav1.Table`.** Every view — whether it came from the API server or was assembled client-side — becomes a `metav1.Table`, and a single renderer turns that into text. This is the same type `kubectl get` prints, so alignment, empty-cell handling and column semantics behave the way operators expect, and one renderer means one place where formatting bugs can live.

Two producers feed it:

- **Server-side.** Every CRD carries `additionalPrinterColumns` (added by this work — the CRDs previously had none, so `kubectl get resources` showed only NAME/AGE). The API server renders those into a `metav1.Table`. This makes plain `kubectl` useful on its own, independently of the CLI, and gives the CLI a zero-logic path for simple listings.
- **Client-side.** The upstream-shaped views are cross-kind joins with specific column names and derived cells — `resource list` joins the resource definition's port, the replica's usage and connection states, and a sync percentage; `--faulty` filters on a computed predicate. Those tables are assembled from store DTOs in the CLI and handed to the same renderer.

Column *names and order* follow the upstream client, because shell in this repo and in operator runbooks greps and awks that output. Where a script parses a column, the parse expression itself becomes a test (see below).

**Colour is preserved** — it is load-bearing during an incident, not decoration. States are classified into semantic classes (healthy / transitional / broken / neutral) and painted green / yellow / red; an unrecognised state is deliberately neutral, never green, so a future DRBD state cannot masquerade as healthy. Painting is enabled only for an interactive terminal, and honours `--color=auto|always|never`, `NO_COLOR` and `TERM=dumb` — a piped or redirected run emits bytes identical to the uncoloured output, which is what keeps the shell harnesses parsing our tables reliable.

`-m/--machine-readable` emits the JSON shape the existing scripts already consume; that shape is a contract with the harness and is pinned by golden tests.

## Writes

A create is a plain CRD create: the controllers fill in the DRBD port, minor and node-id via their set-if-nil allocation pass, and the resource-definition controller adds the tiebreaker and quorum settings. The REST layer performs no allocation on create, so there is nothing to reimplement.

Modifications go through server-side apply with the CLI's own field manager. That is not a stylistic choice: rebuilding a spec from scratch and PUT-ing it would strip the identities the controller allocated (port, node-id, per-volume minor), and those are exactly the fields whose loss causes a DRBD reconnect or a full resync. The carry-across rules the store applies on update are the reference for what must survive a modify.

`resource-group spawn` is the one verb with real orchestration behind it (autoplace). The placement engine is already a reusable library (`pkg/placer`, constructed from `store.Store`) and placement is additionally driven by the resource-group controllers, so the CLI calls the placer rather than reimplementing placement.

## Encryption, and one thing Kubernetes does not hold

The cluster master key lives in a Secret, so `encryption create-passphrase` writes it there and `enter-passphrase` proves knowledge of it against the same Secret. Both compare in constant time: a byte-by-byte compare leaks where two passphrases first differ, and an attacker who can time enough attempts recovers the key one character at a time. `create-passphrase` refuses to replace an existing passphrase with a different value, because rotating the master key leaves every existing LUKS volume undecryptable; re-running it with the same value stays a success so a script's pre-flight step is idempotent.

Serving `enter-passphrase` over REST additionally flips an in-memory unlocked flag in the controller process, and this CLI cannot flip that. It matters less than the name suggests. The flag's only reader is `stampSuspendedOnLUKS` (`pkg/rest/resources.go`), which sets `state.suspended` on LUKS resources in the REST view — the Suspended/Available column. It gates nothing: the LUKS create check reads the Secret (`refuseLUKSWithoutPassphrase`), and satellites decrypt from the Secret too. It is also per-process and resets on restart, so across several apiserver replicas it already disagrees with itself. So the CLI does the part that has an effect, succeeds, and says on stderr that the display flag is untouched.

`error-reports` is deliberately absent from the command surface. The reports are a ring buffer in the controller process's memory; a client that speaks to the API server has nothing to list, and carrying the verb only to refuse it is worse than not advertising it.

One command is approximate rather than exact. `resource-group query-size-info` / `query-max-volume-size` report the physical bound derived from the free capacity of the pools a replica set would occupy. The controller additionally applies the thin-pool oversubscription policy, which lives inside `pkg/rest` and is not reusable; the CLI's figure is therefore always at least as conservative as the controller's, never more optimistic.

## Test plan

Tests come first, and each layer answers a different question.

**Unit — pure logic.** State-to-colour classification (including case-insensitivity, the `SyncTarget(45%)` suffix, and the rule that unknown states are neutral); colour gating (`--color`, `NO_COLOR`, `TERM=dumb`, TTY vs pipe); alias resolution (`sp l` → `storage-pool list`, and every other pair) driven off the same alias table the command tree uses, so a command added without its alias fails; flag parsing and value validation; size and capacity formatting; sort order of rows.

**Golden — rendering.** A fixed `metav1.Table` renders to a fixed string: column alignment, over-long cells, empty cells, and multi-byte characters. Rendered twice — once uncoloured, once with colour forced — so the escape placement is pinned and, crucially, so it is proven that the uncoloured output contains no escapes at all.

**Golden — view assembly.** For each view, fake store DTOs in, `metav1.Table` out, with the exact upstream column names and order asserted. This is the parity-critical layer: it is where a wrong column name or a dropped `--faulty` row shows up as a reviewable diff. Includes the derived cells — usage, connection summary, sync percentage — and the `-m` JSON shape.

**Contract — the harness must keep parsing us.** The grep/awk/jq expressions that `tests/e2e/cli-matrix/` and the stand scripts run against the upstream client's output are extracted and run against our golden output. If a column moves or a separator changes, the test that fails is the one carrying the real parse expression, not a paraphrase of it.

**Integration — envtest.** A real API server with the real CRDs: list/create/delete round-trips through the store-as-library path; a modify proves the controller-allocated identities survive (the field-manager rule above); and the printer columns are asserted by requesting a `metav1.Table` from the API server, which also proves the CRD markers are what we think they are.

**End-to-end — the existing matrix, unchanged.** `tests/e2e/cli-matrix/` runs against a live stand by invoking the CLI. Pointing those same scripts at `blockstor` instead of the upstream client is the acceptance criterion for dropping the dependency: same scripts, same assertions, same stand.

## Clean-room

The upstream python client is GPL. Its source is not read, quoted, or translated. What this CLI reproduces is the *interface* — command names, flag names, column names, colour semantics — observed from running the client and from this repository's own tests, scripts and parity documentation. Interface facts of that kind are what makes a compatible reimplementation possible; the implementation here is written from the blockstor API types.
