# Agent playbook — parallel integration-test execution

Read this BEFORE spawning a sub-agent on this codebase. It exists because the 2026-05-14 wave of 30+ parallel agents shipped several broken commits (lost worktrees, half-finished edits, duplicate PRs targeting the same scope, force-stripped finalizers). Following the contract below removes the entire class of those failures.

## 0. Prerequisites a launcher MUST check

- [ ] The repo is at HEAD of `main` and `git status` is clean. If there are uncommitted changes, commit/stash them on `main` first — sub-agents inherit a dirty worktree as their starting point.
- [ ] `docs/test-strategy.md` exists and has a per-group tracker section (statuses: `pending`, `in_progress`, `done`).
- [ ] The harness scaffold (`tests/integration/harness/*.go`) compiles and `tests/integration/smoke_test.go` passes locally — agents extend the harness, they MUST NOT have to fix it.
- [ ] `.github/workflows/integration.yml` is green on `main` — so an agent's PR CI failure is unambiguously their own.

If any of these fail, fix them BEFORE spawning agents.

## 1. Per-agent scope contract

Every sub-agent gets exactly ONE group from `docs/test-strategy.md` (Groups A through L, plus contract / e2e). The prompt MUST include:

| Field | Value |
|---|---|
| Group | A / B / … / L |
| Branch | `feat/integration-group-<letter>-<slug>` — e.g. `feat/integration-group-A-node` |
| Worktree | `isolation: "worktree"` ALWAYS (so the agent's edits never touch `main`) |
| Files to add | EXACTLY ONE Go file under `tests/integration/<group>_test.go` plus optional helpers under `tests/integration/harness/<group>_helpers.go` |
| Files NOT to touch | `tests/integration/harness/{envtest,manager,satellite,linstor,csi,fixtures,asserts,concurrent}.go`, ANY file outside `tests/integration/` |
| PR title | `test(integration): Group <letter> — <description> (<N> tests)` |
| PR template | "draft" via `gh pr create --draft`, body lists each test name + bug-guard reference from `docs/test-strategy.md` |
| Definition of done | `go test -tags=integration ./tests/integration/... -run '^TestGroup<letter>' -count=1` passes locally + CI green on the PR + scenario tracker row flipped to `done` in the same PR |

If an agent needs to change the harness, it MUST stop and ask — never speculate.

## 2. Anti-collision rules

- One group = one agent. Never dispatch two agents on the same group concurrently.
- Bug-fix agents (those modifying `pkg/...` for a real code bug) are dispatched **after** the integration test that surfaces the bug exists on `main`. The test asserts the failing behavior first, the fix lands second on a separate branch. This prevents "test passes because the bug it was supposed to catch was silently fixed in the same PR".
- Never let an agent run `git push --force` or `gh pr merge`. Merges are operator-only.
- Worktree cleanup: if an agent finishes without a PR (it returned mid-edit, hit token cap, etc.), the launcher MUST `git worktree remove` and `git branch -D` to keep the tree tidy.

## 3. Worktree hygiene

Project convention:

```
/private/tmp/wt-<short-name>          # default location, auto-clean on agent exit
```

NEVER:
- Reuse the main checkout (`/Users/kvaps/git/blockstor`) for an agent's work.
- Cherry-pick from a worktree directly into `main` if the source branch has uncommitted state — first commit on the branch, then cherry-pick.
- Force-strip CRD finalizers as a shortcut on the stand. Use `kubectl delete --grace-period=0` only after the real cleanup is verified.

## 4. Failure modes seen in practice (and the patch)

| Failure | Patch |
|---|---|
| Agent runs out of tokens mid-edit, branch has uncommitted WIP | Launcher pops the stash, finishes the work, commits. NEVER discard mid-edit work without inspection. |
| Two agents in parallel on overlapping files | Strict per-group scope above. If a group needs harness extension, ONLY that agent touches harness. |
| Agent commits something then says "Done" without pushing | Launcher verifies `git log origin/<branch>` matches local. |
| Agent reports "tests pass" but didn't run them | DoD line requires the exact `go test` invocation in the PR body — operator greps for it. |
| Agent rewrites a file from scratch losing project conventions | Prompt instructs: "extend existing patterns, do not regenerate". Operator diffs against `main` before approving merge. |
| Linter warnings already on `main` confuse the agent | Prompt explicitly says: "ignore pre-existing lint findings; only fix lint introduced by your diff". |

## 5. Prompt template

```
You are working in /Users/kvaps/git/blockstor.

Read FIRST:
  - docs/agent-playbook.md (this file)
  - docs/test-strategy.md, especially Group <X>
  - tests/integration/harness/*.go (read every file)
  - tests/integration/smoke_test.go (the canonical example)

Your scope: implement Group <X> from the test plan as
tests/integration/group_<x>_test.go. <N> tests total.

Strict rules:
  - Do NOT edit any harness file. If you need a helper, add it to
    tests/integration/harness/group<x>_helpers.go (new file).
  - Branch: feat/integration-group-<x>-<slug>. Isolation: worktree.
  - Commit message format:
      test(integration): Group <X> — <slug> (<N> tests)
      Co-Authored-By: Claude <noreply@anthropic.com>
  - Open a draft PR with `gh pr create --draft`. Body lists each
    test name with the bug-guard reference (column 3 in the table).
  - Flip the Group <X> tracker row in docs/test-strategy.md from
    `pending` to `done` in the SAME PR.

Definition of done:
  - `go test -tags=integration ./tests/integration/... -run '^TestGroup<X>' -count=1`
    passes locally (include the command output in the PR body).
  - CI green on the PR.

Do NOT:
  - Touch any file outside tests/integration/.
  - Touch any harness file.
  - Merge the PR.
  - Force-push.
  - Speculate when the harness lacks what you need — stop and ask.

Report back: PR URL, list of test names, time to first green CI.
```

## 6. Phase 0 vs Phase 1 vs Phase 2

The plan executes in three phases. Never overlap phases.

| Phase | Agents | Scope |
|---|---|---|
| **Phase 0** | 1 agent | Build the harness scaffold + one smoke test that proves envtest+manager+REST+linstor-CLI works end-to-end. Until Phase 0 lands on `main`, no other agent runs. |
| **Phase 1** | 12 agents in parallel | Groups A–L. Each agent only touches `tests/integration/group_*_test.go` for its group. No harness edits. |
| **Phase 2** | up to 10 agents | Fix code for any failing test from Phase 1. Each fix on its own branch, fixing exactly one bug, with the failing integration test as the regression guard. |

Phase 2 agents are gated on Phase 1 PRs being green-or-failing-for-a-real-reason (not flaky). A flaky Phase 1 test means the harness needs work — back to Phase 0, not Phase 2.

## 7. After-action review

When all Group PRs land, the launcher updates `docs/test-strategy.md`:
- Tracker rows all `done` → strike through the planning section, leave only the executed groups
- Add a "What we caught" section listing which Phase 2 fixes the new integration tests surfaced

If Phase 1 surfaced zero new bugs, that is itself a result worth recording — it means our existing code was robust where we feared.
