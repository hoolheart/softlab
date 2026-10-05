# SIM-001 test plan review — implementability

Reviewer: sw-tom (implementer) | Date: 2026-10-05
Branch: `codex/sim-001-simulation-foundation` (HEAD `86cddb4`)
Input reviewed: `log/release_2/test/sim-001-test-plan.md` (with
`sim-001-baseline.md`, `log/release_2/prd.md`, `log/release_2/tasks.md`,
`AGENTS.md`)

**Verdict: CHANGES-REQUESTED** — one blocker (missing coverage for a
PRD-required behavior), three minors. The plan is otherwise implementable:
every [PLAN] case can be written as a `unittest`-compatible test with the
existing stack (stdlib + NumPy), with production changes confined to
`softlab/tu/` and zero edits to `huo`/`jin`/`shui`/`mu`.

## What I inspected

- `log/release_2/test/sim-001-test-plan.md` (full), `sim-001-baseline.md`
  (full), `log/release_2/prd.md` (SIM-AC-01–08, scope exclusions),
  `log/release_2/tasks.md` (planned `softlab/tu/simulation/` package,
  serial gates), `AGENTS.md` (repo conventions, verification commands,
  scheduler/global-state warnings).
- Existing public APIs I will later bridge (read-only):
  - `softlab/huo/process/common.py` — `AtomJob` (hooks
    `hook_before_set`/`hook_after_set`/`hook_before_get`/`hook_after_get`,
    no-arg synchronous callables invoked once per non-dry-run point inside
    `body()`), `AtomJobSweeper` (final dry-run execution invokes **no**
    hooks), `Counter`, `Scanner`, `count()`, `scan()`,
    `_assign_group_and_record` (in-memory `DataRecord` when no backend
    group is given; `proc.record` readable post-run).
  - `softlab/huo/process/process.py` — `run_process` (requires a started
    `Scheduler`; calls `process.reset()` first), `Process` interface.
  - `softlab/huo/scheduler` — `get_scheduler()` global singleton.
  - `softlab/tu/station/parameter.py` — `Parameter` constructor
    (validator/decoder/encoder/`before_set`/`after_set`/`before_get`
    hooks; set path = settable check → validate → decode → `before_set`
    → store → `after_set`).
  - `softlab/tu/station/device.py` — `Device` (base device is already
    virtual; `add_parameter`, delegated attribute access).
  - Precedent tests: `tests/test_compatibility.py`,
    `tests/test_tu_integration.py` (existing `count`/`scan` +
    `run_process` + scheduler start/stop pattern in `setUp`/`tearDown`;
    a test-local `VirtualDevice(Device)` fixture; `before_get`-based
    getter wiring).

## Issue list

### 1. [blocker] SIM-AC-05 requires "output failures" coverage; no case provides it

- **Location:** SIM-AC-05 / cases SIM-TC-05a–05g (gap), traceability table.
- **Description:** PRD SIM-AC-05 (Must) states: "Input validation, **evolution
  and output failures** are visible, with original callback exceptions
  preserved." The plan covers input-update failure atomicity (05a) and
  `evolve` failure atomicity (05b) only. No case exercises the observation
  callback `G` raising: visibility of the original exception, and that a
  failed observation leaves committed state unchanged and does not corrupt
  subsequent observations. The traceability table claims SIM-AC-05 ↔
  SIM-TC-05a–05g, so this gap is not visible there either.
- **Requested change:** add one case, e.g. **SIM-TC-05h [PLAN]** — model
  whose observation function `G` raises for a specific committed state;
  assert the original exception object propagates (identity preserved, or
  documented wrapper chaining via `__cause__`, matching whichever contract
  05a pins), committed state and prior observations are unchanged, and a
  later valid observation still works. Update the traceability table.

### 2. [minor] SIM-TC-06f is not a mechanically executable test

- **Location:** SIM-TC-06f.
- **Description:** "git diff check in test documentation / review" cannot be
  asserted by `unittest`; as written it will either be skipped silently or
  produce a vacuous pass. The intent (zero `huo` production edits) is real
  but belongs to the review/inspection gate, not the automated suite.
- **Requested change:** reclassify SIM-TC-06f as an explicit checklist item
  owned by the tester/reviewer in the test-results record (and keep the
  automated integration file importing `huo` only via
  `softlab.huo.process` public names as supporting evidence). Do not count
  it as an automated test in the pass/fail tally.

### 3. [minor] Reset-failure atomicity is deferred but not tracked as a pending case

- **Location:** SIM-AC-05 (PRD: "a failed input update, evolution **or
  reset**"), open question 4, SIM-TC-04d.
- **Description:** SIM-TC-04d covers "reset succeeds after a failed
  evolution", not "a reset that itself fails leaves no partially committed
  state". Open question 4 correctly frames this as a design decision, but
  nothing tracks the obligation to add the case once the reset error
  contract exists — AC-05 would otherwise reach execution partially
  covered.
- **Requested change:** record in the plan (e.g. under open questions or a
  "deferred cases" note) that a reset-failure atomicity case must be added
  and bound to the approved reset contract before the SIM-AC-05 gate is
  claimed complete.

### 4. [minor] Scan stepping-hook binding needs an ordering constraint stated now

- **Location:** SIM-TC-06e design dependency paragraph.
- **Description:** The planned assertion "evolve exactly once per point in
  a process hook" is feasible with existing API, but the design could
  plausibly bind the wrong hook for `scan`: `hook_before_set` fires
  **before** the new point value is applied to the setter parameter, which
  would evolve with the previous input — a subtle off-by-one that the
  "recorded outputs match the analytic sequence" assertion would catch only
  if the analytic sequence is computed correctly. For `count` any of
  `hook_before_get`/`hook_after_get` is safe.
- **Requested change:** add one sentence of design input: for `scan`,
  stepping must be bound to `hook_after_set` or `hook_before_get` (after
  the point value is applied, before/with observation); `hook_before_set`
  is not a valid stepping point. Also note hooks are no-arg synchronous
  callables (closure over the simulation object), invoked exactly once per
  non-dry-run point — the terminal dry-run sweep execution invokes no
  hooks, so evolve count equals the point count exactly.

## Answers to the four open design questions (implementability perspective)

Recommendations only; the decision belongs to sw-celeste's detailed design
and sw-jerry's review. All four options are implementable either way — these
are cost/risk preferences.

1. **Pending-input publication (SIM-TC-03c).** Recommend *pending until
   explicit evolution*: assignment stores the pending input, `evolve`
   consumes (a copy of) it. This matches the PRD contract
   `x_next = evolve(u, x_previous)` most directly, gives one clean
   commit boundary per operation (cheapest route to SIM-TC-05a atomicity),
   and makes SIM-TC-02a/03c mechanically testable via call counters. The
   "committed on assignment" alternative is also implementable but needs an
   extra rule about which input a failed `evolve` reports as "previous".
   Either way, the design must state it; the plan already frames this
   correctly.
2. **Copy vs view for inspection (SIM-TC-05d).** Recommend *copies on the
   way in and on the way out* (e.g. `np.array(..., copy=True)` semantics).
   One mechanism then uniformly satisfies 05c (caller-supplied), 05d
   (returned), and 05e (callback arguments — copies out before publish),
   and needs no read-only-view bookkeeping that could leak mutability via
   `numpy` views. Read-only views are feasible but strictly more code for
   no test-visible benefit.
3. **Concrete stepping hook (SIM-AC-06d planned assertion).** Feasible with
   **zero `huo` edits** — verified against the actual API: `count()` and
   `scan()` already accept `hook_before_set`/`hook_after_set`/
   `hook_before_get`/`hook_after_get` as public keyword arguments, passed
   through to `AtomJob`, where they are invoked synchronously, once per
   non-dry-run point; the terminal dry-run execution calls none of them.
   Recommend `hook_before_get` (count and scan) or `hook_after_set` (scan
   only) bound to a test/example-side closure calling `evolve` once — see
   issue 4 for the ordering constraint. No custom `Process` subclass or
   adapter is needed for the MVP worked example; run via existing
   `run_process(proc, scheduler, verbose=False)` with the
   `get_scheduler()` start/stop pattern already used in
   `tests/test_compatibility.py` and `tests/test_tu_integration.py`.
4. **Reset atomicity contract.** Recommend designing reset as
   *build-new-initial-state-then-swap* (fresh copies of all declared
   initial values), which is atomic by construction and needs no rollback
   path to test. Then SIM-TC-04d plus the deferred reset-failure case
   (issue 3) become straightforward. If the design instead mutates in
   place, it must specify the failure contract explicitly — that is more
   implementation and test surface for no user-visible gain.

## Over-testing / under-testing assessment

- **Under-testing:** issue 1 (output/observation failure) is the only
  PRD-required behavior with no covering case. Issue 3 tracks the deferred
  reset-failure portion of the same AC sentence.
- **Over-testing:** none found. Identity-preservation of the original
  exception in SIM-TC-05b is the strongest reading of "original callback
  exceptions preserved", but it matches the Release 1 `Reading` precedent
  ("identity preserved, never wrapped") and is trivially implementable via
  bare re-raise after a non-publishing `evolve`; keep it. SIM-TC-04c's
  "identity checks where inspection allows" is appropriately hedged against
  the copy-return design. SIM-TC-02d "byte-for-byte reproducible" holds for
  exact equality of float outputs under deterministic callbacks in one
  environment; acceptable as written.

## Implementability confirmation (per review question 1)

All [PLAN] cases are writable as `unittest`-compatible tests under `tests/`
with stdlib + NumPy only: fixtures are plain Python callables and
`np.ndarray`s; assertions are concrete (call counts, exact values,
exception identity, `id()`/equality checks, recorded `DataRecord` tables
via `proc.record.table`). No case requires editing `huo`/`jin`/`shui`/`mu`
production modules; SIM-TC-07d additionally pins "no new required
dependencies". The integration file must follow the established scheduler
lifecycle discipline (`get_scheduler()` started in `setUp`, stopped in
`tearDown`, `verbose=False`) per AGENTS.md's global-state warning — the
plan does not contradict this; noting it as an implementation constraint.
