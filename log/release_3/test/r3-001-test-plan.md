# R3-001 test plan — Release 2 gap corrections and closure reconciliation

Tester: sw-mike | Date: 2026-10-06
Branch: `codex/r3-001-corrections` (from `dev` @ `2dee90e`, tip `6ffc090`)
Requirements: `log/release_3/prd.md` | Task: `log/release_3/tasks.md` R3-001
Characterization baseline: see `r3-001-baseline.md` (135 tests OK, zero
warnings; three defect areas reproduced with verbatim evidence).

## Status of this document

This is a **test plan only**. Every case is marked:

- **[BASELINE-READY]** — can be executed against the current baseline
  without any new production code (compatibility re-runs, closure/commit
  reconciliation via git/gh, documentation wording checks). Cases that
  currently **fail** against the baseline are expected-to-fail until the
  R3-001 implementation lands; they are the minimal reproductions with
  assertions required by repo AGENTS.md for behavior fixes.
- **[PLAN]** — requires the R3-001 implementation (and possibly the
  approved detailed design naming the corrected bridge pattern) before
  it can pass.

No production API names are hard-coded where the fix mechanism is design
work: bridge cases pin *observable behavior* (parameter read-back equals
the object's authoritative input) and accept whichever wiring the
approved design documents.

## Acceptance criteria under test (verbatim from `log/release_3/prd.md`)

> **R3-AC-07** | Close Release 2 gaps | Bridge readback follows reset,
> direct changes and shared controllers; mixed callback keys give the
> documented error; reset documentation states deterministic/factory
> limitations accurately; Release 2 board agrees with verified closure
> evidence.

> **R3-AC-08** | Compatibility and usable delivery | Existing standalone
> object/device/theory behavior and imports remain compatible except the
> explicitly corrected gaps. A worked connected example demonstrates the
> clock, delayed feedback, mock interaction and reset. Regression,
> compile/import, warning and candidate CI evidence precede integration;
> architecture describes implemented capability only after delivery.

R3-001 covers **R3-AC-07 in full** and the **standalone portion of
R3-AC-08**: standalone object/device/theory compatibility and imports,
plus regression/compile/import/warning evidence. The worked *connected*
example belongs to R3-004 (and connected-mode readback semantics to
R3-003); this plan explicitly excludes them.

The PRD gap statements pinned by this plan (`prd.md` "Remaining Release 2
gaps", lines 110–126):

1. *Bridge readback:* control reads reflect authoritative object inputs
   after reset, direct input changes and writes through another
   controller; standalone behavior characterized and distinguished from
   pending-source readback in a connected run.
2. *Malformed callback keys:* wrong/missing/extra keys, **including mixed
   string and non-string keys**, produce the documented `ValueError`
   rather than an incidental sorting `TypeError`; non-mapping results
   remain `TypeError`.
3. *Reset claims:* remove claims that any successful reset is
   behaviorally identical to fresh construction, or that failed reset
   guarantees identical future observations, when user
   factories/callbacks have external state. Promise
   preservation/restoration of owned inputs/states only; reproducible
   observations require deterministic callbacks and reproducible
   factories.
4. *Closure evidence:* reconcile stale Release 2 board statements with
   its committed acceptance/completion records — documentation
   reconciliation, recording verified commit references and
   distinguishing acceptance from promotion.

## Planned test files

| File | Covers |
| --- | --- |
| `tests/test_tu_simulation.py` (extend) | R3-TC-07e–07l: malformed callback keys, reset claims characterization (new test methods appended; no existing method is weakened) |
| `tests/test_tu_simulation_integration.py` (extend) | R3-TC-07a–07d: standalone bridge readback (new test methods; SIM-TC-06a may need rebinding to the corrected documented pattern — see risk 3) |
| (no new file) | R3-TC-08a–08d: full-suite regression, compile/import/warning gate, dependency check |
| (manual record checks) | R3-TC-07n–07t: documentation wording (reset claims, guide §9 bridge) and Release 2 closure reconciliation — record checks, not `unittest` cases |

All synthetic fixtures; stdlib + NumPy only; temp dirs only; no real
instruments; UI/hardware gates N/A.

---

## R3-AC-07 — area 1: standalone bridge readback

**Purpose:** the documented `Device`/`Parameter` bridge (user guide §9)
must give control reads that reflect the object's authoritative input
store after (a) direct `set_input` changes, (b) `reset()`, and (c) writes
through another controller sharing the same object. Baseline evidence
(`r3-001-baseline.md` §A): all three scenarios currently return the
parameter's stale stored value (guide line 243 overpromises: "equals the
object's current (pending) input").

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-07a **[PLAN]** (fails on baseline) | Read-back follows direct input changes | Accumulator object; `drive` control parameter wired per the corrected documented bridge pattern | `drive(1.0)`; then `obj.set_input('u', 5.0)`; read `drive()` and `obj.get_input('u')` | `drive()` equals the authoritative `obj.get_input('u')` (5.0); exactly one `set_input` per parameter set; no evolution on set or read |
| R3-TC-07b **[PLAN]** (fails on baseline) | Read-back follows reset | Same fixture; evolve once away from the initial condition; `drive(3.0)` | `obj.reset()`; read `drive()` and `obj.get_input('u')` | `drive()` equals the restored initial input (0.0) — reset makes the stale stored value visible, so this case fails on the baseline |
| R3-TC-07c **[PLAN]** (fails on baseline) | Read-back follows a second controller | Same object shared by `drive1` and `drive2` control parameters | `drive1(1.0)`; `drive2(2.0)`; read `drive1()`, `drive2()`, `obj.get_input('u')` | Both parameters' reads equal the authoritative input (2.0); the object's single input store is the one source of truth |
| R3-TC-07d **[BASELINE-READY]** (passes on baseline; keep passing) | Rejected set keeps parameter and object consistent | Same fixture | `drive(1.0)`; attempt `drive('not-a-number')`; catch; read `drive()` and `obj.get_input('u')` | Parameter validator rejects before the bridge (`TypeError` from `ValNumber`); both stores remain 1.0; no partial write. Verbatim baseline evidence in `r3-001-baseline.md` §A (a4) |
| R3-TC-07t **[PLAN]** (expected-to-fail until the guide correction lands) | Bridge documentation wording, guide §9 (manual record check) | Approved detailed design naming the corrected bridge pattern; corrected `docs/user-guide/simulation.md` §9 | Checklist: (a) §9 input-parameter text states the corrected read-back semantics — a control read via `get()` reflects the object's **authoritative input store** (after direct `set_input`, `reset()` and cross-controller writes) — with the overpromise at today's line 243 ("equals the object's current (pending) input") gone; (b) §9 explicitly distinguishes **standalone** readback (reads follow the authoritative input store) from **pending-source** readback across a connected delayed edge, deferring the connected half's semantics to R3-003 without stating them in R3-001's scope; (c) if the approved design changes the wiring (e.g. a `before_get` closure or bridge support), the two-gate validation asymmetry paragraph and the guide error table (§6) still match the implemented `Parameter.set` order (permission → validate → decode → `before_set` → store → `after_set`) and the implemented error contract — no stale ordering or error-class claim survives | Each checklist item verified against the guide with file/line citations recorded in the test-results document; nothing silently waived |

Notes:

- The fix may change the *documented wiring* (e.g. adding a `before_get`
  closure reading `obj.get_input`) or introduce bridge support; the tests
  pin only observable read-back equality. Implementation-phase tester
  rebinding of fixtures to the approved design is expected.
- Connected-mode readback (pending-source semantics across a delayed
  edge) is **out of scope** here per tasks.md — it belongs to R3-003;
  R3-001 only needs standalone behavior characterized and the guide
  wording distinguishing the two — the wording half is pinned by
  R3-TC-07t.

## R3-AC-07 — area 2: malformed callback keys

**Purpose:** wrong/missing/extra keys — specifically **mixed string and
non-string keys** — must raise the documented `ValueError`; non-mapping
results stay `TypeError`. Root cause (baseline §B): `object.py` lines
775–776 sort the missing/extra key sets; sorting a mixed-type set raises
an incidental `TypeError: '<' not supported between instances of 'int'
and 'str'`. Baseline also found **zero existing test coverage** for
callback-key errors of any shape.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-07e **[PLAN]** (fails on baseline) | Mixed str/non-str extra keys, evolve | Accumulator object; `evolve` returns `{'x': …, 'z': …, 2: …}` | `evolve_once()`; catch | `ValueError` (documented key-discrepancy error) naming the missing/extra keys; **not** `TypeError`; committed state unchanged |
| R3-TC-07f **[PLAN]** (fails on baseline) | Mixed str/non-str extra keys, observe | Object whose `observe` returns `{'y': …, 'z': …, 2: …}` | `observe_outputs()`; catch | `ValueError`, not `TypeError`; no committed store touched |
| R3-TC-07g **[PLAN]** (passes on baseline; keep passing) | Single non-str extra key stays a named `ValueError` | `evolve` returns `{'x': …, 2: …}` | `evolve_once()`; catch; inspect message | `ValueError` whose message names the extra key (`[2]`); guards against overcorrection that drops key names or flips this to `TypeError` |
| R3-TC-07h **[PLAN]** (passes on baseline; keep passing) | Pure-string missing/extra keys | `evolve` returns `{}` or `{'x': …, 'z': …}` | `evolve_once()`; catch | `ValueError` naming missing/extra per the existing contract |
| R3-TC-07i **[PLAN]** (passes on baseline; keep passing) | Non-mapping results remain `TypeError` | `evolve` returns a list; `observe` returns an int | `evolve_once()`; `observe_outputs()`; catch each | `TypeError` ("must return a mapping") for both — the PRD explicitly preserves this |
| R3-TC-07j **[PLAN]** | Failed evolve atomicity under malformed keys | Object with committed state; `evolve` returns mixed extra keys | Snapshot state; `evolve_once()`; catch; re-read state | Committed state bit-identical to snapshot (the existing build-then-swap contract must survive the fix) |

## R3-AC-07 — area 3: reset claims

**Purpose:** documentation must stop claiming (i) successful reset is
behaviorally identical to fresh construction, and (ii) failed reset
guarantees identical future observations, when factories/callbacks carry
external state. Owned inputs/states preservation/restoration is the only
promise; reproducible observations require deterministic callbacks and
reproducible factories. Claim sites (baseline §C): `object.py` line 660
and lines 666–669, guide §5 line 132, `arch.md` line 279.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-07k **[BASELINE-READY]** (passes; regression) | Reset restores owned initial condition with reproducible factories | Existing SIM-TC-04a/05i fixtures (deterministic factories/callbacks) | Re-run as part of the suite | Reset restores the documented initial inputs/states and replay reproduces observations — existing assertions must keep passing |
| R3-TC-07l **[PLAN]** (new characterization with assertions) | Reset re-invokes factories; value tracks the factory, not history | Factory with a call counter (external state) declared as an input's initial value | Construct (invocation 1); `set_input` away; `reset()`; read input; `reset()` again; read input | Each `reset()` re-invokes the factory once per factory-declared variable (invocation count increments by exactly one per reset); committed input equals the factory's *current* result — pinning that reset restores owned stores from their declared sources and makes no equivalence claim to fresh construction |
| R3-TC-07m **[PLAN]** (new characterization with assertions) | Failed reset: owned stores and exception identity preserved | Factory armed to raise on a chosen invocation (public construction API only, no monkeypatching) | Snapshot inputs/states; arm; `reset()`; catch; read stores; disarm; `reset()` again | Identical exception object propagates (`assertIs`); committed inputs/states equal the pre-reset snapshot exactly; a later `reset()` succeeds — pins the owned-state promise after a failed reset |
| R3-TC-07n **[BASELINE-READY]** | Reset documentation wording (manual record check) | Current `object.py` `reset()` docstring, guide §5, arch.md Simulation section | Checklist: no unqualified "behaviorally identical to a newly constructed one"; failed-reset wording promises owned inputs/states only and states that subsequent *observations* require a deterministic `observe`; reproducible-observation condition (deterministic callbacks + reproducible factories) stated | Each document states the deterministic/factory limitation accurately; nothing silently waived. Manual check recorded in the test-results document with file/line citations |

## R3-AC-07 — area 4: Release 2 closure reconciliation (records vs commits)

**Purpose:** the Release 2 board/summary statements must agree with
verified closure evidence; record verified commit references;
distinguish acceptance from promotion. Baseline §D already executed the
verification commands; the plan below pins what the corrected record
must contain. These are **manual record checks** (git/gh verification),
not `unittest` cases.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-07o **[BASELINE-READY]** | Cited Release 2 commits exist and match their claims | Board/summary/acceptance cited hashes | `git log -1` each cited hash; compare message vs the board's description | Every cited commit exists with a matching message (baseline §D: all 20 hashes verified present; content mismatches recorded as findings REC-2/REC-3 below) |
| R3-TC-07p **[BASELINE-READY]** | CI evidence reference corrected | gh CLI access | `gh run view 37298623628` and `gh run view 37298431248 --json headBranch,headSha,conclusion,jobs` | The record distinguishes the pre-merge task-branch run (`37298431248`, branch `codex/sim-001-simulation-foundation`, head `cd90ff6`, both matrix jobs success) from the post-merge dev run (`37298623628`, branch `dev`, head `9408c79`). Baseline finding REC-1: the board and acceptance cite the post-merge run as "before the dev merge" — must be reconciled, not silently waived |
| R3-TC-07q **[BASELINE-READY]** | Board acceptance status reconciled | `sprint-board.md` vs `acceptance.md` | Compare statements | Board header "release close pending" / "acceptance ... recorded as pending" vs committed verdict ACCEPTED at `c69272a` (baseline finding REC-2): board updated to reflect the committed acceptance record, still distinguishing acceptance from `main` promotion (promotion remains a separate, unrecorded step — no invented promotion) |
| R3-TC-07r **[BASELINE-READY]** | Sprint summary baseline reference reconciled | `sprint_summary.md` line 4 vs `sim-001-baseline.md` | Compare | "Baseline: `80b0b05`" (a Release 1 record commit) vs the baseline record "branch `codex/sim-001-simulation-foundation` (from `dev` @ `f04e783`)" (baseline finding REC-3): summary corrected to the actual parent (`f04e783`), with `80b0b05` explainable as an earlier dev ancestor if intended |
| R3-TC-07s **[BASELINE-READY]** | No invented promotion evidence | git history | `git log main..dev --oneline` / branch inspection | No record claims `dev`→`main` promotion occurred; Release 2 promotion remains unrecorded/pending; acceptance and promotion stay distinguished in every corrected record |

## R3-AC-08 (standalone portion) — compatibility and evidence

The connected worked example, clock, delayed feedback and connected-mode
mock interaction are **excluded** (R3-003/R3-004 scope).

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-08a **[BASELINE-READY]** | Pre-implementation regression | Current branch | `python -m unittest discover -s tests -p 'test_*.py'` | 135 tests, OK, exit 0 — matches `r3-001-baseline.md`; recorded pre-implementation |
| R3-TC-08b **[PLAN]** | Post-implementation regression + gates | R3-001 implementation on this branch | Re-run unittest; `python -m compileall -q softlab`; `python -c "import softlab; print(softlab.__version__)"`; `python -W error::Warning -m unittest discover ...` | All pre-existing tests pass (135 + new R3-001 cases); zero warnings in all runs; compile exit 0; import smoke clean; no OBS-004/005/006 or DEFECT-2 warning fires (any firing recorded verbatim and dispositioned through the workflow) |
| R3-TC-08c **[PLAN]** | Standalone compatibility beyond the corrected gaps | Implementation present | Existing `tests/test_tu_*.py` (station/device/parameter/visa/theory contract suites) pass unmodified; public import identity `softlab.tu.simulation.SimulatedObject is softlab.tu.simulation.object.SimulatedObject` | Standalone object/device/theory behavior and imports compatible **except** the explicitly corrected gaps (bridge readback, mixed-key error) — no other behavior change |
| R3-TC-08d **[PLAN]** | No new required dependencies | Implementation present | `git diff dev...HEAD -- pyproject.toml setup.py` | No new required dependencies; optional extras untouched |
| R3-TC-08e **[PLAN]** | Scope gate (reviewer-owned checklist, not in automated tally) | Implementation complete | `git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui softlab/mu` and under `softlab/tu/` | Production runtime changes confined to `softlab/tu/`; tests/docs/log changes allowed outside it; recorded in the test-results document (CHK-08-1, reviewer-owned, same convention as Release 2 CHK-06-1) |

## Traceability summary

| AC (R3-001 scope) | Test cases |
| --- | --- |
| R3-AC-07 (bridge readback) | R3-TC-07a–07d (automated); R3-TC-07t (manual guide §9 wording check) |
| R3-AC-07 (malformed callback keys) | R3-TC-07e–07j (automated) |
| R3-AC-07 (reset claims) | R3-TC-07k–07m (07k–07m automated; 07n wording check is manual — see note) |
| R3-AC-07 (closure evidence) | R3-TC-07o–07s (manual record checks via git/gh) |
| R3-AC-08 (standalone portion) | R3-TC-08a–08d (automated); CHK-08-1 (reviewer checklist) |

Manual vs automated, explicitly: automated `unittest` cases are
R3-TC-07a–07m and R3-TC-08a–08d; manual record checks are R3-TC-07n
(reset documentation wording), R3-TC-07t (guide §9 bridge wording) and
R3-TC-07o–07s (closure reconciliation), plus reviewer checklist
CHK-08-1. Manual checks require only git, gh CLI access and file
reading — no new tooling.

## Risks and notes for the design/implementation review

1. **Bridge fix mechanism is design work.** The tests pin read-back
   equality, not wiring. If the design adds `before_get` to the control
   parameter (reading `obj.get_input`), R3-TC-07d (validator fires
   before the bridge) and the two-gate asymmetry wording must be
   re-verified; the guide's §9 pattern and error table are part of the
   correction scope (the wording half is pinned by R3-TC-07t).
2. **SIM-TC-06a pins today's stale contract.** `tests/test_tu_simulation_integration.py` line 99 asserts `drive() == 1.0` immediately after the set — that still holds under a corrected bridge, but if the design changes the documented pattern the tester will rebind the fixture and record the rebind in the results document (test code may change outside `tu`).
3. **Error message content for mixed keys.** R3-TC-07e/07f require
   `ValueError` and a useful message; the exact rendering of non-string
   key names in the message is implementation choice, asserted only to
   be a `ValueError` naming the discrepancy.
4. **Docstring corrections inside `softlab/tu/` are production edits** —
   they are explicitly in scope ("includes existing docstring/guide/architecture
   wording corrections", tasks.md), confined to `tu` for `object.py`,
   with guide/arch/log outside it.
5. **No retroactive repair.** Closure reconciliation corrects statements
   about evidence; it must not rewrite historical records' verdicts or
   invent a `main` promotion (PRD line 125; R3-TC-07s pins this).

## Revision history

- **2026-10-06 — Revision 1** (tester: sw-mike): initial plan,
  written against the characterization baseline recorded the same day in
  `r3-001-baseline.md`.
- **2026-10-06 — Revision 2** (tester: sw-mike): review issue 1
  closure — added sibling manual record check R3-TC-07t (guide §9
  bridge wording) pinning the corrected read-back wording, the
  standalone vs pending-source distinction, and the conditional
  two-gate asymmetry / error-table wording check; updated the
  R3-AC-07 bridge-readback traceability row, the manual-vs-automated
  paragraph, the planned-files table row (range corrected to
  `07n–07t`), and risk note 1 cross-reference. No existing case ID or
  assertion changed.
