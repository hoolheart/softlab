# R3-003 test plan review — implementability

Reviewer: sw-tom (implementer) | Date: 2026-10-11
Branch: `codex/r3-003-coordination` (reviewed at `6fcdb4e`, synced with
`origin/codex/r3-003-coordination`)
Input reviewed: `log/release_3/test/r3-003-test-plan.md` Revision 1 (with
`r3-003-baseline.md`, `log/release_3/prd.md` R3-AC-01–06/08 (verbatim),
tasks.md R3-003 row + design-gate requirements (lines 41–44),
`log/release_3/architecture-plan.md`, and the cited code in
`softlab/tu/simulation/object.py`, `softlab/tu/simulation/builder.py`,
`softlab/tu/station/station.py`, `tests/test_tu_simulation.py`,
`tests/test_tu_station_objects.py`)

**Verdict: CHANGES-REQUESTED** — four minor issues, none structural. The
criterion coverage, failure-mode matrix, baseline consistency, scope
containment and design-gate mapping are all sound; every one of the 40
automated cases is implementable as written once the capability exists.
The issues are: one over-constrained data row (R3-TC-03b pins rejection
of real-valued scalar types the AC text would permit), two under-tested
permissions (the positive half of PRD line 85; device/endpoint
registration order in R3-TC-01d), and one internal tally typo in the
planned-test-files table. All are tester-side rewordings; no issue
requires re-baselining or scope change.

## What I inspected

- Test plan (full read, 327 lines) and characterization baseline (full
  read, 186 lines); PRD R3-AC-01–06 and the R3-AC-08 compatibility
  portion verbatim against the plan's quoted text — the plan's AC
  quotations match `prd.md` lines 139–146 exactly.
- tasks.md R3-003 scope row and the design-gate paragraph (lines 41–44);
  architecture-plan mechanisms (lines 13–94) against the plan's
  "Status of this document" and risk-note claims.
- Release 3 review convention (`r3-002-test-plan-review.md`).
- Code spot-checks against every line citation the baseline and plan
  rely on:
  - `softlab/tu/simulation/` contains exactly `__init__.py`,
    `object.py` (804 lines), `builder.py` (101 lines) — matches
    baseline §2 verbatim;
  - grep over `softlab/` for
    `coordinator|coordinated|connection|sim_dt|edge|snapshot|tick|pending|delivered`:
    in `softlab/tu/simulation/` only the unrelated set_input docstring
    word "pending publication" (object.py line 481, read verbatim), and
    in `station.py` only the forward-looking `rm_object` docstring
    (lines 205–208, read verbatim: "removal of an object enrolled in
    an active coordinated simulation will be rejected by the future
    ownership layer (R3-003 scope)"). GAP-1 "entirely absent" claim
    **confirmed independently** — no coordinator/connection/clock/
    pending/delivered artifact exists anywhere in `tu`;
  - `softlab/tu/simulation/object.py` — module docstring "no dedicated
    clock API" (lines 10–12, verbatim), `_copy_value` ndarray
    copy-on-boundary (lines 196–219, verbatim — anchor 3 mechanism),
    `evolve_once` "identical exception object propagates (never caught,
    wrapped or replaced)" (lines 557–561, verbatim — anchor 2),
    `reset` build-then-swap with identical-exception guarantee and
    narrowed claims (lines 649–704, verbatim — baseline §3 row 4 and
    anchor 5);
  - `softlab/tu/station/station.py` — R3-002 membership block
    (`add_object`/`objects`/`object`/`rm_object`/`build_object`,
    lines ~175–255, exercised live), `_default_station` module global
    at **line 257 exactly** as risk note 7 cites;
  - `softlab/tu/simulation/builder.py` — `ObjectBuilder` model identity
    + empty-model `ValueError` + `build` interface (lines 14–40), the
    R3-002-delivered convention the plan re-runs as regression;
  - `tests/test_tu_simulation.py` — the accumulator fixture
    (`x' = x + u`, `y = 2x`, evolve/observe counters, `seen` log) and
    the integrator fixture (declared `dt` input) the plan's fixture
    convention references — both exist exactly as described, so "no
    new fixture machinery" is accurate for the scalar case and the
    dt-declaring model case;
  - `tests/test_tu_station_objects.py` — `patch.dict(..., clear=True)`
    registry isolation pattern (lines 172–210) the risk note 7
    isolation discipline mirrors;
  - Fresh regression re-run on this branch:
    `python -m unittest discover -s tests -p 'test_*.py'` → **Ran 165
    tests, OK, exit 0** — matches baseline §1 and R3-TC-08a's
    expectation verbatim;
  - R3-TC-08c's five named suites (`test_tu_simulation*.py`,
    `test_tu_station_objects.py`, `test_tu_contracts.py`,
    `test_tu_descriptions.py`, `test_tu_lifecycle.py`) and R3-TC-08e's
    pathspec inputs (`pyproject.toml`, `setup.py`) all exist — every
    evidence command in the plan is runnable as written.
- Tally: automated cases R3-TC-01a–e (5) + 02a–f (6) + 03a–f (6) +
  04a–e (5) + 05a–g (7) + 06a–e (5) + 08a–f (6) = **40**, matching the
  review brief's expectation; per-section tables and the traceability
  summary are internally consistent on this (see issue 4 for the one
  table that is not).

## Issue list

### 1. [minor] R3-TC-03b: rejection of `Decimal`/NumPy-scalar pins type-strictness beyond R3-AC-03 and the architecture plan

- **Location:** R3-AC-03 table, R3-TC-03b (line 161).
- **Description:** the data-driven row requires that a `Decimal` value
  and a NumPy-scalar value be "explicitly rejected". R3-AC-03 (prd.md
  line 57) requires a "strictly positive finite real duration
  (booleans rejected)" — a `Decimal('0.5')` and `np.float64(0.5)`
  *are* positive finite reals, and the architecture plan (lines 73–78)
  adds only "explicit, finite, positive and non-boolean … never infer
  a variable by name" — nothing about exact types. As written, the
  case would fail a design that accepts real-valued scalars through a
  documented coercion policy, i.e. it constrains the detailed design
  beyond both source-of-truth documents. (The other listed values —
  `0`, `-1.0`, `nan`, `±inf`, `True`/`False`, `'0.5'`, `None`,
  `0.5+0j` — are all genuinely non-positive, non-finite, non-real or
  boolean and are correctly pinned rejections.)
- **Required change:** split the last data rows. Keep explicit
  rejection pinned for values that violate the AC as written (zero,
  negative, non-finite, boolean, non-real types). For real-valued
  scalars other than built-in `int`/`float` (`Decimal`, NumPy real
  scalars), pin only that behavior "matches the design's documented
  coercion/representability rule", with that rule's documentation
  location cited by CHK-03-1 (the same convention R3-TC-03f already
  uses for the representability policy) — or, if the tester intends
  type-strict rejection as the pinned behavior, record it explicitly
  as a bounded decision requiring PO/architect sign-off, since it goes
  beyond the AC text.
- **Owner:** sw-mike (tester).
- **Status: OPEN**.

### 2. [minor] Positive half of PRD line 85 — multiple mock controllers/readers within one simulation is asserted but never exercised

- **Location:** R3-AC-04 table, R3-TC-04c expected result (line 183);
  traceability row R3-AC-04.
- **Description:** R3-TC-04c's expected result states "multiple mock
  controllers/readers *within* S1's simulation are permitted (PRD line
  84–85)", but no case in the plan ever attaches two distinct mock
  endpoints to one participant in a single active simulation (R3-TC-02e
  writes twice to the *same* control; R3-TC-01b fans out to two
  distinct object destinations). A wrong implementation that rejects a
  second mock endpoint once a participant is enrolled — directly
  contradicting PRD line 85 — passes all 40 cases, since 04c only
  exercises the negative half (second *station* rejected).
- **Required change:** add one positive case (or extend R3-TC-04c's
  steps with a positive phase): enroll one object with two mock
  endpoints in one active simulation — e.g. two controls driving two
  different inputs, or a control and a measurement on the same
  participant — run a transition, and assert both endpoints functional
  (both pending writes delivered across their edges; measurement shows
  its delivered sample). Single-driver-per-input (R3-TC-05b) is
  unaffected: the two controls drive *different* inputs.
- **Owner:** sw-mike (tester).
- **Status: OPEN**.

### 3. [minor] R3-TC-01d varies object registration order but not mock-endpoint (device-side) registration order

- **Location:** R3-AC-01 table, R3-TC-01d (line 131).
- **Description:** R3-AC-01 requires uniform results "regardless of
  object/device registration or iteration order". R3-TC-01d pins two
  different *object* build/registration orders plus an inspection
  interleaving, but both runs register the mock control/measurement
  endpoints in the same order. Mock endpoints are station/coordinator-
  registered participants whose pending/delivered storage participates
  in the boundary snapshot, so endpoint registration order is within
  the AC's literal "device registration" wording; an implementation
  whose delivered-sample bookkeeping accidentally depends on endpoint
  insertion order would slip through.
- **Required change:** extend R3-TC-01d so the two runs also register
  the mock endpoints in reversed order (and, if the design supports
  multiple controllers/readers per participant, interleave those), with
  the bit-identical expectation unchanged — or record an explicit
  justification in the plan for why endpoint registration order cannot
  influence committed results.
- **Owner:** sw-mike (tester).
- **Status: OPEN**.

### 4. [minor] Planned-test-files table mislabels two case ranges (02a–02e, 04a–04c) — tally reads 37 instead of 40

- **Location:** "Planned test files" table (line 102).
- **Description:** the table row lists "R3-TC-01a–01e, 02a–02e,
  03a–03f, 04a–04c, 05a–05g, 06a–06e" = 31 coordination cases, but the
  R3-AC-02 section defines 02a–**02f** (six cases) and the R3-AC-04
  section defines 04a–**04e** (five cases); the traceability summary
  (lines 238–243) correctly says 02a–02f and 04a–04e. As written the
  table undercounts 3 cases (37 automated instead of 40) and
  contradicts the plan's own per-section tables — an implementer
  tracking coverage from the files table alone would under-scope the
  suite.
- **Required change:** correct the row to "R3-TC-01a–01e, 02a–02f,
  03a–03f, 04a–04e, 05a–05g, 06a–06e" (31 → 34 coordination cases,
  40 automated including R3-TC-08a–08f); no other content changes.
- **Owner:** sw-mike (tester).
- **Status: OPEN**.

## Answers to the five review points

### 1. Criterion coverage

**R3-AC-01 — fully covered.** Chain with per-edge previous-boundary
consumption (01a, with C explicitly pinned to B's *tick-0* output and
A's new output reaching C no earlier than transition 1→2), fan-out
(01b), feedback cycle with initialized boundary value on the first
transition (01c), registration/iteration-order determinism (01d) and
the negative same-tick assertion with a per-tick golden trace (01e).
The AC's "No edge uses a newly computed same-tick source" clause is
covered twice independently (01a's structural pin and 01e's
edge-by-edge golden trace), so a same-tick-propagation bug cannot
survive both.

**R3-AC-02 — fully covered.** Tick-zero outputs computed at explicit
preparation only (02a, with the inspection-never-prepares negative
phase), failed preparation leaves the simulation unstarted (02b),
staged pending writes with immediate readback and no implicit tick
(02c), last-write-wins (02e), measurement delivered-samples with a
tick-zero sample distinct-by-construction from the first delivered
value (02d), and the comprehensive never-tick sweep across every
read/write/inspection path (02f). The "documented" half of the AC
(tick-zero object outputs and mock initial samples are *documented*)
is pinned by CHK-03-1 item 3's file/line citation requirement — the
R3-002 review's lesson on unpinned "documented" expectations is applied
correctly here.

**R3-AC-03 — fully covered** (subject to issue 1's rewording). Valid
shared interval with ordinary `Station(name)` construction (03a —
baseline anchor 7 folded in), invalid-value rejection (03b), no clock
advance on failed tick (03c), mid-run change rejected until reset
boundary (03d), explicit validated model-dt binding with second-driver
rejection and no name inference (03e), and numeric representability /
roundoff / overflow limits asserted against the *documented* policy
(03f — deliberately pins "documented and enforced", not a specific
representation; no over-constraint).

**R3-AC-04 (remaining clause) — covered, plus issue 2.** Connected
removal rejected (04a), safe-detach-then-remove (04b), shared-active-
ownership conflict across two stations (04c), standalone
evolve/reset/connected-input-write prevention with the unconnected-
write and read-access positive sub-assertions (04d), and reentrancy
(04e). The membership/builder portion is correctly re-run only as
regression via R3-TC-08a/08c (17 R3-002 methods inside the 165).
Issue 2 notes the one clause whose positive half is asserted but not
exercised.

**R3-AC-05 — fully covered.** Endpoint/direction validation with
graph-bit-identical atomicity (05a), single-driver rejection with the
original edge intact (05b), value compatibility at connection and tick
time with atomic failure (05c), ndarray fan-out mutation isolation
across destinations *and* committed snapshots (05d), unconnected-input
value retention (05e), topology changes only at stopped/reset boundary
(05f), and batch/individual graph-edit atomicity with the no-batch-API
fallback (05g).

**R3-AC-06 — fully covered.** Evolve failure with `assertIs` exception
identity, committed-state/sample/clock preservation and pending-write
survival across the retry (06a — the three pinned semantics from the
brief are all present: original exception identity, prior committed
network, pending-write survival), observation/validation failure with
no partial candidate publication (06b), factory failure during reset
leaving the prior network *usable* with the documented external-side-
effect limitation cited via CHK-03-1 item 4 (06c), successful reset
rebuild with a full re-run reproducibility check (06d), and mid-
staging reset failure atomicity with a *later-declared* participant
armed to raise — precisely the non-atomic-partial-application trap the
brief calls out (06e).

**R3-AC-08 (this task's portion) — covered.** Pre-regression (08a),
post-implementation regression + compile/import/`-W error` gates with
verbatim recording of any OBS-004/005/006 or DEFECT-2 firing (08b),
standalone compatibility with the seven baseline anchors re-verified
live (08c), opt-in-only device participation (08d), dependency/scope
diff checks with runnable pathspecs (08e), and candidate CI evidence on
the task branch with run-ID recording per the R3-001 REC-1 lesson
(08f). **No R3-004 leakage:** the worked connected example and user
guide are excluded in the scope-split paragraph, the traceability table
and baseline §6; no case exercises example/guide content.

**Unambiguous coverage:** every clause maps to at least one case in
the traceability summary; no AC clause is untestable as specified.

### 2. Implementability of the automated cases

All 40 automated cases are writable with stdlib + NumPy against public
APIs once the capability exists:

- **Fixtures need nothing new.** The fixture convention reuses
  `SimulatedObject` exactly as `tests/test_tu_simulation.py` does
  (verified in code); call counters, `seen` logs, armed-to-raise
  factories/callbacks and sentinel exception objects are all
  established patterns in the existing suites; mock endpoints are
  synthetic; temp dirs only; `patch.dict` isolation per risk note 7
  mirrors `test_tu_station_objects.py:172–210`.
- **[PLAN] cases genuinely vacuously fail today.** Confirmed by
  independent grep (not assumed): there is no coordinator, connection,
  clock, pending/delivered or ownership artifact anywhere in `tu`, so
  every [PLAN] case fails by `AttributeError`/absent-API error on the
  current baseline. No production code exists that could let a [PLAN]
  case pass vacuously.
- **Expected results are precisely pinned where determinism requires
  it:** previous-boundary values are computed in-test from declared
  fixture initial values (01a); the feedback first-transition value is
  the initialized boundary value (01c); tick-zero is sim-time 0 / tick
  0 published as one operation (02a); delivered value at t is the
  source value committed at t−d (01e's golden trace); failed ticks
  leave tick count and sim-time bit-identical (03c); pending writes
  survive a failed tick and are delivered by the retry (06a);
  exception identity is `assertIs`-pinned in 02b, 03c, 06a, 06b, 06c;
  order-independence is bit-identical across runs (01d); reset returns
  tick/time to 0 with re-run reproducibility (06d).
- **Wrong-implementation detection (the brief's four named traps):**
  - *Same-tick source use* — caught by 01a (structural: C consumes B's
    tick-0 output) **and** 01e (golden-trace edge-by-edge); a
    zero-delay implementation fails both.
  - *Non-atomic reset partial application* — 06e arms a later-declared
    participant's factory to raise mid-staging and asserts every
    earlier participant and the clock unchanged; a staged-then-partial-
    publish reset fails. 06c covers the "prior network still usable"
    follow-up transition.
  - *ndarray aliasing between participants/snapshots* — 05d mutates a
    delivered ndarray via the destination's getter and asserts the
    other destination's delivered value **and** every committed
    snapshot/source buffer unchanged; aliasing at any of the three
    boundaries (fan-out copy, delivered sample, committed snapshot)
    fails.
  - *Clock advancing on failed tick* — 03c pins tick/time bit-identical
    around the failing attempt; 06a re-asserts clock preservation under
    the evolve-failure path.
  - Additional traps: 02e (intermediate write v1 never reaches any
    destination) catches leaky pending-store implementations; 04d
    catches silent snapshot invalidation instead of explicit
    prevention; 05b catches implicit summation/coercion on duplicate
    drivers; 05a/05g catch partial graph mutation on rejected edits;
    02a catches inspection-triggered preparation (callback counters).
- **Over-constraint:** issue 1 is the only instance found — one data
  row of 03b pins type-strictness beyond the AC and architecture plan.
  Everything else pins observable behavior and explicitly leaves
  class/method names, error classes (03b's "per approved design", 05a's
  "per design"), batch-API presence (05g's either/or), connection-time
  vs tick-time validation split (05c) and numeric policy (03f) to the
  approved detailed design. Placeholder spellings are declared as
  capabilities, not contracts.
- **Performance note (not an issue):** R3-TC-03f(b)'s 10⁶-tick
  accumulation is feasible for a small single-object fixture
  (order-of-seconds in pure Python) and the case already carries the
  "kept fast, or the design's documented limit" hedge, so it cannot
  deadlock the suite.

### 3. Baseline consistency with code

**All spot-checked gap claims and anchors verified, zero mismatches.**

- *Entirely-absent claims (baseline §2/§3):* confirmed by independent
  grep over `softlab/` — the only matches in `tu` are object.py:481
  "pending publication" (unrelated set_input docstring) and station.py
  205–208 (the forward-looking `rm_object` docstring), exactly as the
  baseline records. All eight gap rows of baseline §3 (coordinator,
  mock endpoints, boundary snapshot, tick-zero preparation, coordinated
  reset, station-owned interval, ownership guards, connection API) are
  consistent with the code state.
- *Seven no-regression anchors (baseline §4):* anchors 2 (identical
  exception identity, object.py 557–561 verbatim), 3 (`_copy_value`
  copy-on-boundary, object.py 196–219 verbatim), 4 (R3-002 membership
  block present and exercised), 5 (R3-001 corrected gaps inside the
  165), 6 (device-side contract suites exist and run) and 7 (no-clock
  docstring, object.py 10–12 and 233–236 verbatim) all match the cited
  code. Anchor 1's contract (evolve advances once, observe does not
  evolve) is pinned by the passing `test_tu_simulation.py` suite inside
  the re-run 165.
- *Counts:* 165 tests OK re-run live on this branch (matches §1 and
  R3-TC-08a); `object.py` 804 lines and `builder.py` 101 lines match
  §2; `_default_station` at station.py:257 matches risk note 7.
- *Environment limits (§5):* honestly recorded (3.9 leg deferred to CI,
  `-W error` gate deferred to R3-TC-08b) — pending, not waived,
  matching repo convention.

### 4. Scope check

The plan stays within the R3-003 row exactly. No case forces a concrete
`huo` process, scheduler, automatic advancement or real-time pacing
(risk note 8 states this explicitly; CHK-03-1 item 1 enforces it
mechanically with the `git diff dev...HEAD --stat` emptiness check on
`huo`/`jin`/`shui`/`mu`). No real instruments — fixtures are synthetic
`SimulatedObject`s and mock endpoints; R3-TC-08d's "real (non-mock)
device" is a synthetic `Device`/`Parameter` fixture, not hardware.
Release 1 OBS-004/005/006 and DEFECT-2 debt is not repaired or waived:
R3-TC-08b records any warning firing verbatim and dispositions it.
R3-002's 17 methods re-run unmodified as regression; the R3-AC-04
membership/builder portion is correctly not re-tested as new work. No
new dependencies are implied by any case; R3-TC-08e's pathspecs are
runnable (both `pyproject.toml` and `setup.py` exist). Excluded
mechanisms (solvers, multi-tick delays, graph persistence, noise,
thread safety) appear only in the zero-coverage list.

### 5. Design gates (CHK-03-1's five required proofs)

Verifiable as specified. tasks.md lines 41–44 require the detailed
design to prove snapshot publication, ownership/reentrancy, safe
detach, model-dt binding and numeric clock limits before
implementation. The plan maps each gate to concrete, independently
checkable evidence:

| Design gate | Blocking [PLAN] cases | Proof form |
| --- | --- | --- |
| Snapshot publication (candidates detached, publish only on full success, publication invokes no user code) | 06b (06a cross-ref) | design citation + 06b's no-partial-publication assertion |
| Ownership / reentrancy | 04d, 04e | design citation + standalone-op prevention and mid-tick reentrancy assertions |
| Safe detach | 04b | design citation + detach→remove→standalone-availability flow |
| Model-dt binding | 03e | design citation + binding-validation and second-driver rejection; documentation location cited via CHK-03-1 item 3 |
| Numeric clock limits | 03f | design citation + representability/roundoff/overflow assertions against the *documented* policy |

CHK-03-1 item 2 correctly states the gates are "failed, not waived"
when unproven, and baseline §3 records them as unproven/pending today
(no `log/release_3/design/` document exists for R3-003 yet — confirmed).
The three supplementary documentation citations (model-dt semantics,
tick-zero mock initial samples, reentrancy/safe-detach rules) and the
reset external-side-effect limitation (item 4, for 06c) are all
file/line-verifiable. No design gate depends on R3-004 deliverables.

## Over-testing / under-testing assessment

- **Over-testing:** issue 1 only. R3-TC-03f's "documented policy"
  pinning and 05g's either/or batch formulation are deliberate
  design-work flexibility, not assertion weakness.
- **Under-testing:** issues 2 and 3 (two asserted-but-unexercised
  permissions) plus one tally typo (issue 4). The four
  wrong-implementation traps named in the brief are all covered by at
  least one strong negative assertion; the gaps found are permissions
  and orderings, not traps.

## Implementability confirmation

Subject to the four minor tester-side rewordings above, every automated
case is writable under `tests/` with stdlib + NumPy only, synthetic
fixtures and temp dirs; [PLAN] cases fail today by `AttributeError`
(confirmed by independent grep, not assumed) and are constructed so
that only a correct implementation — previous-boundary-only edges,
explicit-only clock advancement, atomic tick/reset transactions with
original exception identity, staged pending writes, enforced ownership,
non-aliasing fan-out — can turn them green. Once issues 1–4 are
closed, this plan is ready for the detailed-design gate and
implementation.

## Closure log

(No entries yet — issues 1–4 OPEN, awaiting sw-mike.)
