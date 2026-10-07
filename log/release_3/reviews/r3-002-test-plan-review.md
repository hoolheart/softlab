# R3-002 test plan review — implementability

Reviewer: sw-tom (implementer) | Date: 2026-10-07
Branch: `codex/r3-002-builders` (reviewed at `89d6f62`, synced with
`origin/codex/r3-002-builders`)
Input reviewed: `log/release_3/test/r3-002-test-plan.md` (with
`r3-002-baseline.md`, `log/release_3/prd.md` R3-AC-04/08, tasks.md R3-002
row, `log/release_3/architecture-plan.md` lines 29–34, and the cited code in
`softlab/tu/station/station.py`, `softlab/tu/station/device.py`,
`softlab/tu/simulation/object.py`, `tests/test_tu_contracts.py`)

**Verdict: APPROVED** — originally CHANGES-REQUESTED with two minor
implementability issues (issue 1: ambiguous fixture injection point in
R3-TC-04h; issue 2: the "documented" half of R3-TC-04o's expectation
pinned by no check). Both issues were re-confirmed CLOSED on
2026-10-07 against Revision 2 of the test plan (`6b34844`); see the
closure log and re-confirmation note below. The coverage, scope and
safety-case structure were sound throughout and every case is
implementable as written.

## What I inspected

- Test plan and baseline (full read), PRD R3-AC-04/08 (verbatim),
  tasks.md R3-002 scope row, architecture-plan constraints (lines 29–34),
  Release 3 review convention (`r3-001-test-plan-review.md`).
- Code spot-checks against every line citation the baseline and plan rely on:
  - `softlab/tu/station/station.py` — `self._devices` + delegation
    (lines 45–46), `devices` property (60–62), device-only `snapshot`
    (67–76) and `describe` (78–116), `add_device` with the `TypeError`
    guard (118–121) and duplicate `ValueError` (122–123), `rm_device`
    lenient `pop(name, None)` (126–128), `device` lookup (130–132),
    `build_device` with unknown-model `RuntimeError` (134–145,
    specifically 142–144) — **all match the baseline's citations
    verbatim**;
  - `softlab/tu/station/device.py` — `DeviceBuilder` (515–553:
    constructor empty-model `ValueError` 530–532, `model` property
    535–538, `build(name, **kwargs)` raising `NotImplementedError`
    543–553), module-global `_device_builders` (556–557),
    `register_device_builder` type check + duplicate-model `ValueError`
    (560–567, no construction at registration — line 567 stores only),
    `get_device_builder` (570–572) — the "device-builder convention to
    mirror" is characterized **correctly** in every detail;
  - `softlab/tu/simulation/object.py` — constructor non-empty `str`
    name guard (293–296, verbatim match), `name` property **read-only**
    (428–436: property with no setter, docstring "read-only") — relevant
    to R3-TC-04i's mutability contract and to issue 1 below;
  - `tests/test_tu_contracts.py` lines 170–194 — the existing
    `patch.dict(module._device_builders, {}, clear=True)` isolation
    pattern, duplicate-registration `ValueError`, builder identity via
    `get_device_builder`, `build_device` read-back, snapshot node,
    `rm_device` non-None, unknown-model `RuntimeError` — the R3-TC-04l
    anchor and risk-note-5 cleanup-discipline model both exist exactly
    as described;
  - `grep -rn "ObjectBuilder\|object_builder\|register_object\|build_object\|add_object\|rm_object" softlab/ tests/ docs/` — **zero hits**
    (exit 1), confirming GAP-1/GAP-2: no object membership or
    object-builder artifact exists anywhere, so every [PLAN] case fails
    today by `AttributeError` exactly as the plan claims;
  - `git diff dev...HEAD` pathspec inputs for R3-TC-08e — both
    `pyproject.toml` and `setup.py` exist, so the dependency check is
    runnable as written.

## Issue list

### 1. [minor] R3-TC-04h: invalid-name fixture injection point is ambiguous and one reading is not writable through public APIs

- **Location:** area 2 table, R3-TC-04h (line 137 of the plan).
- **Description:** the case asks to "attempt insertion of an object whose
  effective name is empty or non-string". Two readings are possible:
  (a) drive the invalid name through the public construction/insertion
  entry (e.g. a station-level `build_object(model, name)` call with
  `name=''` or `name=123`), or (b) inject an object whose `name`
  attribute already holds an invalid value. Reading (b) is **not
  constructible through public APIs**: `SimulatedObject.__init__` rejects
  empty/non-`str` names outright (`object.py:293–296`) and the `name`
  property is read-only (`object.py:428–436`, no setter), so a fixture
  would have to mutate the private `._name` attribute — fault injection
  into private state, which the plan's own "all synthetic fixtures;
  stdlib + NumPy only" constraint and the repo's public-API testing
  convention discourage. A tester who picks reading (b) writes a brittle
  test that over-fits a private attribute; one who picks reading (a)
  needs the plan to say the invalid name arrives via the construction
  entry's `name` argument. The parenthetical "(cf. existing
  `SimulatedObject` constructor guard …; `Device` name guard …)" already
  gestures at the constructor-level guard, which supports reading (a),
  but the words "an object whose effective name is" assert a property of
  the object, not of the request.
- **Required change:** reword R3-TC-04h so the invalid name is driven
  through the public construction/insertion entry point (the design's
  build-and-insert call with an empty or non-string `name` argument,
  and/or an insertion attempt whose effective key would be invalid),
  with the expectation unchanged: explicit rejection before membership
  changes, no partial registration. If the tester additionally wants a
  fault-injection variant, record it explicitly as private-state
  injection so it does not masquerade as a public-API test. Update the
  traceability row wording if it quotes the case text.
- **Owner:** sw-mike (tester).
- **Status: CLOSED** (sw-mike fix, sw-tom re-confirmation
  2026-10-07) — see re-confirmation note in the closure log.

### 2. [minor] R3-TC-04o: "explicit and documented" — the "documented" half is pinned by no check

- **Location:** area 4 table, R3-TC-04o (line 170); risk note 3
  (lines 236–238); CHK-04-1 definition (lines 214–224).
- **Description:** R3-TC-04o's expected result asserts the unknown-model
  error is "explicit and documented", but the case's automatable content
  covers only "explicit" (`assertRaises`-style check plus unchanged
  membership). Nothing pins "documented": an implementation that raises
  a correct, explicit, undocumented error passes every case in the plan.
  The repo convention (AGENTS.md) requires English docstrings for new
  public APIs including error behavior, and the plan elsewhere treats
  documentation verification as checkable work (CHK-04-1), so this is a
  real — if small — coverage gap of the plan's own self-imposed
  expectation, the same category as the R3-001 review's issue 1.
- **Required change:** either (a) extend CHK-04-1 (or add a sibling
  manual record check) to pin that the station-level object-construction
  entry's docstring documents the unknown/unregistered-model error
  (class or category, per the approved design) with a file/line citation
  recorded in the results document, or (b) drop "and documented" from
  R3-TC-04o's expected result. Option (a) is preferred for consistency
  with the repo docstring convention; either closes the issue.
- **Owner:** sw-mike (tester).
- **Status: CLOSED** (sw-mike fix, sw-tom re-confirmation
  2026-10-07) — see re-confirmation note in the closure log.

## Answers to the five review points

### 1. Criterion coverage

**R3-AC-04 membership/builder portion — fully covered.** Clause by
clause, from the verbatim AC text:

- *Existing device use remains compatible* — R3-TC-04l (automated
  re-run of the device builder-registry contract) + R3-TC-08a/08c/08d.
- *Object membership* (add / look up / list / remove) — R3-TC-04a–04e.
- *Explicit lookup* — R3-TC-04d, with device-side precedence pinned by
  the baseline §C evidence and R3-TC-04l.
- *Naming* — R3-TC-04f (cross-namespace shadowing), 04g (duplicate
  object names), 04h (empty/non-string), 04i (rename key orphaning).
- *Builder discovery/construction* — R3-TC-04j (convention mirror),
  04k (registry semantics), 04m (station construction).
- *Failed-build safety* — R3-TC-04n (raising builder), 04o (unknown
  model), 04p (membership-validation failure), plus the device-path
  analog pinned by 04l.
- *Independent instances* — R3-TC-04q (mutable-state isolation,
  ndarray-aware), 04r (already-owned instance rejection, per
  architecture-plan lines 32–33).

**R3-AC-08 (this task's portion) — covered.** Compatibility/regression
(08a/08b/08c), registry coexistence (08d), no new dependencies (08e),
candidate CI evidence on the task branch with run-ID recording (08f) —
the R3-001 REC-1 lesson (cite the pre-merge task-branch run) is applied
explicitly. **Connected criteria correctly excluded:** the connected
removal / shared-active-ownership clause of R3-AC-04 is assigned to
R3-003/004 by tasks.md (row 3 owns "ownership"), the plan excludes it in
both the scope-split paragraph and the traceability table, and no case
exercises edges, a coordinator, `sim_dt` or connected reset. The
worked connected example portion of R3-AC-08 is likewise excluded. No
criterion is untestable as specified.

### 2. Implementability of the automated cases

Tally check: the automated `unittest` cases are R3-TC-04a–04l (12) +
04n–04r (5) = **17** object-side cases, plus R3-TC-08a–08f (6) = **23
automated total**; R3-TC-04m is conditional-manual (station wiring
verified by file/line citation if construction lands outside
`station.py`) and CHK-04-1 is reviewer-owned. The plan's own accounting
is internally consistent on this.

All cases are writable with stdlib + NumPy against public APIs once the
capability exists; fixtures need nothing beyond synthetic
`SimulatedObject`s (public constructor), builder closures with call
counters (the `DeviceBuilder` subclass pattern in
`test_tu_contracts.py:172–176` is the direct model), and `patch.dict`
registry isolation. Assertion precision, case by case:

- **Fail-on-baseline claims verified:** every [PLAN] case fails today by
  `AttributeError` — confirmed by the zero-hit grep over `softlab/`,
  `tests/`, `docs/` for all placeholder API names (the same probe the
  baseline §2/§3 records). There is no production artifact to import, so
  no [PLAN] case can pass vacuously today.
- **No false security (wrong-implementation detection):**
  - R3-TC-04k(c) (build count zero after registration) catches a
    registry that constructs at registration time;
  - R3-TC-04q catches a builder closing over shared mutable state
    (`a` mutated, `b` read; ndarray values chosen to expose aliasing);
  - R3-TC-04r catches a builder returning one shared/owned instance
    twice — precisely the "returns shared/owned instances twice"
    wrong-implementation mode called out for review;
  - R3-TC-04n's `assertIs` exception-identity pin plus bit-identical
    membership snapshot catches remapping/wrapping of the build error
    and partial registration, mirroring the SIM-TC-05i precedent;
  - R3-TC-04l catches accidental disturbance of the device registry
    (duplicate `ValueError`, identity, read-back, snapshot node,
    unknown-model `RuntimeError` all re-asserted).
- **No over-constraint:** cases pin observable behavior and explicitly
  leave class/method names, exception classes (04o, risk note 3) and the
  rename policy (04i, two acceptable outcomes) to the approved design.
  04j's "visibly mirrors `DeviceBuilder`" is backed by concrete pins
  (`model` property, `build(name, **kwargs)` signature, registry
  identity); the PRD's actual prohibition (do not refactor
  `DeviceBuilder` into an unrelated fluent builder) is enforced by 04l,
  not by over-constraining the new builder's class shape.
- **Honest keep-passing cases:** 04h and 04l are flagged as
  baseline-passing and labeled as such in the document; 04h's label
  "(passes vacuously on baseline; keep passing)" is exactly the
  disclosure the workflow requires — with the injection-point caveat
  raised as issue 1.

### 3. Baseline consistency with code

**All cited references verified, zero mismatches.** Every station.py
citation (45–46, 60–62, 67–76, 118–145), every device.py citation
(129–133 delegation-precedence docstring, 143–145 name guard, 515–572
builder/registry block), and the object.py constructor guard (293–296)
match the cited content verbatim. The device-builder convention is
characterized correctly in full: model identity property, empty-model
rejection, `build(name, **kwargs)` base raising `NotImplementedError`,
module-global dict, duplicate-model `ValueError`, store-without-
construct registration, `Optional` return lookup. The GAP-1/GAP-2
absence claims are confirmed independently by the zero-hit grep. One
observation (not a mismatch, feeds issue 1): `SimulatedObject.name` is
read-only with a constructor-level guard, so "an object whose effective
name is empty or non-string" cannot exist via public APIs — the invalid
name must enter at the construction/insertion call boundary.

### 4. Scope check

The plan stays within the R3-002 row exactly. Production changes implied
by the cases are confined to `softlab/tu/` (station membership + object
builder/registry), enforced mechanically by CHK-04-1's
`git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui
softlab/mu` emptiness check plus the no-connected-production-code
clause. No case exercises connected simulation, real instruments, or
`huo`/`jin`/`shui`/`mu` behavior; fixtures are synthetic, temp dirs
only, no new required dependencies (08e pathspec verified runnable).
Release 1 debt is not silently repaired or waived: the device
rename-after-add defect (baseline C-3) is explicitly out of scope —
R3-TC-04i asserts only that the *object* side does not reproduce it and
risk note 2 forbids expanding into the device fix. The shared-active-
ownership conflict handling beyond rejection wording is excluded per the
tasks.md split. R3-TC-08b's warning-gate records verbatim and
dispositions any OBS-004/005/006 or DEFECT-2 firing rather than waiving
it (baseline confirms none fired in the `-W default` re-run).

### 5. Safety cases (build-failure safety and instance independence)

Both are concrete and assertion-pinned:

- **Build-failure safety** — 04n pins `assertIs`-identical exception
  propagation and bit-identical membership (names + identities) after a
  builder armed to raise; 04o pins unchanged membership after an
  unknown model; 04p pins unchanged membership and untouched existing
  device when the constructed object violates a membership rule. The
  device-path atomicity mechanism they mirror (`build_device` calls
  `add_device(builder.build(...))`, station.py:145, raising build
  propagates before insertion) was verified in code. No partial-
  registration or name-reservation escape is possible to pass these.
- **Instance independence** — 04q pins `a.get_state('x') == 1.0` /
  `b.get_state('x') == 0.0` after mutating `a` only, with ndarray-valued
  fixtures chosen to expose aliasing; 04r pins that a builder returning
  the same instance on every call is rejected on second insertion with
  the first member unmodified. Both wrong-implementation modes named in
  the review brief (shared mutable state, same-instance reuse) are
  directly caught.

## Over-testing / under-testing assessment

- **Under-testing:** issue 2 (the "documented" half of R3-TC-04o has no
  pinning check) — the only coverage gap found.
- **Over-testing:** none found. The two acceptable-outcome formulations
  (04c's "returns the object or per design", 04i's "forbidden or
  atomic") are deliberate design-work flexibility, not assertion
  weakness; both outcomes are separately checkable and either fully
  satisfies the AC. R3-TC-04m's conditional manual/automated split is
  handled cleanly in the traceability section.

## Implementability confirmation

Subject to the two minor rewordings above, every automated case is
writable today under `tests/` with stdlib + NumPy only, synthetic
fixtures, and temp dirs; [PLAN] cases fail today by `AttributeError`
(confirmed by grep, not assumed) and are constructed so that only a
correct implementation — explicit rejections, unchanged membership on
every failure path, registration-without-construction, distinct
registries, independent instances, already-owned rejection — can turn
them green. New test code appends to or adds files alongside existing
suites without weakening any existing method (the 04l anchor test is
re-run unmodified). Once issues 1 and 2 are closed, this plan is ready
for implementation.

## Closure log

### 2026-10-07 — Issues 1 and 2 closed (sw-mike)

- **Issue 1 (R3-TC-04h injection point): RESOLVED.** The case steps
  now drive the invalid name through the public construction/insertion
  entry's `name` argument ("attempt build-and-insert with an empty or
  non-string `name` argument (e.g. `name=''` or `name=123`): the
  invalid name enters at the entry point's `name` argument, not as
  pre-existing object state"), removing the ambiguous "an object whose
  effective name is" wording and the private-state-injection reading.
  Expected result unchanged (explicit rejection before membership
  changes; no partial registration); case ID unchanged; no
  fault-injection variant added. No traceability row quoted the old
  wording, so no table change was needed.
- **Issue 2 (R3-TC-04o "documented" half): RESOLVED** via the
  preferred option (a). CHK-04-1 is extended to pin that the
  station-level object-construction entry's docstring documents the
  unknown/unregistered-model error (class or category, per the
  approved design), verified by a file/line citation of that docstring
  recorded in the results document; risk note 3 updated to state that
  "explicit" is pinned by the automated case and "documented" by
  CHK-04-1's citation. R3-TC-04o's expected result retains "explicit
  and documented".
- **Commit:** `e6f1af2` (`docs(log): close r3-002 test plan review
  issues`, test-plan file); closure entries in this file committed
  with the same message. Branch `codex/r3-002-builders`, pushed to
  `origin`.
- **Closed by:** sw-mike (tester), 2026-10-07. Verdict unchanged
  (CHANGES-REQUESTED recorded at `fa40165`; issues now resolved).

### 2026-10-07 — Re-confirmation and APPROVED (sw-tom)

I re-reviewed the test plan Revision 2 against both issues on
2026-10-07 at branch tip `6b34844` (synced with
`origin/codex/r3-002-builders`).

- **Issue 1 — CLOSED, confirmed.** R3-TC-04h's steps now drive the
  invalid name explicitly through the public construction/insertion
  entry's `name` argument ("attempt build-and-insert with an empty or
  non-string `name` argument (e.g. `name=''` or `name=123'): the
  invalid name enters at the entry point's `name` argument, not as
  pre-existing object state"). The ambiguous "an object whose effective
  name is" wording and the private-state-injection reading are gone.
  The expected result is verbatim unchanged ("Explicit rejection before
  membership changes; no partial registration"), the case ID is
  unchanged, and the "(passes vacuously on baseline; keep passing)"
  disclosure is preserved. The constructor-guard cross-references
  (object.py 294–296, device.py 143–145) remain as supporting context.
  No fault-injection variant was added, as agreed.
- **Issue 2 — CLOSED, confirmed.** CHK-04-1 (traceability section,
  lines 224–228) now pins the "documented" half of R3-TC-04o: the
  station-level object-construction entry's docstring must document the
  unknown/unregistered-model error (class or category, per the approved
  design), verified by a file/line citation recorded in the results
  document. This is the preferred option (a) from my issue. Risk note 3
  now explicitly assigns "explicit" to the automated case and
  "documented" to CHK-04-1's citation, and R3-TC-04o's expected result
  retains "explicit and documented" — so the case text and the pinning
  check are now consistent.
- **No collateral damage.** The full revision diff (`fa40165..6b34844`)
  touches only R3-TC-04h's steps cell, the CHK-04-1 paragraph, risk
  note 3, and the revision history — three review fixes plus their
  record. No other case was modified, weakened or re-labeled; the
  [BASELINE-READY]/[PLAN] accounting (147-test baseline reference,
  23 automated + 04m conditional-manual tally), traceability rows,
  scope-split paragraph, and safety-case pins (04n `assertIs`,
  04q/04r independence) are identical to Revision 1. No scope
  expansion: no connected-simulation, ownership, or R3-003/004 content
  was added.

**Final verdict: APPROVED.** This plan is ready for implementation.
