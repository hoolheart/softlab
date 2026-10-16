# R3-004 test results — executed connected example, user guide, final architecture and release evidence

Tester: sw-mike | Date: 2026-10-16
Branch: `codex/r3-004-delivery` (test head `01af2dd675afcbbfb4ec1ffa49ea9a4d8347cbcf`,
synced with `origin/codex/r3-004-delivery`; this execution adds only this record)
Plan: `log/release_3/test/r3-004-test-plan.md` (Revision 2, approved)
Requirements: `log/release_3/prd.md` — R3-AC-08 in full (remaining clauses),
release-wide acceptance evidence of R3-AC-01–08
Implementation under test: commits `0c9bd34..3b8e42c` (code review APPROVED `b82f5ec`)
Baseline: `log/release_3/test/r3-004-baseline.md` (`72fcfbf`)
Predecessor evidence: `r3-001-test-results.md`, `r3-002-test-results.md`,
`r3-003-test-results.md`

## Verdict

**PASS — all 14 planned case IDs pass (8 automated cases R3-TC-09a–09h + 6
manual record checks R3-TC-09m1–09m6), plus release-wide acceptance
R3-AC-01–08 confirmed with the 09m5 matrix below.** Full suite: **210
tests, OK, exit code 0** (207 pre-existing unchanged + 3 new R3-004).
Zero warnings, including under the `-W error::Warning` gate. Production
diff against `origin/dev` is **empty** (`softlab/`, `pyproject.toml`,
`setup.py` untouched); the only `tests/` changes are the two new R3-004
files, so all 207 pre-existing test files are byte-identical to `dev`.
One explicit flag carried to sw-prod (not a failure): the 09m6
scope-gate ratification signature of sw-prod/sw-camille remains
outstanding by design of the process records (disposition text recorded
in §09m6 below). Nothing below is marked passed that was not run.

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env per AGENTS.md; invoked as `.venv/bin/python` — `conda activate` does not persist across this shell, so the venv interpreter is used directly, same environment) |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |
| New test dependencies | none (stdlib + NumPy only) |
| Unverifiable items | none |

## Commands and verbatim results

### 1. unittest full suite (R3-TC-09a–09h; R3-TC-09d)

Command:

```
.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
```

Result (final confirmation run at head `01af2dd`):

```
----------------------------------------------------------------------
Ran 210 tests in 1.139s

OK
```

- Test count: **210** = 207 pre-existing unchanged + 3 new R3-004
  (`tests/test_sim_connected_guide.py` 2, `tests/test_arch_links.py` 1);
  0 `FAIL`/`ERROR` lines.
- Result: **OK**, exit code **0**.
- The backend lines ("Connected to sqlite3 backend …", "Connected to
  HDF5 backend …") are informational prints from the data-backend
  tests, not warnings.

Per-case mapping run (all three R3-004-scope files plus the guide
regression anchor, verbose tail):

```
.venv/bin/python -m unittest tests.test_sim_user_guide \
    tests.test_sim_connected_guide tests.test_arch_links -v
```

```
test_sim_tc_08a_guide_topic_checklist ... ok
test_sim_tc_08b_executed_deterministic_example ... ok
test_sim_tc_08c_api_docstrings ... ok
test_sim_tc_08d_evidence_assembly ... ok
test_r3_tc_09a_golden_trace ... ok
test_r3_tc_09b_byte_reproduction ... ok
test_r3_tc_09h_arch_reference_integrity ... ok

Ran 7 tests in 0.004s

OK
```

### 2. Warning gate (R3-TC-09e)

Command:

```
.venv/bin/python -W error::Warning -m unittest discover -s tests -p 'test_*.py'
```

Result: **Ran 210 tests … OK**, exit code **0** — every warning class is
an error under this gate, so a green run proves zero warnings. No
`DeprecationWarning`/`UserWarning`/other warning fired; no Release 1
debt warning (OBS-004/005/006, DEFECT-2) fired — none observed, nothing
waived silently.

### 3. compileall (R3-TC-09f)

Command: `.venv/bin/python -m compileall -q softlab`
Exit code **0**, no output.

### 4. import smoke + export identity (R3-TC-09f)

Command:

```
.venv/bin/python -c "import softlab; print(softlab.__version__)"
```

Output: `0.3.0`, exit code **0**.

The four R3-003 public exports still import from their documented path
with public identity against the backing modules:

```
.venv/bin/python -c "
from softlab.tu.simulation import Coordinator, MockControl, MockMeasurement, Endpoint
import softlab.tu.simulation.coordination as c
import softlab.tu.simulation.mock as m
assert c.Coordinator is Coordinator
assert m.MockControl is MockControl
assert m.MockMeasurement is MockMeasurement
assert c.Endpoint is Endpoint
print('EXPORTS_OK')"
```

Output: `EXPORTS_OK`. (Production diff is empty — exports are
definitionally unchanged; the identity re-run is the recorded evidence.)

### 5. Scope gate (R3-TC-09g)

- `git diff origin/dev...HEAD --stat -- softlab pyproject.toml setup.py`
  → **empty**, exit 0. **Zero production changes, zero packaging
  metadata changes, zero new dependencies.** Consequently no
  `snapshot()`/`describe()` schema change can exist (scope-gate §2
  mechanical enforcement).
- `git diff origin/dev...HEAD --stat -- tests/` → exactly two **new**
  files:
  - `tests/test_sim_connected_guide.py` (+202)
  - `tests/test_arch_links.py` (+72)
- Byte-identity of every pre-existing test file to `dev`, checked
  mechanically:

  ```
  for f in $(git ls-tree -r --name-only origin/dev -- tests/ | grep 'test_.*\.py$'); do
      git diff --quiet origin/dev HEAD -- "$f" || echo "DIFFERS: $f"
  done
  ```

  Output: no `DIFFERS` line — **all 207 pre-existing tests are in
  byte-identical files**. This satisfies plan Revision 2's reworded
  09g expectation (which permitted design-bound extensions of
  `tests/test_sim_user_guide.py`): no such extension was needed, and no
  unrelated test edit exists.
- `git diff origin/dev...HEAD --stat` (whole tree) → only
  `arch.md`, `docs/user-guide/simulation.md`, `log/release_3/*` process
  records and the two new test files (3,096 insertions, docs/records and
  tests only). No UI/hardware content; no silent scope expansion.

## R3-TC-09m1 — guide connected-topic coverage (manual, file:line verified)

Delivered guide sections: `docs/user-guide/simulation.md` §12
"Coordinated simulation" (lines 415–480) and §13 "Executed connected
example" (lines 482–619). Both exist. Per-topic citations, guide text
verified against the implementation docstrings/code at head `01af2dd`:

| Required topic | Guide location | Implementation anchor (verified) |
| --- | --- | --- |
| Shared explicit clock: `sim_dt`, rejection rules, mid-run change rule | §12 "Shared explicit clock", simulation.md:427–436 (bool/non-real → `TypeError`; non-finite/non-positive → `ValueError`; `Decimal` rejected via non-real; mid-run change → `RuntimeError` until coordinated reset; `sim_time == float(tick) * sim_dt` computed, never accumulated; non-finite candidate time fails the tick atomically with `OverflowError`) | `station.py:105-168` (`sim_dt` property + setter, setter docstring 124–142 with the exact validation-order rule); `coordination.py:58-97` (`_validate_interval`); clock policy `coordination.py:195-202`; `sim_time` property `coordination.py:311-330`; overflow gate `coordination.py:1144-1150` |
| Delayed edges and feedback: previous-boundary rule, no same-tick propagation, tick-zero initialization | §12 "Delayed edges and feedback", simulation.md:438–447 ("each destination consumes the committed boundary snapshot S_k — the source's previous-boundary value — so no edge propagates a newly computed same-tick value by construction (publication is the only writer)"; "`S_0` is computed from the initial states at the explicit `prepare()`; a feedback edge therefore consumes its initialized previous value on the first transition"; fan-out independent copy) | `coordination.py:1104-1135` (step phase 2: deliveries read `_frozen_edge_value`, verified at 1120–1132); `_frozen_edge_value` 1316–1318 (reads the frozen boundary copies only); prepare tick-zero publication `coordination.py:967-1035` |
| Mock interaction: `MockControl` pending staging + readback; `MockMeasurement` tick-zero sample vs delivered samples; reads/writes never tick | §12 "Mock interaction", simulation.md:449–457 ("stages a pending value (last write wins); the readback shows the pending value immediately, the next transition samples and delivers it, the pending persists across ticks and survives failed ticks, and writes issued while a tick or reset is in flight are rejected with `RuntimeError`"; "the tick-zero sample is the constructor initial, because `prepare()` delivers nothing to measurements. Reads and writes never tick") | `mock.py:33-139` (`MockControl` class docstring 35–42 pending/last-write-wins; `write` Errors/busy guard 99–121); `mock.py:142-215` (`MockMeasurement` class docstring 148–152 tick-zero initial, "Reads never fetch the current source directly"; `read` docstring 200–203 "never fetches") |
| Coordinated reset: one-operation rebuild + external-side-effect limit | §12 "Coordinated reset", simulation.md:465–474 ("One operation rebuilds every participant's inputs and states … zeroes the clock and lands STOPPED. Topology is retained; a failure leaves the prior committed network fully usable … Atomicity covers library-owned state only — external side effects of user callbacks are outside every rollback guarantee — **and reset is not claimed to equal fresh construction**") | `coordination.py:1165-1258` (`reset` docstring 1166–1168 "staged rebuild, publish zero, land STOPPED"); external-side-effect limit `coordination.py:1194-1197` |

No-overclaim check: the guide explicitly disclaims fresh-construction
equivalence (simulation.md:473–474) and claims no capability outside
the docstrings. §12 statements match the PRD behavioral text
(prd.md:31–106) and the r3-003 §6/§7 citation anchors, which re-verified
unchanged at this head (production diff empty). **PASS.**

## R3-TC-09m2 — standalone/connected split honesty (manual)

- §9 "Bridging to Device and Parameter (mock devices)" still documents
  the **standalone** pattern only for its mechanism (simulation.md:
  238–320): the input-parameter wiring (249–253), the standalone
  readback rule (255–262), two-gate validation asymmetry (275–284),
  measurement parameter (286–294), sharing (296–298), huo hooks
  (300–320).
- The former deferral wording is **resolved**: simulation.md:264–273 now
  reads "**Standalone vs connected.** This section documents the
  **standalone** pattern … In a **connected** (coordinated) run the
  readback surfaces differ by design: a `MockControl` read returns the
  staged **pending** source value and a `MockMeasurement` read returns
  the most recently **delivered** sample obeying the edge-delay rule —
  see §12. **Both patterns are implemented and both are stated here.**"
  No "planned Release 3 work" / "not stated here" text survives: a
  case-insensitive scan for `planned|not stated here|future release|to
  be (added|delivered)` over the whole guide returns **zero matches**.
- Connected readback semantics in the new text match the implemented
  contract: pending-source control readback (`mock.py:35-42`,
  `coordination.py:1123-1125` phase-2 sampling of frozen pendings) and
  delivered-sample measurement reads (`mock.py:142-152`,
  `coordination.py:1136-1146` phase-3 measurement publication).
- R3-TC-07t pins (r3-001 results §"R3-TC-07t") still hold: the
  standalone readback text (255–262) retains "the read reflects the
  object's current input after direct `set_input` changes, `reset()`
  and writes through another controller sharing the same object", and
  SIM-TC-08a–08d re-ran green (§1 mapping run). **PASS.**

## R3-TC-09m3 — final architecture verification (manual, anchors verified at head `01af2dd`)

Clause-by-clause read of the delivered `arch.md` (591 lines):

**(a) Status/header (arch.md:5-14)** reflects both integration points
with the Release-2-pinning discipline: Release 2 state (`470e2c7`, 135
tests, CI run 37298623628), R3-001/002/003 dev merges (`6dc4d36`,
`0bd97d5`, `2991a63`, dev CI run 38820136267 — verified green via
`gh run view` in §09m4), and the R3-004 candidate point at `f32b796`
(candidate CI run 38826003029) with the explicit note "the cited
candidate run certifies an ancestor of this fill commit, per the
two-stage fill-commit convention" — the ancestor claim verified here:
`git merge-base --is-ancestor f32b796 HEAD` → true; likewise for the
fill commit `45d4244` → true.

**(b) Claim-by-claim traceability.** The delivered A9 table
(arch.md:553-573) matches the design's A9 table
(r3-004-detailed-design.md:606-627) row-for-row, 20 rows. I
independently spot-checked **17 anchors** (requirement ≥8, including
`object.py:947`) against the code at this head — all resolve to the
claimed construct:

| # | Anchor | Verified content |
| --- | --- | --- |
| 1 | `softlab/tu/simulation/__init__.py:3-15` | eight-name re-export block (SimulatedObject, ObjectBuilder, register_object_builder, get_object_builder, MockControl, MockMeasurement, Coordinator, Endpoint) |
| 2 | `coordination.py:171-259` | `class Coordinator` at 172, one-per-station/permanent/duck-checked docstring 173–181 |
| 3 | `coordination.py:105-162` | `class Endpoint` at 105 with resolution docstring |
| 4 | `coordination.py:370-436` | `_EndpointBase` resolution machinery (module region confirmed) |
| 5 | `coordination.py:750-856` | `connect` at 750 with single-driver/STOPPED-only docstring |
| 6 | `coordination.py:858-924` | `bind_interval` at 858, D11 explicit-binding docstring 862–875 |
| 7 | `coordination.py:509-566` | `withdraw` at 509, D12 safe-detach docstring 512–520 |
| 8 | `coordination.py:820-856` | `disconnect` region (D12 sequence) |
| 9 | `coordination.py:967-1035` | `prepare` at 968, "tick-zero … all-or-nothing" docstring 969–970 |
| 10 | `coordination.py:1104-1135` | step phase-2 deliveries via `_frozen_edge_value` (frozen boundary copies) |
| 11 | `coordination.py:1144-1150` | exact `float(next_tick) * sim_dt` + `math.isfinite` → `OverflowError` before publication |
| 12 | `coordination.py:1165-1258` | `reset` at 1166, side-effect limit 1194–1197 |
| 13 | `coordination.py:196-202` / `292-330` | clock multiplication policy; `sim_time` property at 311 |
| 14 | `mock.py:33-139` | `MockControl` pending/last-write-wins/busy guard |
| 15 | `mock.py:142-215` | `MockMeasurement` delivered-sample, tick-zero constructor initial |
| 16 | `station.py:105-168`, `station.py:324-331`, `station.py:337-376`, `station.py:190-239`, `station.py:100-102`, `station.py:273-335` | `sim_dt` property at 105; `rm_object` ownership guard 324–331; `build_object` at 337; `snapshot()` device-only at 190–195; `objects` property at 100 |
| 17 | `object.py:390`, `object.py:521-522`, `object.py:586`, `object.py:715`, **`object.py:947`** | `_active_coordinator` claim slot at 390; `set_input` guard delegation 521–522; `evolve_once` claimed-rejection RuntimeError 586–590; `reset` claimed-rejection 715–719; **`def _validate_callback_result(` at 947** — the R3-001 malformed callback-key error site |

**(c) Planned-vs-implemented markers resolved.** Only two `[PLANNED]`
marks remain in arch.md: the standing convention note (line 18,
"Sections marked **[PLANNED]** describe future work") and line 575
("**[PLANNED — not implemented]** Run services, persistence policy,
calibration workflows, schedulers and remote applications") — both
describe genuinely non-R3 future work. No **[PLANNED]** text describes
delivered Release 3 capability, and no delivered Release 3 capability
is described as future/absent: the baseline GAP-3/4 gaps (header,
`tu` row, simulation section stopping at Release 2) are all closed —
the header carries the R3 record (5–14), the `tu` row lists the R3
corrections/additions (34), and the Simulation section documents the
coordinated-simulation record (347–398). The deferral of the
snapshot/describe node is explicitly recorded as a **decision**, not a
gap (392–398, 411–414, 583–586). Candidate labeling present where
required: the header states the candidate-point convention and certifies
only an ancestor of the fill commit.

**(d) Release 1/2 content not regressed.** Sections on parameters,
devices/stations, VISA, theory, execution interaction, dependencies and
the Release 1/2 implemented-scope records are unchanged except the
additive R3 notes; OBS/DEFECT disposition table intact (427–438);
Release 1/2 suite/test-file records (494–507) consistent with the
predecessor results docs. **PASS.**

## R3-TC-09m4 — CI evidence (manual, `gh run view`)

| Run | Branch | headSha | Conclusion | Matrix jobs |
| --- | --- | --- | --- | --- |
| 38826003029 (candidate) | `codex/r3-004-delivery` | `f32b796249da2f6be749bd49db8fa7aa09b6f3ae` | **success** | `compatibility (3.9)` success, `compatibility (3.13)` success (regression/compile/import steps all success) |
| 38826175913 (fill commit) | `codex/r3-004-delivery` | `45d4244660bf6e68aaad59e0e4545a1a764d87d6` | **success** | `compatibility (3.9)` success, `compatibility (3.13)` success |
| 38820136267 (dev integration, R3-003) | `dev` | `2991a63e9bab7804a8b8e9a9dedd726f7addc2fe` | **success** | `compatibility (3.9)` success, `compatibility (3.13)` success |
| 38828471698 (final head — recorded beyond the plan minimum) | `codex/r3-004-delivery` | `01af2dd675afcbbfb4ec1ffa49ea9a4d8347cbcf` (= test head) | **success** | `compatibility (3.9)` success, `compatibility (3.13)` success |

- Recorded SHAs match the commits they certify: `f32b796` = Stage-2
  arch-record commit, `45d4244` = fill commit, `2991a63` = R3-003 dev
  merge (the R3-001/002 merges `6dc4d36`/`0bd97d5` are recorded in the
  predecessor results docs; the plan requires at least the last one,
  verified above). Both candidate SHAs are ancestors of the test head
  (`git merge-base --is-ancestor` → true for both), matching the
  two-stage fill-commit convention claimed in arch.md:11-14.
- `gh run list --branch codex/r3-004-delivery --limit 5` → the five
  most recent runs (38828471698 @ 01af2dd, 38828385304 @ b82f5ec,
  38827973951 @ 3b8e42c, 38827821245 @ 940bfd8f, 38826487032 @
  a6bb006e) all `success` — every commit from the implementation range
  through the test head is CI-certified; no CI result is marked green
  before it exists (R3-001 REC-1 discipline). **PASS.**

## R3-TC-09m5 — release-wide acceptance matrix R3-AC-01–08

Per-criterion rules applied (design V1): criterion text quoted from
prd.md:139-146; covering tasks named; results-doc anchors as section
references; per-case verdicts; release-level re-verification citing this
task's run records; single-task-evidence criteria flagged; nothing
marked PASS that was not run at the recorded head.

| Criterion (prd.md) | Covered by task | Results doc anchor | Release-level re-check | Verdict |
| --- | --- | --- | --- | --- |
| R3-AC-01 "A deterministic chain, fan-out and feedback network produce the specified previous-boundary results regardless of object/device registration or iteration order. No edge uses a newly computed same-tick source." | R3-003 | r3-003-test-results.md §"Per-case results" 01a–01e (all PASS; key pins §"Key behavioral pins" delayed-network bullet) | §1 full suite 210 OK (09d); §2 warning gate (09e); connected golden trace re-demonstrated by 09a/09b this task | **PASS** |
| R3-AC-02 "Tick-zero object outputs and mock initial samples are documented; the first transition has defined results. Writes stage controls, control readback shows pending values, measurements show delivered samples, and reads/writes never implicitly tick." | R3-003 | r3-003 02a–02f (PASS; doc citations its §7 rows "Tick-zero mock initial-sample documentation", "Guarded-method RuntimeError docstrings") | §1 (09d/09c); guide §12 mock-interaction text re-cited against code in 09m1 this task | **PASS** |
| R3-AC-03 "Valid explicit `sim_dt` controls all coordinated participants; invalid/nonfinite/zero/negative/bool values are rejected. Successful ticks advance one interval; failed ticks do not. Mid-run interval changes fail until reset. Models using dt have a documented consistent binding." | R3-003 | r3-003 03a–03f (PASS; binding/clock-limit proofs its §6 items 4-5; coercion doc its §7) | §1 (09d); guide §12 clock text re-cited in 09m1 (station.py:105-168, coordination.py:1144-1150) | **PASS** |
| R3-AC-04 "Existing device use remains compatible. Object membership, explicit lookup, naming, builder discovery/construction, failed-build safety and independent instances work; connected removal and shared-active-ownership conflicts are rejected." | R3-002 (membership/builders), R3-003 (connected half) | r3-002-test-results.md 04a–04r (PASS) + 08a–08f; r3-003 04a–04f + 08a/08c | §1 (09d); arch.md A9 rows "Station object membership…", "Object builder…" spot-checked this task (09m3) | **PASS** |
| R3-AC-05 "Endpoint/direction/value compatibility and single-driver rules are enforced. Fan-out copies values safely; ndarray mutation cannot alter another participant or committed snapshot. Unconnected inputs retain their values; invalid graph changes are atomic." | R3-003 | r3-003 05a–05g (PASS) | §1 (09d); arch.md A9 rows "Endpoint validation…", "Single-driver…" spot-checked (09m3) | **PASS** |
| R3-AC-06 "An evolution, observation, validation or factory failure preserves the prior committed network and clock with original callback exception identity. A successful reset rebuilds initial participants/buffers/clock; accepted pending writes survive failed ticks. External side-effect limits are explicit." | R3-003 | r3-003 06a–06e (PASS; side-effect citation its §7) | §1 (09d); reset side-effect limit re-cited (coordination.py:1194-1197) in 09m1; reset demonstrated in the worked example (§13 output lines 613-615) | **PASS** |
| R3-AC-07 "Bridge readback follows reset, direct changes and shared controllers; mixed callback keys give the documented error; reset documentation states deterministic/factory limitations accurately; Release 2 board agrees with verified closure evidence." | R3-001 — **flagged: rests primarily on R3-001's evidence; release-level re-verification is 09d + 09m2** (design V1 prediction confirmed) | r3-001-test-results.md 07a–07m (automated PASS) + 07n–07t (manual PASS, incl. §"R3-TC-07t" guide §9 wording) + CHK-08-1 | §1 (09d, byte-identical test files); 09m2 this task re-verified guide §9 standalone wording and R3-TC-07t pins still hold | **PASS (flagged single-task reliance, re-verified at release level)** |
| R3-AC-08 "Existing standalone object/device/theory behavior and imports remain compatible except the explicitly corrected gaps. A worked connected example demonstrates the clock, delayed feedback, mock interaction and reset. Regression, compile/import, warning and candidate CI evidence precede integration; architecture describes implemented capability only after delivery." | R3-001 (standalone half), R3-002, R3-003, **R3-004 (connected example + final evidence)** | r3-001 08a–08d; r3-002 08a–08f; r3-003 08a–08f + CHK-03-1; **this doc 09a–09h (§§1–5) + 09m1–09m6** | 09c–09h green at 210 tests; 09m1–09m5 this section; 09m6 disposition below | **PASS** |

All 8 rows trace to recorded PASS evidence; the single-flagged criterion
(R3-AC-07) is re-verified at release level as required. **PASS.**

## R3-TC-09m6 — scope-gate ratification record (disposition text)

**Mechanical guard (09g): green** — the production diff is empty, so the
additive simulation-description node cannot have entered Release 3 by
construction; no test case sneaks it back in (the two new test files
touch no production code).

**Decision explicitness:** the §2 decision is recorded in two process
locations — the approved test plan §2 (DECISION paragraph, "OUT OF
SCOPE for Release 3; recommend deferral beyond Release 3") and
sprint-board.md:138-140 ("snapshot/describe simulation node OUT of
scope for Release 3, recorded as ratification item R3-TC-09m6"). It is
not silent in either direction, and the deferral is also recorded in
arch.md (392-398, 411-414, 583-586) so the architecture neither claims
the node nor omits the decision.

**Disposition text recorded for sw-prod/sw-camille ratification**
(quoting the design's record wording, r3-004-detailed-design.md
§"Scope-gate ratification", with the mechanical confirmation added by
this execution):

> Scope-gate ratification (R3-TC-09m6): the additive, inert
> simulation-description node for `Station.snapshot()`/`describe()` is
> consciously **excluded from Release 3** and deferred beyond it as a
> separately authorized future change. Basis: the gate analysis in
> r3-004-test-plan.md §2 (no R3-AC criterion requires it — membership
> and network visibility are already observable via `Station.objects`
> (station.py:100-102) and `Coordinator.participants`/
> `Coordinator.connections` (coordination.py:333, 346); R3-TC-08d pins
> the legacy snapshot/describe shape byte-stable; PRD scope discipline
> prd.md:148-154), confirmed by r3-004-detailed-design.md §"Scope
> guard"/§A4 item 7. Mechanical guard: R3-TC-09g green at test head
> `01af2dd` — production diff vs `origin/dev` empty. Ratified by
> sw-prod/sw-camille, 2026-10-__.

**Flag for sw-prod (not a test failure):** per sprint-board.md:156-157
("Scope-gate ratification record (R3-TC-09m6) remains with
sw-prod/sw-camille"), the ratification **signature** itself is still
outstanding and is sw-prod/sw-camille's call per the plan §2 ("a
recommendation for sw-prod/sw-camille to ratify"). Everything testable
at this phase is green: the decision is explicit, the guard is
mechanically enforced, and no override decision exists. **PASS with the
ratification signature explicitly carried to sw-prod/sw-camille before
integration.**

## Baseline-vs-regression (R3-TC-09d portion; plan item 6)

- Pre-delivery baseline (r3-004-baseline.md @ `72fcfbf`): 207 tests OK,
  zero warnings; delivery artifacts (guide §13, arch.md R3 records)
  confirmed ABSENT (baseline GAP-1/2/3).
- Post-delivery at test head `01af2dd`: **210 tests OK** (§1) = 207
  pre-existing + 3 new (09a, 09b, 09h). The byte-identity check (§5)
  proves every pre-existing test file is unchanged vs `dev`, so all 207
  pre-existing tests re-ran green, unmodified, inside the suite —
  including the R3-001 (07a–07m), R3-002 (04a–04r) and R3-003
  (01a–06e, 08a–08f, 41 cases) suites and the standalone
  simulation/station/contracts/descriptions/lifecycle anchors pinned by
  predecessor 08a/08c cases. **PASS.**

## Per-case results

| Case ID | Method | Verdict | Evidence |
| --- | --- | --- | --- |
| R3-TC-09a | `tests/test_sim_connected_guide.py` `ConnectedGuideTests.test_r3_tc_09a_golden_trace` | PASS | `... ok` (§1 mapping run; §13 executed output equals the independently design-derived golden trace: prepare tick=0/time=0.0, states 1.0/10.0/100.0/0.0; step 0→1 delivers A.u=100.0 (C's tick-0), B.u=1.0, C.u=10.0 — C's input is 10.0, not 11.0 — D.u=2.0; states 101.0/11.0/110.0/2.0; probe=100.0; drive write 3.0 staged with readback=3.0 and D.x still 2.0 before the next tick; step 1→2 delivers D.u=3.0; coordinated reset restores tick/time/states/pendings and a re-run reproduces the trace) |
| R3-TC-09b | `test_r3_tc_09b_byte_reproduction` | PASS | `... ok` — guide §13 code block executes byte-identically to the guide recorded output block, and repeated execution is byte-identical; the guide code block is the same program as 09a's |
| R3-TC-09c | `tests/test_sim_user_guide.py` SIM-TC-08a–08d | PASS | `... ok` ×4 (§1 mapping run) — §11 standalone example still byte-reproduced; guide topic checklist still green |
| R3-TC-09d | full-suite count + baseline arithmetic | PASS | §1 — 210 tests, OK, exit 0 = 207 pre-existing + 3 new; §"Baseline-vs-regression" |
| R3-TC-09e | warning gate | PASS | §2 — 210 tests OK under `-W error::Warning`, exit 0 |
| R3-TC-09f | compileall + import smoke + export identity | PASS | §§3-4 — compile exit 0 silent; `0.3.0`; `EXPORTS_OK` |
| R3-TC-09g | scope guard | PASS | §5 — production/packaging diff empty; tests diff = two new files only; all pre-existing test files byte-identical to dev; no snapshot/describe change possible |
| R3-TC-09h | `tests/test_arch_links.py` `ArchReferenceTests.test_r3_tc_09h_arch_reference_integrity` | PASS | `... ok` — every reference target in arch.md resolves under the repo root; zero dangling links |
| R3-TC-09m1 | manual record check | PASS | §09m1 — §12/§13 exist; all four required topic groups cited file:line against coordination.py/mock.py/station.py; no overclaim |
| R3-TC-09m2 | manual record check | PASS | §09m2 — §9 standalone-only mechanism retained; deferral paragraph resolved to "Both patterns are implemented"; zero stale "planned" text; 07t pins hold |
| R3-TC-09m3 | manual record check | PASS | §09m3 — header dual-pinning verified incl. ancestor claims; 20-row A9 table matches design; 17 anchors independently verified (≥8 required, incl. object.py:947); [PLANNED] markers only on genuine future work; Release 1/2 content unregressed |
| R3-TC-09m4 | manual record check | PASS | §09m4 — candidate run 38826003029 @ f32b796 success (3.9/3.13); fill run 38826175913 @ 45d4244 success; dev run 38820136267 @ 2991a63 success; final-head run 38828471698 @ 01af2dd success; SHAs match; ancestor relations verified |
| R3-TC-09m5 | manual record check | PASS | §09m5 — R3-AC-01–08 matrix, all rows PASS with traceable anchors; R3-AC-07 flagged and release-level re-verified |
| R3-TC-09m6 | manual record check | PASS (flag recorded) | §09m6 — decision explicit in plan §2 + sprint-board + arch.md; mechanical guard green; disposition text recorded; ratification signature remains with sw-prod/sw-camille |

## Per-AC coverage summary

| AC (R3-004 scope + release-wide) | Verdict | Covered by |
| --- | --- | --- |
| R3-AC-01 — uniform delayed network | PASS (release-level) | r3-003 01a–01e + this doc 09a/09d/09e |
| R3-AC-02 — defined initial state and device timing | PASS (release-level) | r3-003 02a–02f + 09m1 re-citation + 09a |
| R3-AC-03 — shared clock | PASS (release-level) | r3-003 03a–03f + 09m1 re-citation |
| R3-AC-04 — station membership and builders | PASS (release-level) | r3-002 04a–04r + r3-003 04a–04f/08a/08c + 09m3 anchors |
| R3-AC-05 — valid safe connections | PASS (release-level) | r3-003 05a–05g + 09d |
| R3-AC-06 — atomic tick/reset | PASS (release-level) | r3-003 06a–06e + 09m1 + 09a reset demonstration |
| R3-AC-07 — close Release 2 gaps | PASS (release-level, flagged) | r3-001 07a–07t + CHK-08-1 + 09d + 09m2 |
| R3-AC-08 — compatibility and usable delivery (connected example + final evidence) | PASS | 09a–09h + 09m1–09m6 (this doc) |

## Key behavioral pins recorded during execution

- **Worked connected example (09a, guide §13):** four-object station
  (chain A→B→C, feedback C→A, drive→D, C.y→probe) with `sim_dt=0.5`.
  Prepare publishes tick 0 / time 0.0 with states 1.0/10.0/100.0/0.0 and
  probe at its constructor initial −1.0. Step 0→1: every edge delivers
  the previous-boundary value (A.u=100.0 from C's tick-0, B.u=1.0,
  C.u=10.0 — C's input is its driver's *previous* value 10.0, never the
  same-tick 11.0), states 101.0/11.0/110.0/2.0, probe=100.0.
  Between-ticks drive write 3.0: readback shows 3.0 immediately, tick
  and D.x unchanged. Step 1→2 delivers D.u=3.0 (D.x=5.0). Coordinated
  reset restores tick/time/states/pendings/samples in one operation;
  prepare+step re-runs the trace bit-identically.
- **Byte-reproduction (09b):** the §13 guide code block and recorded
  output block are the executed program and its byte-identical output;
  repeated execution is deterministic.

## Warnings observed

**None.** Under `python -W error::Warning` the suite is green (210
tests, OK, exit 0), which is only possible with zero emitted warnings.
No pre-existing Release 1 debt warning (OBS-004/OBS-005/OBS-006 or
DEFECT-2) fired. No waiver, silent or otherwise, was applied.

## CI reference

Local evidence above was produced on Python 3.13.15. CI evidence
(§09m4): candidate run 38826003029 @ `f32b796`, fill-commit run
38826175913 @ `45d4244`, dev-integration run 38820136267 @ `2991a63`,
and final-head run 38828471698 @ `01af2dd` — all `success` on the
Python 3.9/3.13 matrix. Integration to `dev` per AGENTS.md additionally
requires the pre-merge CI to remain green at the merge candidate; no CI
result is marked green before it exists. Out of task scope per plan §5.7:
Product Owner release acceptance, `main` promotion, and the 09m6
ratification signature remain separately authorized steps.

## Issues found

None. No production-code defects were observed during this execution;
no bug reports to sw-tom arise from this run. The only change produced
by this test phase is this record. One process flag (not a defect):
the 09m6 scope-gate ratification signature remains with
sw-prod/sw-camille (sprint-board.md:156-157), with the disposition
text recorded in §09m6.

## Dispositions

1. **No case rebinding was needed.** Plan Revision 2's reworded 09g
   expectation permitted design-bound extensions of
   `tests/test_sim_user_guide.py`; the delivery needed none (the guide
   byte-reproduction convention was reused read-only via the new
   extraction in `test_sim_connected_guide.py`), and the mechanical
   byte-identity check confirms zero edits to existing test files.
2. **[PLAN] cases all became runnable:** 09a/09b/09h ran green at the
   delivery head — baseline GAP-1/2/3 closed by the implementation;
   nothing was marked passed that was not run.
3. **09m6 recorded as PASS with an explicit flag:** every check
   executable by the tester is green (decision explicit in the records,
   mechanical guard enforced, disposition text recorded); the human
   ratification signature is sw-prod/sw-camille's call per the plan and
   is carried to them, not silently closed.
