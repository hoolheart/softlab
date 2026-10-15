# R3-003 test results — cohesive coordinated simulation

Tester: sw-mike | Date: 2026-10-15
Branch: `codex/r3-003-coordination` (implementation HEAD `0003f5d`, synced
with `origin/codex/r3-003-coordination`; this execution adds only this
record and one test-only addition)
Plan: `log/release_3/test/r3-003-test-plan.md` (Revision 2, approved)
Requirements: `log/release_3/prd.md` (R3-AC-01–06 in full; R3-AC-08
compatibility/evidence portion)
Implementation under test: commits `5d8fc6a..1aa961f` (code review
APPROVED at `7a4a30b`)

## Verdict

**PASS — all 41 planned case IDs pass (40 automated plan cases + 1
automated CI-record case + CHK-03-1 manual record check), plus one
test-only extension case added at test phase with recorded disposition.**
Full suite: **207 tests, OK, exit code 0** (206 at implementation HEAD
`0003f5d` = 165 pre-existing unchanged + 41 new R3-003; 207 after the
test-only 04e extension). Zero warnings, including under the
`-W error::Warning` gate. Nothing below is marked passed that was not
run.

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env per AGENTS.md; invoked as `.venv/bin/python` — `conda activate` does not persist across this shell, so the venv interpreter is used directly, same environment) |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |
| New test dependencies | none (stdlib + NumPy only) |
| Unverifiable items | none |

## Commands and verbatim results

### 1. unittest full suite (R3-TC-01a–06e, 08a–08e)

Command:

```
.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
```

Result (at implementation HEAD `0003f5d`, final confirmation run):

```
----------------------------------------------------------------------
Ran 206 tests in 1.130s

OK
```

- Test count: **206** = 165 pre-existing unchanged + 41 new R3-003
  (all 41 in `tests/test_tu_coordination.py`), every verbose line ends
  in `... ok`; 0 `FAIL`/`ERROR` lines.
- Result: **OK**, exit code **0**.
- The backend lines ("Connected to sqlite3 backend …", "Connected to
  HDF5 backend …") are informational prints from the data-backend
  tests, not warnings.
- After the test-only 04e extension (section 10) the re-run gives
  `Ran 207 tests in 1.137s — OK`, exit 0; the warning gate and
  compileall were re-run green at 207 as well.

### 2. Warning gate (R3-TC-08b)

Command:

```
.venv/bin/python -W error::Warning -m unittest discover -s tests -p 'test_*.py'
```

Result: **Ran 207 tests … OK**, exit code **0** — every warning class is
an error under this gate, so a green run proves zero warnings. No
`DeprecationWarning`/`UserWarning`/other warning fired; no Release 1
debt warning (OBS-004/005/006, DEFECT-2) fired — none observed, nothing
waived silently.

### 3. compileall (R3-TC-08c)

Command: `.venv/bin/python -m compileall -q softlab`
Exit code **0**, no output (re-run green after the test-only addition).

### 4. import smoke + new public exports (R3-TC-08c/08d)

Command:

```
.venv/bin/python -c "import softlab; print(softlab.__version__)"
```

Output: `0.3.0`, exit code **0**.

The four new public exports import from their documented path
(`softlab.tu.simulation`, per `softlab/tu/simulation/__init__.py), and
public identity holds against the backing modules:

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

Output: `EXPORTS_OK`. `Coordinator`/`Endpoint` re-export
`softlab.tu.simulation.coordination`; `MockControl`/`MockMeasurement`
re-export `softlab.tu.simulation.mock`.

### 5. Scope gate (R3-TC-08e + CHK-03-1 item 1)

- `git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui
  softlab/mu` → **empty**, exit 0. No `huo` process, scheduler or
  automatic-advancement code anywhere in the production diff
  (`grep -icE 'huo|scheduler'` over the production diff → 0).
- `git diff dev...HEAD --stat -- softlab/ tests/` → exactly **5
  production files, all under `softlab/tu/`**:
  `softlab/tu/simulation/__init__.py` (+8),
  `softlab/tu/simulation/coordination.py` (+1376, NEW),
  `softlab/tu/simulation/mock.py` (+215, NEW),
  `softlab/tu/simulation/object.py` (+218/−9),
  `softlab/tu/station/station.py` (+131); plus one new test file
  `tests/test_tu_coordination.py`. Matches the approved design's
  file-level change list.
- `git diff dev...HEAD --stat -- pyproject.toml setup.py` → **empty**:
  no new required dependencies; Python-support range unchanged.
- Test side: `git diff dev...HEAD --stat -- tests/` → only
  `tests/test_tu_coordination.py` (NEW) at implementation HEAD, i.e.
  **all 165 pre-existing test files are byte-identical to `dev`**.

### 6. CHK-03-1 — five design-gate proofs (file:line citations, verified against actual code at HEAD)

1. **Snapshot publication (one committed snapshot per boundary;
   publication invokes no user code).**
   `Coordinator.step` docstring phases, coordination.py:1037-1100:
   phase 1 freeze (1102-1108), phase 2 candidates "the only user
   code" (1111-1135), phase 3 measurement + clock candidates "no user
   code" (1136-1146), phase 4 publication "no user code" (1151-1161),
   with the in-code marker `# ---- publication: no user code below
   this line ----` also in `prepare` (1032-1036) and `reset`
   (1256-1259). All-outputs snapshot cardinality:
   `_publish_snapshot`, coordination.py:1358-1376 ("Publication
   helper: pure dict rebinds, no user code"). Delayed edges read the
   frozen boundary copies only (`_frozen_edge_value`, 1318-1326;
   phase-2 deliveries at 1123-1126). ✔ (01a/01e/06a/06b green)
2. **Ownership/reentrancy.** Claim-at-enroll `enroll`,
   coordination.py:452-507 (check order stopped → busy → membership →
   duplicate → foreign claim, claim recorded last; release only via
   `withdraw`, 509-566, with reference-absence gate 555-560). The
   `_busy` flag is checked and held (set after operation-specific
   preconditions, cleared in `finally`) on all 13 design-enumerated
   coordinator operations (bodies at coordination.py:483, 538, 594,
   636, 685, 723, 787, 842, 896, 946, 1008, 1095, 1223 — same lines
   the code review verified at `1aa961f`; the intervening `0003f5d` is
   docs-only). Cross-layer guards: `MockControl.write` consults
   `_busy` (mock.py:117-121), `SimulatedObject.set_input` delegates to
   `_guard_set_input` (object.py:521-522 → coordination.py:1264-1296),
   standalone `evolve_once`/`reset` raise `RuntimeError` while claimed
   (object.py:586-590, 715-719). Reentrancy rule documented at
   coordination.py:27-30 and 188-193. ✔ (04a-04e green)
3. **Safe detach (D12).** Class-level contract
   coordination.py:204-207 ("`disconnect` -> `withdraw` ->
   `Station.rm_object`; every removal path is gated on reference
   absence"); `withdraw` docstring 513-520 with the rejection at
   555-560 ("Cannot withdraw …: connections or interval bindings still
   reference it; disconnect and unbind first"); `rm_control`/
   `rm_measurement` reference-absence rejections (coordination.py:685,
   723); `disconnect` docstring 820-825; the `Station.rm_object`
   ownership guard resolves the member first and rejects a claimed
   object with `RuntimeError` (station.py — verified in the diff;
   automated 04a/04b). ✔
4. **Model-dt binding (D11).** `bind_interval`, coordination.py:858-
   925: "**Binding semantics (design D11):** the binding is an
   explicit …" (862-875) — enrolled/input-declared `KeyError`,
   non-float-declared `TypeError`, already-driven `ValueError`
   (single-driver extended to bindings); delivery is
   `float(sim_dt)` each transition (step phase 2, 1131-1132); a
   `dt`-named input without a binding receives no special treatment
   (name inference explicitly absent). Second driver / contradictory
   `set_input` rejected via `_guard_set_input` (coordination.py:1278-
   1288). ✔ (03e green)
5. **Numeric clock limits (D7).** Clock policy documented at
   coordination.py:195-202: "`tick` is an exact unbounded `int`;
   `sim_time` is computed, never accumulated — `sim_time ==
   float(tick) * sim_dt` exactly … A candidate time that is not finite
   fails the tick atomically with `OverflowError` before any
   publication." `sim_time` property: coordination.py:311-330 (exact
   Python expression `float(tick) * sim_dt`); enforcement:
   coordination.py:1143-1150 (`next_time = float(next_tick) * sim_dt`,
   `math.isfinite` gate → `OverflowError` before publication); interval
   representability rule: `_validate_interval`, coordination.py:58-97.
   ✔ (03f green: 5e-324 accepted, largest-finite rejected at set,
   10⁵ ticks of 1e-9 match the exact expression, overflow fails the
   tick atomically)

**CHK-03-1 items 1/2: PASS.**

### 7. CHK-03-1 — documentation citations (file:line, verified at HEAD)

| Required citation (plan item) | Location verified |
| --- | --- |
| sim_dt coercion/representability rule docstring (03b cont./03f) | `coordination.py:60-69` (`_validate_interval` docstring: non-bool `numbers.Real` coerced via `float()`; `Decimal` → `TypeError`; NumPy reals/`Fraction` accepted; bool/non-real/non-finite/non-positive rejected); restated in the setter docstring `station.py:124-142` |
| Guarded-method RuntimeError docstrings, incl. `evolve_once`/`reset` | `object.py:507` (`set_input` Errors), `object.py:572-573` (`evolve_once` Errors: "standalone `evolve_once` is rejected while claimed (withdraw first)"), `object.py:707-708` (`reset` Errors, same wording) — the exact docstrings the code-review fix (`1aa961f`) added; guards at object.py:586-590 and 715-719 |
| Tick-zero mock initial-sample documentation (02d) | `mock.py:148-150` (class docstring: "The **tick-zero sample is the explicitly initialized** … constructor-declared initial sample"), `mock.py:160`, `mock.py:202` ("The constructor initial sample is returned until the first … delivery") |
| Reentrancy-rule documentation (04e) | `coordination.py:27-30` (module docstring), `coordination.py:188-193` (class docstring, design P2-2), `mock.py:99-110` (`MockControl.write` Errors) |
| Safe-detach documentation (04b) | `coordination.py:204-207`, `coordination.py:513-520` (withdraw docstring), `coordination.py:820-825` (disconnect docstring) |
| Reset external-side-effect limitation (06c, PRD line 105) | `coordination.py:1194-1197` (reset docstring: "**External side-effect limit:** atomicity covers library-owned simulation state only; factory/callback side effects are outside the guarantee"); object-side precedent `object.py:697-698` |

**CHK-03-1 items 3/4: PASS.**

### 8. Baseline-vs-regression (R3-TC-08a/08c)

- Pre-implementation baseline (`r3-003-baseline.md`): 165 tests OK,
  zero warnings, coordinated-simulation capability confirmed ABSENT.
- Post-implementation: 206 tests OK at HEAD `0003f5d` (section 1) =
  165 pre-existing + 41 new; `git diff dev...HEAD --stat -- tests/`
  shows the only test change is the NEW `test_tu_coordination.py`, so
  every pre-existing test file is byte-identical to `dev` and all 165
  re-ran green, unmodified, inside the suite (includes the R3-002
  membership/builder 17 methods and the standalone simulation/
  station/contracts/descriptions/lifecycle suites pinned by 08c).
  Standalone anchors: 08c re-runs `test_tu_simulation*.py`,
  `test_tu_station_objects.py`, `test_tu_contracts.py`,
  `test_tu_descriptions.py`, `test_tu_lifecycle.py` green in-suite.
  **PASS.**

### 9. Candidate CI evidence (R3-TC-08f)

Command: `gh run view 38758868819 --json headBranch,headSha,conclusion,jobs`

- Run **38758868819**: `headBranch=codex/r3-003-coordination`,
  `headSha=0003f5d787b3adf0e674faa4b6a525aa3ffe6794` (implementation
  HEAD `0003f5d` under test), `conclusion=success`.
- Both matrix jobs green: `compatibility (3.9)` and
  `compatibility (3.13)` (workflow includes regression, compile and
  import-check steps).
- Pre-merge task-branch evidence per the R3-001 REC-1 lesson. The
  test-only 04e extension and this results record are committed after
  that run; they touch one test method and one log document only.
  **PASS.**

### 10. Dispositions

1. **R3-TC-02b injection-point rebind (recorded).** The plan text arms
   "object initial-state factory **or** tick-zero output evaluation";
   the case as implemented arms the **tick-zero output evaluation** —
   the `observe` callback — with a sentinel (`sentinel =
   RuntimeError('tick-zero boom')`), because the design (D9) defines
   prepare's candidate phase as exactly one `observe` per participant
   on committed states with **no `evolve` and no factory invocation**
   (coordination.py:967-1006, esp. docstring line 973-976 and body
   1024-1025; factories fire only at construction
   (`object.py:393-430` `_resolve_initial`) and reset
   (`object.py:834-864` `_reset_candidate`)). The case pins the
   rebind: `assertIs(caught.exception, sentinel)` (identity),
   `coord.prepared` false, `coord.tick`/`coord.sim_time` raise
   "Simulation not started", `obj.get_state('x') == 1.0` (nothing
   committed), the D4 ownership claim pre-exists enrollment and is
   untouched by the failed preparation
   (`assertIs(obj._active_coordinator, coord)`), and a later healthy
   prepare works from untouched declarations
   (`tests/test_tu_coordination.py:232-266`). The "no factory at
   prepare" half is additionally pinned by 02a's prepare-phase
   counters (zero `evolve`, exactly one `observe` per participant;
   `tests/test_tu_coordination.py:224-230`) and by prepare's
   no-factory side-effect contract (coordination.py:1003-1005).
   **PASS as implemented; disposition recorded.**
2. **Optional R3-TC-04e extension (sim_dt setter `_busy` half) —
   TAKEN.** The code review (M6/Observation 2) verified the normative
   D6-step-5 `_busy` guard only by runtime probe; the suite pinned the
   `prepared` half but not the `_busy` half. At test phase (explicitly
   permitted without a plan revision) I added the automated case
   `OwnershipTests.test_r3_tc_04e_sim_dt_setter_busy_guard_extension`
   (`tests/test_tu_coordination.py`, added after line 720): a callback
   reached **during `prepare()`** (STOPPED state, so the `prepared`
   guard cannot fire — only `_busy` can) attempts
   `station.sim_dt = 0.25`; the test asserts the attempt raises
   `RuntimeError`, the stored interval is unchanged (0.5), the
   preparation still succeeds, and the subsequent tick reads the
   original interval (`coord.sim_time == 0.5`). Test-only addition; no
   production change; red-check not needed per the recorded
   disposition (guard already code- and probe-verified) — the case
   passes on first run. Suite grows 206 → 207. **PASS; disposition
   recorded.**
3. **Planned-test-files naming note.** The plan's placeholder file
   name `tests/test_tu_coordination.py` was kept unchanged — no rebind
   needed.

## Per-case results

| Case ID | Method | Verdict | Evidence |
| --- | --- | --- | --- |
| R3-TC-01a | `DelayedNetworkTests.test_r3_tc_01a_chain_advances_at_most_one_edge` | PASS | `... ok` |
| R3-TC-01b | `test_r3_tc_01b_fan_out_delivers_identical_value` | PASS | `... ok` |
| R3-TC-01c | `test_r3_tc_01c_feedback_uses_initialized_previous_value` | PASS | `... ok` |
| R3-TC-01d | `test_r3_tc_01d_determinism_independent_of_registration_order` | PASS | `... ok` (both build orders, both endpoint-registration orders, inspection interleaving) |
| R3-TC-01e | `test_r3_tc_01e_no_edge_uses_same_tick_source_golden_trace` | PASS | `... ok` (per-tick golden trace) |
| R3-TC-02a | `InitialStateTimingTests.test_r3_tc_02a_inspection_never_prepares_then_explicit_prepare` | PASS | `... ok` |
| R3-TC-02b | `test_r3_tc_02b_failed_tick_zero_leaves_unstarted` | PASS | `... ok`; disposition §10.1 |
| R3-TC-02c | `test_r3_tc_02c_control_write_stages_and_readback_shows_pending` | PASS | `... ok` |
| R3-TC-02d | `test_r3_tc_02d_measurement_tick_zero_sample_is_declared_initial` | PASS | `... ok`; doc citation §7 |
| R3-TC-02e | `test_r3_tc_02e_last_write_wins_before_tick` | PASS | `... ok` |
| R3-TC-02f | `test_r3_tc_02f_reads_writes_inspection_never_tick` | PASS | `... ok` |
| R3-TC-03a | `SharedClockTests.test_r3_tc_03a_valid_interval_shared_by_all_participants` | PASS | `... ok` |
| R3-TC-03b | `test_r3_tc_03b_invalid_interval_values_rejected` | PASS | `... ok` (0, −1.0, nan, ±inf, True, False, '0.5', None, 0.5+0j) |
| R3-TC-03b (cont.) | (same method, coercion half) | PASS | `... ok` (Decimal → TypeError, NumPy real → accepted — rule documented §7) |
| R3-TC-03c | `test_r3_tc_03c_failed_tick_does_not_advance_clock` | PASS | `... ok` |
| R3-TC-03d | `test_r3_tc_03d_mid_run_interval_change_rejected_until_reset` | PASS | `... ok` |
| R3-TC-03e | `test_r3_tc_03e_model_dt_binding_explicit_validated_single_driver` | PASS | `... ok`; binding doc citation §6 item 4 |
| R3-TC-03f | `test_r3_tc_03f_numeric_clock_limits` | PASS | `... ok`; clock-limit proof §6 item 5 |
| R3-TC-04a | `OwnershipTests.test_r3_tc_04a_connected_removal_rejected` | PASS | `... ok` |
| R3-TC-04b | `test_r3_tc_04b_removal_succeeds_after_safe_detach` | PASS | `... ok`; safe-detach citations §6 item 3/§7 |
| R3-TC-04c | `test_r3_tc_04c_shared_active_ownership_conflict_rejected` | PASS | `... ok` |
| R3-TC-04d | `test_r3_tc_04d_standalone_operations_prevented_while_coordinated` | PASS | `... ok` |
| R3-TC-04e | `test_r3_tc_04e_reentrancy_guard_mid_operation` | PASS | `... ok` (nested step + nested control write rejected; nested-propagating failure atomic) |
| R3-TC-04e ext. | `test_r3_tc_04e_sim_dt_setter_busy_guard_extension` (test-only addition) | PASS | §10.2 |
| R3-TC-04f | `test_r3_tc_04f_two_mock_endpoints_on_one_participant_permitted` | PASS | `... ok` |
| R3-TC-05a | `ConnectionSafetyTests.test_r3_tc_05a_endpoint_direction_validation_atomic` | PASS | `... ok` |
| R3-TC-05b | `test_r3_tc_05b_single_driver_per_input` | PASS | `... ok` |
| R3-TC-05c | `test_r3_tc_05c_value_compatibility_at_prepare_and_tick_time` | PASS | `... ok` |
| R3-TC-05d | `test_r3_tc_05d_fan_out_copies_isolate_ndarray_mutation` | PASS | `... ok` |
| R3-TC-05e | `test_r3_tc_05e_unconnected_inputs_retain_staged_values` | PASS | `... ok` |
| R3-TC-05f | `test_r3_tc_05f_topology_changes_require_stopped_boundary` | PASS | `... ok` |
| R3-TC-05g | `test_r3_tc_05g_invalid_graph_changes_individually_atomic` | PASS | `... ok` |
| R3-TC-06a | `AtomicTickResetTests.test_r3_tc_06a_evolve_failure_preserves_network_and_pending` | PASS | `... ok` |
| R3-TC-06b | `test_r3_tc_06b_observe_failure_at_tick_time_is_atomic` | PASS | `... ok` |
| R3-TC-06c | `test_r3_tc_06c_reset_factory_failure_leaves_network_usable` | PASS | `... ok`; side-effect-limit citation §7 |
| R3-TC-06d | `test_r3_tc_06d_successful_reset_rebuilds_everything` | PASS | `... ok` |
| R3-TC-06e | `test_r3_tc_06e_reset_failure_atomic_across_participants` | PASS | `... ok` |
| R3-TC-08a | `CompatibilityEvidenceTests.test_r3_tc_08a_pre_implementation_regression_baseline` | PASS | §8 — 165 pre-existing pass unchanged inside 206/207 OK |
| R3-TC-08b | `test_r3_tc_08b_post_implementation_gates` | PASS | §§1, 2, 3 — 207 OK under `-W error::Warning`; compile exit 0 |
| R3-TC-08c | `test_r3_tc_08c_standalone_compatibility_beyond_new_capability` | PASS | §§1, 4 — suites green; import smoke 0.3.0; export identity §4 |
| R3-TC-08d | `test_r3_tc_08d_no_automatic_device_hook_execution` | PASS | `... ok`; opt-in property verified (coordinator getter never creates; §4 exports) |
| R3-TC-08e | `test_r3_tc_08e_no_new_dependencies_and_scope` | PASS | §5 — production confined to `softlab/tu/` 5 files; packaging metadata untouched |
| R3-TC-08f | `test_r3_tc_08f_candidate_ci_workflow_matrix` | PASS | §9 — CI run 38758868819 green, both matrix jobs, head 0003f5d |
| CHK-03-1 | manual record check | PASS | §§5, 6, 7 — scope empty outside `softlab/tu/`; five design proofs cited; doc citations verified |

## Per-AC coverage summary

| AC (R3-003 scope) | Verdict | Covered by |
| --- | --- | --- |
| R3-AC-01 — uniform delayed network | PASS | R3-TC-01a–01e |
| R3-AC-02 — defined initial state and device timing | PASS | R3-TC-02a–02f (+doc citation 02d) |
| R3-AC-03 — shared clock | PASS | R3-TC-03a–03f; CHK-03-1 (binding §6.4, numeric limits §6.5, coercion doc §7) |
| R3-AC-04 — connected removal & shared-active-ownership | PASS | R3-TC-04a–04f (+04e extension); regression portion via 08a/08c |
| R3-AC-05 — valid safe connections | PASS | R3-TC-05a–05g |
| R3-AC-06 — atomic tick/reset | PASS | R3-TC-06a–06e (+side-effect-limit citation §7) |
| R3-AC-08 — compatibility and evidence (this task's portion) | PASS | R3-TC-08a–08f + CHK-03-1 |

## Key behavioral pins recorded during execution

- **Delayed network (01a–01e):** every edge delivers the source value
  committed at the previous boundary; chain A→B→C at transition 0→1
  gives B=1.0 (A's tick-0), C=10.0 (B's tick-0, not 11.0); fan-out
  delivers identical values; feedback consumes the initialized
  boundary value (cycles permitted); bit-identical 3-tick traces under
  reversed object-build order, reversed mock-endpoint registration
  order, and inspection interleaving; golden trace per tick.
- **Tick-zero/timing (02a–02f):** inspection never prepares or invokes
  callbacks; explicit `prepare()` publishes tick 0 / time 0.0 with
  exactly one `observe` per participant (no evolve, no factories);
  failed tick-zero propagates the identical sentinel, leaves clock/
  network untouched, and a healthy retry works; control writes stage
  pending (readback shows pending, last write wins, delivered at the
  next boundary); measurement tick-zero read is the explicitly
  initialized constructor sample; reads/writes/inspection never tick.
- **Clock (03a–03f):** one station-owned explicit interval shared by
  all participants; 0/negative/NaN/±inf/bool/non-real rejected with
  category-naming errors; Decimal rejected (`TypeError`) and NumPy
  reals coerced per the documented rule; failed ticks don't advance;
  mid-run changes rejected until coordinated reset; model-dt binding
  is explicit, validated, single-driver, never name-inferred;
  `sim_time == float(tick) * sim_dt` exactly, 10⁵ ticks of 1e-9 match
  the exact expression, non-finite candidate time fails the tick with
  `OverflowError` before publication.
- **Ownership (04a–04f):** connected removal rejected naming the active
  coordination; removal succeeds only after disconnect → withdraw →
  rm_object; second-station activation rejected with S1 coherent;
  standalone evolve/reset/connected-input writes rejected while
  claimed (unconnected-input writes still stage); nested step and
  nested control write rejected mid-tick with the in-flight snapshot
  intact (and the propagating-nested-failure tick atomic); two mock
  endpoints on one participant permitted within one simulation.
- **Connections (05a–05g):** invalid endpoints/directions rejected
  before any graph change (bit-identical after); single-driver per
  input; value incompatibility rejected at connection and at tick
  time (tick fails atomically); fan-out copies isolate ndarray
  mutation from destinations and committed snapshots; unconnected
  inputs retain staged values; topology edits require a stopped
  boundary; no batch API — each edit individually atomic.
- **Atomicity (06a–06e):** evolve/observe/factory failures preserve
  the prior committed network, clock and original exception identity
  (`assertIs`); accepted pending writes survive failed ticks and are
  delivered by the retry; successful reset rebuilds participants,
  buffers, controls, measurement samples and the clock as one
  operation (accepted writes do not leak across reset); mid-staging
  factory failure leaves every participant at pre-reset values.

## Warnings observed

**None.** Under `python -W error::Warning` the suite is green (207
tests, OK, exit 0), which is only possible with zero emitted warnings.
No pre-existing Release 1 debt warning (OBS-004/OBS-005/OBS-006 or
DEFECT-2) fired. No waiver, silent or otherwise, was applied.

## CI reference

Local evidence above was produced on Python 3.13.15. The candidate-commit
CI (`.github/workflows/ci.yml`: unittest regression, compile and import
checks on Ubuntu, Python 3.9/3.13 matrix) ran green on the task branch at
the implementation head under test — run 38758868819, both matrix jobs
`success` (§9). Integration to `dev` per AGENTS.md additionally requires
that pre-merge CI to remain green at the merge candidate; no CI result is
marked green before it exists.

## Issues found

None. No production-code defects were observed during this execution; no
bug reports to sw-tom arise from this run. The only changes produced by
this test phase are this record and the test-only R3-TC-04e extension
(§10.2) — no production code was modified.
