# SIM-001 test results — deterministic simulated-object foundation

Tester: sw-mike | Date: 2026-10-05
Branch: `codex/sim-001-simulation-foundation` (HEAD `6734693` at execution;
test commits add `tests/test_tu_simulation.py`,
`tests/test_tu_simulation_integration.py`, `tests/test_sim_user_guide.py`
and this record)
Plan: `log/release_2/test/sim-001-test-plan.md` (revision 2, approved)
Requirements: `log/release_2/prd.md` SIM-AC-01 through SIM-AC-08
Implementation under test: `softlab/tu/simulation/` (commits
`4eab94d..6914475`, code review APPROVED at `dfa535c`)

## Verdict

**PASS — all 41 planned case IDs pass; full suite 135 tests, OK, exit
code 0, zero warnings (including under the `-W error::Warning` gate).**
Nothing below is marked passed that was not run; the only items not
executed as automated unittest cases are explicitly identified as
review-gate evidence (CHK-06-1/CHK-06-2) or integration-gate CI,
with their required dispositions recorded.

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env, per AGENTS.md) |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |
| New test dependencies | none (stdlib + NumPy only) |

## Commands and verbatim results

### 1. unittest full suite (SIM-TC-07b)

Command:

```
python -m unittest discover -s tests -p 'test_*.py'
```

Output (verbatim, final confirmation run):

```
.......................................................................................................................................
----------------------------------------------------------------------
Ran 135 tests in 0.444s

OK
Connected to sqlite3 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmpgf0xx5uv/readings.db.
Connected to HDF5 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmpgf0xx5uv/readings.hdf5.
```

- Test count: **135** (baseline 99 + 36 new SIM-001 tests:
  27 object-contract + 5 integration + 4 user-guide)
- Result: **OK**
- Exit code: **0**
- The two backend lines are informational prints from the data-backend
  tests, not warnings.

### 2. compileall (SIM-TC-07b)

Command: `python -m compileall -q softlab`
Exit code: **0**, no output.

### 3. import smoke (SIM-TC-07b)

Command: `python -c "import softlab; print(softlab.__version__)"`
Output: `0.3.0`, exit code **0**.

### 4. Warning gate (SIM-TC-07b / SIM-TC-07e)

Command:

```
python -W error::Warning -m unittest discover -s tests -p 'test_*.py'
```

Result: **135 tests, OK, exit code 0** — every warning class is an
error under this gate, so a green run proves zero warnings. No
`DeprecationWarning`/`UserWarning`/other warning fired; in particular
**no Release 1 debt warning (OBS-004/OBS-005/OBS-006/DEFECT-2) fired —
none observed, nothing waived silently** (SIM-TC-07e disposition).

### 5. Whitespace/conflict check

Command: `git diff --check`
Exit code: **0**, no output.

### 6. New public imports without circularity (SIM-TC-07c)

Command (verbatim output):

```
$ python -c "
import importlib
import softlab
import softlab.tu
import softlab.tu.simulation
import softlab.tu.simulation.object as object_mod
from softlab.tu.simulation import SimulatedObject
assert SimulatedObject is object_mod.SimulatedObject
assert softlab.tu.simulation.SimulatedObject is SimulatedObject
assert 'simulation' in softlab.tu.__dict__ or hasattr(softlab.tu, 'simulation')
print('SIM-TC-07c import/circularity check OK, version', softlab.__version__)
"
SIM-TC-07c import/circularity check OK, version 0.3.0
```

All imports succeed, no circular-import error, public class identity
holds across `softlab.tu.simulation` and
`softlab.tu.simulation.object` (same pattern as Release 1
`public_exports_retain_class_identity`).

### 7. No new required dependencies (SIM-TC-07d)

Command: `git diff f04e783..HEAD -- pyproject.toml setup.py | wc -l`
Output: `0` — zero changes to packaging metadata; optional extras
untouched; imports work in the existing environment.

### 8. Review-gate checklist items (SIM-AC-06, not in automated tally)

- **CHK-06-1 (zero `huo`/`jin`/`shui`/`mu` production edits)** —
  **discharged by the code reviewer at `dfa535c`** (recorded in
  `log/release_2/reviews/sim-001-code-review.md`: `git diff
  99da38c..HEAD -- softlab/huo softlab/jin softlab/shui softlab/mu` is
  empty, 0 lines). Independently re-verified by the tester at execution
  time: `git diff 99da38c..HEAD --stat -- softlab/huo softlab/jin
  softlab/shui softlab/mu` → empty, exit 0. **Reviewed evidence; PASS.**
- **CHK-06-2 (integration file imports `huo` only via public names)** —
  **discharged by the code reviewer at `dfa535c`** (huo imports via
  public names only). Tester-side supporting evidence:
  `tests/test_tu_simulation_integration.py` imports `count`, `scan`,
  `run_process` from `softlab.huo.process` and `get_scheduler` from
  `softlab.huo.scheduler` (the public name and start/stop pattern used
  by the existing `tests/test_tu_integration.py`); no private module is
  imported. **Reviewed evidence; PASS.**

## Per-case results

| Case ID | Verdict | Evidence |
| --- | --- | --- |
| SIM-TC-01a | PASS | `tests/test_tu_simulation.py` `test_sim_tc_01a_scalar_declaration_contract` |
| SIM-TC-01b | PASS | `test_sim_tc_01b_ndarray_value_contract` |
| SIM-TC-01c | PASS | `test_sim_tc_01c_invalid_declaration_errors` |
| SIM-TC-01d | PASS | `test_sim_tc_01d_unknown_variable_errors` |
| SIM-TC-01e | PASS | `test_sim_tc_01e_incompatible_value_errors` |
| SIM-TC-02a | PASS | `test_sim_tc_02a_accumulating_evolve_semantics` |
| SIM-TC-02b | PASS | `test_sim_tc_02b_sequence_accumulates_deterministically` |
| SIM-TC-02c | PASS | `test_sim_tc_02c_memoryless_model` |
| SIM-TC-02d | PASS | `test_sim_tc_02d_dt_as_input_context` |
| SIM-TC-02e | PASS | `test_sim_tc_02e_output_from_resulting_state` |
| SIM-TC-03a | PASS | `test_sim_tc_03a_inspection_does_not_invoke_evolve` |
| SIM-TC-03b | PASS | `test_sim_tc_03b_repeated_observation_stability` |
| SIM-TC-03c | PASS | `test_sim_tc_03c_input_assignment_alone_does_not_evolve` |
| SIM-TC-03d | PASS | `test_sim_tc_03d_no_hardware_scheduler_access_on_read` |
| SIM-TC-04a | PASS | `test_sim_tc_04a_reset_restores_initial_condition` |
| SIM-TC-04b | PASS | `test_sim_tc_04b_replay_reproduces_observations` |
| SIM-TC-04c | PASS | `test_sim_tc_04c_independent_objects_do_not_share_mutable_state` |
| SIM-TC-04d | PASS | `test_sim_tc_04d_reset_after_failed_evolution` |
| SIM-TC-05a | PASS | `test_sim_tc_05a_failure_atomicity_input_update` |
| SIM-TC-05b | PASS | `test_sim_tc_05b_failure_atomicity_evolve` |
| SIM-TC-05c | PASS | `test_sim_tc_05c_mutable_alias_protection_caller_supplied` |
| SIM-TC-05d | PASS | `test_sim_tc_05d_mutable_alias_protection_returned_values` |
| SIM-TC-05e | PASS | `test_sim_tc_05e_mutable_alias_protection_callback_arguments` |
| SIM-TC-05f | PASS | `test_sim_tc_05f_unsupported_value_category_documented` |
| SIM-TC-05g | PASS | `test_sim_tc_05g_external_side_effects_out_of_rollback_scope` |
| SIM-TC-05h | PASS | `test_sim_tc_05h_failure_atomicity_observation_callback` |
| SIM-TC-05i | PASS | `test_sim_tc_05i_failure_atomicity_reset` |
| SIM-TC-06a | PASS | `tests/test_tu_simulation_integration.py` `test_sim_tc_06a_parameter_bridge_for_inputs` |
| SIM-TC-06b | PASS | `test_sim_tc_06b_parameter_bridge_for_observations` |
| SIM-TC-06c | PASS | `test_sim_tc_06c_one_object_shared_between_devices` |
| SIM-TC-06d | PASS | `test_sim_tc_06d_huo_count_integration_with_explicit_stepping` |
| SIM-TC-06e | PASS | `test_sim_tc_06e_huo_scan_integration_with_explicit_stepping` |
| SIM-TC-07a | PASS | Pre-implementation baseline re-run recorded in `sim-001-baseline.md` (99 tests, OK, exit 0, zero warnings) |
| SIM-TC-07b | PASS | Section "1. unittest full suite" (135 tests OK), "2. compileall", "3. import smoke", "4. Warning gate" |
| SIM-TC-07c | PASS | Section "6. New public imports without circularity" |
| SIM-TC-07d | PASS | Section "7. No new required dependencies" |
| SIM-TC-07e | PASS | Section "4. Warning gate" — no OBS-004/005/006 or DEFECT-2 warning observed; no silent waiver |
| SIM-TC-08a | PASS | `tests/test_sim_user_guide.py` `test_sim_tc_08a_guide_topic_checklist` |
| SIM-TC-08b | PASS | `test_sim_tc_08b_executed_deterministic_example` |
| SIM-TC-08c | PASS | `test_sim_tc_08c_api_docstrings` |
| SIM-TC-08d | PASS | `test_sim_tc_08d_evidence_assembly` (this record) |

## Per-AC coverage summary

| AC | Verdict | Covered by |
| --- | --- | --- |
| SIM-AC-01 | PASS | SIM-TC-01a–01e |
| SIM-AC-02 | PASS | SIM-TC-02a–02e |
| SIM-AC-03 | PASS | SIM-TC-03a–03d |
| SIM-AC-04 | PASS | SIM-TC-04a–04d |
| SIM-AC-05 | PASS | SIM-TC-05a–05i |
| SIM-AC-06 | PASS | SIM-TC-06a–06e (automated) + CHK-06-1/CHK-06-2 (review-gate evidence, section 8) |
| SIM-AC-07 | PASS | SIM-TC-07a–07e |
| SIM-AC-08 | PASS | SIM-TC-08a–08d |

## Key behavioral pins recorded during execution

- **Evolve-once semantics (SIM-TC-02a/02b):** callback counters prove
  exactly one `evolve` invocation per explicit `evolve_once()`, and none
  from construction, `set_input`, inspection, `observe_outputs` or
  `reset`.
- **Hook stepping (SIM-TC-06d/06e):** `count` with
  `hook_before_get=step` and `scan` with `hook_after_set=step` each
  evolve exactly once per point (5/5 and 4/4 respectively); recorded
  values equal manual replay of the identical steps, pinning the
  ordering constraint (never `hook_before_set`; the point value is
  committed before evolution). Delays default to zero — no wall-clock
  sleep is interpreted as a simulation step; all stepping is synchronous
  in the hook.
- **Scheduler hygiene (AGENTS.md):** huo tests start the scheduler via
  `get_scheduler()` in `setUp` and stop it in `tearDown` when running;
  `run_process` is given the scheduler explicitly; no global state
  leaks between tests (135-test suite passes in any order within the
  discovery run).
- **Failure atomicity (SIM-TC-05a–05i):** original exception objects
  propagate with identity preserved (`assertIs` on the identical
  `RuntimeError`/`ValueError` objects; `__cause__` is `None` — never
  wrapped); failed input update / evolve / observe / reset leave
  committed state bit-identical to the pre-failure snapshot.

## Warnings observed

**None.** Under `python -W error::Warning` the suite is green (135
tests, OK, exit 0), which is only possible with zero emitted warnings.
No pre-existing Release 1 debt warning (OBS-004/OBS-005/OBS-006 or
DEFECT-2) fired; SIM-TC-07e therefore requires no disposition beyond
this explicit none-observed record. No waiver, silent or otherwise, was
applied.

## CI reference (integration gate, per AGENTS.md)

Local evidence above was produced on Python 3.13.15. Integration to
`dev` additionally requires the candidate-commit CI
(`.github/workflows/ci.yml`: unittest regression, compile and import
checks on Ubuntu, Python 3.9/3.13 matrix) to pass; that CI run is an
integration-gate step and is **not** claimed as passed here — no CI
result is marked green before it exists.

## Issues found

None. No production-code defects were observed; no bug reports to
sw-tom arise from this execution.
