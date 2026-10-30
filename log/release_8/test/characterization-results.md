# M02-W01 characterization results

Owner: Tester (sw-mike role) | 2026-10-30 Asia/Shanghai
Status: **EXECUTED** — assertion-based characterization ran against the
unchanged production baseline after user-authorized resume. No production
code was modified. This report records actual outcomes only; no CI,
integration, promotion or release acceptance is claimed.

## Environment

| Item | Value |
| --- | --- |
| OS | macOS (darwin, arm64) |
| Python | 3.13.15 (repo `.venv`, conda-created per AGENTS.md) |
| softlab | 0.3.0 (editable install of the baseline tree) |
| numpy | 2.5.3 |
| pandas | 3.0.6 |
| scipy | 1.18.1 |
| matplotlib | 3.11.2 |
| h5py | 3.16.0 |
| plotly | 7.1.0 |
| PyVISA / PyVISA-sim | 1.16.2 / 0.7.1 (installed, not exercised) |
| Baseline revision | `35e6e9be6ce38dddbc4178c58bde4122d34c77c8` |
| Branch | `codex/m02-w01-preparation` |
| Working tree | clean before test creation; only the new test file added |

Python 3.9 was **not** exercised; the CI matrix counts only when an actual
candidate run succeeds (none claimed here).

## Commands and actual outcomes

All commands from the repository root with `.venv/bin/python`.

| Command | Outcome |
| --- | --- |
| `python --version` | Python 3.13.15 |
| `python -m pip list` | recorded above (grep of scientific stack) |
| `git status --short` | clean at baseline; `?? tests/test_m02_w01_characterization.py` after creation |
| `git rev-parse HEAD` | `35e6e9be6ce38dddbc4178c58bde4122d34c77c8` |
| `python -W all -m unittest discover -s tests -p 'test_m02_w01_characterization.py'` | **Ran 25 tests — OK** (no warnings, no skips) |
| `python -W all -m unittest discover -s tests -p 'test_huo_execution_contracts.py'` | Ran 7 tests — OK |
| `python -W all -m unittest discover -s tests -p 'test_thermal_execution.py'` | Ran 21 tests — OK |
| `python -W all -m unittest discover -s tests -p 'test_thermal_adapters.py'` | Ran 6 tests — OK |
| `python -W all -m unittest discover -s tests -p 'test_*.py'` | **Ran 306 tests — OK** (281 pre-existing baseline + 25 new) |
| `python -m compileall -q softlab` | OK |
| `python -m compileall -q tests/test_m02_w01_characterization.py` | OK |
| `python -c "import softlab; print(softlab.__version__)"` | `0.3.0` |
| `git diff --check` | clean |

**Warnings**: none observed in any run above under `-W all`. The M01
historical pandas concatenation FutureWarnings and Actions runtime notices
did **not** reproduce in this environment; per W01-AC06 they are recorded as
not observed here, not waived.

**Classification**: every test passes; there are no new-test failures to
classify. All behavioral findings below are **baseline** (pre-existing)
behaviors characterized by assertion, not defects introduced by this work.

## Assertion traceability (plan T-IDs → W01-AC IDs → executable evidence)

| Plan ID / AC | Executable evidence (test method, all PASS) |
| --- | --- |
| T02 / AC02, AC05 | `CountCompatibility.test_count_two_readings_hooks_record_and_shape` — ordered hook/get events, 2 values/rows, column order, `(bool, elapsed)` shape, completion, scheduler reusable before caller cleanup. `test_count_invalid_inputs_and_commit_join_transitions` — invalid count/getter inputs raise documented exceptions; commit/join against a non-started scheduler return False; chained/threaded actions rejected. |
| T03 / AC02, AC05 | `ScanCompatibility.test_scan_traces_rows_dry_run_and_repeat` — hook/set/get order vs predeclared trace, set/get values from rows, terminal dry-run = one extra dispatch with no set/get events and no row (3 waits, 2 rows), reset/repeat appends. |
| T04 / AC01, AC02, AC06 | `SchedulerIsolation.test_singleton_probe_in_child_process` — singleton start/commit/wait True, stop semantics, wait-after-stop raises RuntimeError, commit-after-stop False, borrowed loop usable. `test_private_concrete_scheduler_probe_in_child_process` — see F-01/F-02. `test_thermal_private_scheduler_owns_loop_and_leaves_borrowed_state` — thermal serial scheduler executes actions, leaves borrowed thread loop and module-global singleton untouched. |
| T05 / AC02, AC03 | `ThermalPolicyComparison.test_coordinated_previous_boundary_policy` and `test_direct_current_boundary_policy_extra_observation` — command-once then per-row advance/read(/observe)/append order vs predeclared traces; independent Euler oracle matches both timing policies; source ticks [0..7] vs [1..8]; extra pure observation verified (F-05); persistence replay separated (F-04). |
| T06 / AC03, AC04 | `SignalMonitorClient.test_three_boundaries_paired_readings_and_differences` — exact predeclared policy trace (`configure(5)`; per boundary advance/signal/reference/difference/append), 3 reads per source plus counting-double agreement, count-owned vs caller-owned derived rows both inspectable, no heater policy. |
| T07 / AC02, AC03, AC05 | `SignalMonitorFailures.test_first_read_failure_at_boundary_two`, `test_second_read_failure_consumes_signal_value` — independent first/second read faults: attempt+exception events, exact consumed counts (failed reference read still consumed the second signal value), one completed pair/derived row, boundary 2 reached, no derived row for the failed boundary, no rollback, run_process returns False with scheduler diagnostics. `ThermalFaultFixtures.test_advance_fault_before_boundary_retains_partial_rows` / `test_read_fault_after_boundary_retains_partial_rows` — thermal advance fault at tick 3 keeps rows=3/tick=3; read fault at tick 4 keeps rows=3/tick=4 (boundary ≠ row count); cause identity retained. |
| T08 / AC02, AC05 | `StopResetCancellation` — stop before first cycle acknowledges with zero steps; mid-run stop after row 3 prevents later cycles (boundaries [1,2,3]); late request after completion is not acknowledged and does not rewrite the completed outcome; reset during a running body signals abort without preemption; data-group change while pending raises RuntimeError; private `_CtrlPoint.cancel` seam: cancelled point reports done, status/wait raise CancelledError (private characterization, not a public API). |
| T09 / AC02, AC05 | `ThermalFaultFixtures.test_failed_run_closes_owned_resources` — failed acquisition still drains and closes the owned loop, no cleanup errors, terminal snapshot. `test_primary_failure_with_obstructed_cleanup_records_both` — primary cause and obstructed-cleanup diagnostics recorded independently; other cleanup actions still attempted and evidenced (work drained, loop closed). `RepeatAndTransitions.test_invalid_scheduler_operations_observed_outcomes` — commit rejected after stop, unknown point reports False. |
| T10 / AC02, AC05 | `RepeatAndTransitions` — two fresh runs after success and after interruption match numerically excluding `timestamp`, with fresh group identities; legacy count repeat **appends** rows instead of clearing and `reset()` does not clear the record. |
| T01, T11, T12 / AC01, AC04, AC07, AC08 | Documentation and gate activities; no executable claims made here. |

## Baseline findings characterized (observed behavior, not new guarantees)

- **F-01 (R4-BASE-SCHED-01 confirmed at runtime)** — A concrete
  `_SchedulerImpl` created directly (not via `get_scheduler()`) executes its
  committed action, but the action done-callback routes completion through
  the module-global `_scheduler`; the post point therefore never finishes.
  Observed in an isolated child process: `wait_point(point, timeout=2)`
  raised `CancelledError` after the bounded wait. The singleton obtained via
  `get_scheduler()` completes normally (`wait_point` → True). The thermal
  `_CountScheduler` avoids the broken path by dispatching the action inside
  `wait_point` itself.
- **F-02** — `_CtrlPoint.wait(timeout)` does not deliver the documented
  `None` on timeout: `asyncio.wait_for` cancels the pending future and
  `_CtrlPoint.status` then raises `CancelledError` on the cancelled future.
  Any still-pending point waited with a timeout raises instead of returning
  a status.
- **F-03** — Thermal `_CountScheduler.wait_point` reports `False` (not
  `None`) for unknown or missing points, and rejects commits silently after
  stop. Distinguish these outcomes; they are not interchangeable.
- **F-04** — HDF5 reload and round-trip verification replay `add_rows` on
  rebuilt records: T05 spies observed 9 extra append events after the 8
  acquisition rows (8 reload row appends + 1 record copy). Instrumentation
  must tag record identity to keep persistence replay out of acquisition
  order evidence.
- **F-05 (hypothesis verified)** — The direct current-boundary adapter never
  calls `MockMeasurement.read` and performs exactly one extra pure
  `observe_outputs` call per completed row (8 extra model observations for
  8 rows); the coordinated previous-boundary adapter performs zero
  `observe_outputs` calls (frozen boundary delivery). The extra call is a
  real extra observation of the model output, visible to stateful sources.
- **F-06** — Legacy `count`/`scan` rerun appends rows to the existing record
  (no clear); `reset()` restores run eligibility without touching the
  record. T03/T10 assert this as observed legacy behavior only.
- **F-07** — `DataGroup.record(name)` returns a copy; inspection code must
  hold the process-owned record (or re-fetch) to observe appended rows.

## Resource ownership and cleanup evidence

- Every caller-owned thermal scheduler is stopped and its private loop
  closed via test `addCleanup`/`finally`; the suite reuses no global default
  scheduler instance in-process.
- `impl_scheduler._scheduler` module-global identity is captured before and
  asserted unchanged after the in-process scheduler tests; singleton and
  private-concrete probes each ran in their own child process with a 30 s
  parent deadline (never hit; children exited 0 after closing owned loops in
  normal cooperative paths).
- Thermal `execute` evidence on failure and obstruction paths:
  `work_drained=True`, `loop_closed=True`, owned scheduler stopped;
  obstructed `stop` is recorded in `cleanup_errors` without hiding the
  primary cause or the remaining cleanup attempts.
- Synthetic data only (temporary directories auto-removed); no instruments,
  no VISA, no network, no real storage backends beyond temporary HDF5 files
  created by the unchanged example.

## Limitations (explicitly not run / not claimed)

- Python 3.9 and CI matrix not exercised; results hold for the recorded
  environment only. Exact-candidate CI, integration, promotion and product
  acceptance remain future gates (W01-AC08).
- Child-probe cleanup was verified only on cooperative exit paths; forced
  termination cleanup remains unverifiable by design (never triggered here).
- The scan setter-value assertion reads the recorded rows rather than
  spying `Parameter.__call__`, because the job sets through the class-level
  call path; hook/set/get ordering is asserted from the hook and getter
  events.
- `run_process` verbose console output was suppressed only where the plan
  asserts return shapes; no behavior depends on it.
- T01 inventory, T11 decision record and T12 closure remain with their
  document/gate owners; this report supplies their runtime evidence input.
