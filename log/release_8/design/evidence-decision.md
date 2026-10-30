# M02-W01 evidence decision — minimal justified seam for bounded count execution

Owner: Software Designer (sw-celeste role) | 2026-10-30 Asia/Shanghai
Status: **RECORDED — PENDING Architect boundary review and Product Owner scope
agreement**. This document decides the W01 extraction question from runtime
evidence. It ships **no executor**; per W01-AC05 it only supplies the safe
boundary for the next release. W02 implementation remains unauthorized until
the pending reviews and release gates complete.

Inputs: [PRD](../prd.md) (W01-AC01–AC08), [detailed design](characterization-design.md),
[characterization results](../test/characterization-results.md) (findings F-01…F-07,
suite of 25 tests, 306-test full discovery green at baseline `35e6e9b`,
Python 3.13.15), executable evidence in
`tests/test_m02_w01_characterization.py`, and the
[M02 plan](../../../plan/milestone-02.md).

## 1. Evidence summary with traceability

| Evidence | Finding / test | AC | Decision relevance |
| --- | --- | --- | --- |
| Directly constructed `_SchedulerImpl` executes its action but the done-callback routes completion through the module-global `_scheduler`; the post point never finishes and a bounded `wait_point(point, timeout=2)` raises `CancelledError`. The `get_scheduler()` singleton completes normally. | F-01; `SchedulerIsolation.test_private_concrete_scheduler_probe_in_child_process` (child process, exit 0) | AC02, AC05 | **Refutes private ownership of the generic concrete scheduler.** R4-BASE-SCHED-01 is confirmed at runtime, not just in source. |
| `_CtrlPoint.wait(timeout)` does not deliver its documented `None` on timeout: `asyncio.wait_for` cancels the pending future and `status` then raises `CancelledError`. | F-02; same private probe; `StopResetCancellation.test_private_control_point_cancellation_seam` (explicitly private characterization) | AC02, AC05 | The generic scheduler's documented wait contract differs from runtime; a seam cannot rely on it. |
| Thermal `_CountScheduler` returns `False` (not `None`) for unknown/missing points and rejects commits silently after stop; distinct outcomes, not interchangeable. | F-03; `RepeatAndTransitions.test_invalid_scheduler_operations_observed_outcomes`, `CountCompatibility.test_count_invalid_inputs_and_commit_join_transitions` | AC02, AC05 | The rejection policy of the proven serial executor is characterized and must be carried forward verbatim. |
| HDF5 reload replays `add_rows` on rebuilt records (9 extra appends vs 8 acquisition rows); `DataGroup.record(name)` returns a copy. | F-04, F-07; `ThermalPolicyComparison.split_trace` | AC03 | Persistence replay is separable from acquisition order; not an executor concern. Confirms instrumentation/ownership rules for W02 tests. |
| Direct current-boundary adapter performs zero `MockMeasurement.read` and exactly one extra pure `observe_outputs` per completed row; coordinated adapter performs zero extra observations. | F-05 (pre-registered hypothesis **verified**); `ThermalPolicyComparison` both tests | AC03 | The extra observation is **caller/adapter policy**, not executor behavior. This is the sharpest evidence of where the shared/caller-owned boundary lies. |
| Legacy `count`/`scan` rerun appends rows; `reset()` restores run eligibility without clearing the record. | F-06; `ScanCompatibility`, `RepeatAndTransitions.test_count_record_append_versus_clear_on_repeat` | AC02, AC05 | Legacy append semantics are observed behavior; the seam must not promise clearing. |
| Public `count` + `run_process` work unchanged through a caller-owned serial executor: ordered hook/get events, exact rows and column order, `(bool, elapsed)` return, scheduler reusable after the run. | T02; `CountCompatibility` | AC02, AC05 | **Demonstrates the reuse hypothesis**: the existing public `Scheduler` interface already decouples count execution from executor implementation. |
| Scan setter/getter order, terminal dry-run (3 waits, 2 rows), reset/repeat append — all through the same caller-owned executor. | T03; `ScanCompatibility` | AC02 | The same seam carries scan without modification; no scan extension is implied. |
| The distinct signal-monitoring client runs through public count machinery with its own acquisition policy: exact predeclared trace, 3 reads per source confirmed by the counting double, count-owned and caller-owned derived rows both inspectable. | T06; `SignalMonitorClient` | AC03, AC04 | A **second real client** (not a rename, not a double alone) reuses the same serial execution shape — executor reuse across runs through public `count`/`run_process` — with a different acquisition policy. T06/T07 do **not** demonstrate terminal-state, stop-acknowledgment or owned-cleanup behavior; that evidence is thermal-derived (T08/T09). |
| First/second read failures: exact consumed counts (failed reference read still consumed the second signal value), one completed pair, boundary 2 reached, no derived row for the failed boundary, no rollback, `run_process` returns `False` with scheduler diagnostics. | T07; `SignalMonitorFailures` | AC03, AC04 | Failure truthfulness is achievable at the seam boundary; partial consumption is caller-policy-visible and must stay observable. |
| Thermal advance fault at tick 3 keeps rows=3/tick=3; read fault at tick 4 keeps rows=3/tick=4 (boundary ≠ row count); cause identity retained. | T07; `ThermalFaultFixtures` | AC03, AC05 | The truthful partial-row/final-boundary invariant required by AC05 is demonstrated. |
| Failed run drains and closes the owned loop (`work_drained`, `loop_closed`); obstructed `stop` is recorded in `cleanup_errors` without hiding the primary cause; other cleanup still attempted. | T09; `ThermalFaultFixtures` cleanup tests | AC05 | Owned-resource cleanup and primary/cleanup error independence are demonstrated and must be carried forward. |
| Stop before first cycle acknowledges with zero steps; mid-run stop after row 3 prevents later cycles; late request after completion is not acknowledged and does not rewrite the completed outcome. | T08; `StopResetCancellation` | AC02, AC05 | Cooperative request/acknowledgment semantics demonstrated; no preemption is claimed or needed. |
| Two fresh runs after success and after interruption match numerically excluding `timestamp`, with fresh group identities. | T10; `RepeatAndTransitions` | AC05 | Repeat-with-fresh-identity behavior demonstrated. |
| Thermal private scheduler owns its loop and leaves the borrowed thread loop and the module-global singleton untouched. | T04; `SchedulerIsolation.test_thermal_private_scheduler_owns_loop_and_leaves_borrowed_state` | AC01, AC05 | The ownership invariant (own your loop, never touch borrowed/global resources) holds at runtime. |

Limitations respected: Python 3.9 and CI were not exercised; child-probe
cleanup was verified only on cooperative exit paths; no hardware, VISA or real
storage backends were involved. Nothing below claims evidence outside the
recorded environment.

## 2. Per-candidate verdict

The approved design compared three candidates: (1) retained local execution,
(2) reuse or compatible extension of existing helpers, (3) a minimal additive
seam.

### Candidate 1 — Retained local execution: **REJECTED (refuted by evidence)**

The design required "concrete shared behavior across real clients" to justify
any seam. The evidence now supplies it. Two real clients with distinct
acquisition policies — the thermal acquisition loop (`_acquire`, both adapters)
and the signal-monitoring client (T06/T07) — execute the **same** serial
execution shape: caller-owned serial executor with a private loop, one
unchained nonthreaded `Action` per acquired point, commit/join per boundary
through the public `count`/`run_process` path, and truthful action-failure
propagation with caller-visible partial consumption (T07). The shared shape
demonstrated by both clients ends there. Truthful terminal state, cooperative
stop acknowledgment, and owned cleanup (drain/close with `cleanup_errors`
diagnostics) are demonstrated **only by the thermal client** (T08/T09); the
monitoring fixture implements no terminal result, stop acknowledgment,
owned-work drain, or cleanup diagnostics — its test cleanup merely stops and
closes the fixture scheduler. Those lifecycle obligations are thermal-derived
W02 contract requirements, not second-client demonstrations. Without a seam,
the monitor client must copy the executor and its rejection/failure semantics
verbatim (the characterization suite literally reuses the thermal
`_CountScheduler` as a fixture for lack of a shared one). Retaining local
execution preserves a demonstrated duplication, contrary to the M02 product
outcome.

### Candidate 2 — Reuse/extend the existing generic scheduler: **REJECTED (refuted by evidence)**

This is the candidate the runtime evidence kills:

- **F-01** confirms R4-BASE-SCHED-01 at runtime: a privately owned
  `_SchedulerImpl` never completes its points because `_callback_action`
  routes through the module-global `_scheduler`. The only working
  configuration is the `get_scheduler()` singleton — whose global ownership is
  fundamentally incompatible with the per-run owned lifecycle (drain, close,
  stop without touching shared state) that AC05 and the cleanup evidence
  require.
- **F-02** shows the generic `_CtrlPoint.wait(timeout)` breaks its own
  documented contract (`None` on timeout) by raising `CancelledError` on the
  cancelled future.
- Repairing the generic scheduler is explicitly outside W01's scope per the
  PRD disposition of R4-BASE-SCHED-01 ("a generic singleton repair is outside
  implicit scope"), and extending it for private ownership would be exactly
  the speculative, evidence-free API invention the PRD forbids.

Reuse of the **public interfaces** (`Scheduler`, `count`, `scan`,
`run_process`) is demonstrated; reuse of the generic **implementation** is
refuted. These must not be conflated.

### Candidate 3 — Minimal additive seam: **ACCEPTED**

The seam is demonstrated, narrow, and already proven in two operational
contexts at runtime: a **bounded serial count executor** implementing the
existing public `Scheduler` interface, privately owning its event loop,
accepting one unchained nonthreaded action per point, with the rejection,
failure-truthfulness and cleanup policies characterized by the suite. The
thermal `_CountScheduler` is its working prototype; the monitor client (T06)
proves a second distinct client runs on the same shape; F-05 proves the
acquisition/timing policy (including the extra pure observation) stays with
callers, not the executor.

**Decision: the reuse hypothesis is DEMONSTRATED for candidate 3. No M02
milestone scope revision is required.** The evidence supports proceeding to
W02 with the boundary defined below. Scope revision was the fallback only if
reuse were unproven; it is not triggered.

## 3. Final decision

**Accept the minimal justified seam (candidate 3).** W02 implements, in the
appropriate existing `huo` module (exact placement is a W02 detailed-design
decision), a bounded serial count executor against the unchanged public
`Scheduler` interface. W01 ships no executor, no production code and no
public API; this record is its only boundary deliverable (W01-AC05).

### 3.1 Supported boundary for W02

| Aspect | Supported (must carry forward) | Evidence |
| --- | --- | --- |
| Interface | Implements the existing public `softlab.huo.scheduler.Scheduler` interface; consumed by unchanged public `count`, `scan` (as-is), `run_process`, `Process.commit/join/reset` — with unchanged legacy reset/abort semantics (see 3.5, item 1). Legacy imports and behavior preserved. | T02, T03, T06 |
| Execution model | Serial, bounded: one `Action` per acquired point, dispatched inside `wait_point` on the executor's privately owned loop; sync bodies and awaitable results both dispatchable (as characterized). | F-01 workaround, T02, T04 |
| Ownership | Executor owns its private event loop; the caller owns the executor and stops/drains/closes it after terminal state; borrowed thread loops and the module-global singleton are never mutated, stopped or closed. | T04, T09 cleanup evidence |
| Lifecycle | `start()` before any commit; `stop()` disables commits and discards owned references; `is_running` truthful; executor reusable after a completed run before caller cleanup. | T02 (post-run reuse), T10 |

### 3.2 Rejection policy (rejected before side effects)

| Mode | Behavior | Evidence |
| --- | --- | --- |
| Chained action (nonempty `begin_point`) | `commit_action` → `False` | Recorded regression `CountSchedulerContractTests.test_serial_action_success_failure_and_unsupported_actions` (`tests/test_thermal_example.py`; green within the recorded 306-test full discovery): chained commit rejected on a **started** thermal scheduler |
| Threaded action (`to_thread=True`) | `commit_action` → `False` | Same recorded regression: threaded commit rejected on a **started** thermal scheduler |
| Commit when not started / after stop | `commit_action` → `False` (silent rejection, no raise) | F-03, T02 (commit/join against a never-started scheduler), T10 |
| Unknown or missing point in `wait_point` | return `False` — **not** `None`, not an exception | F-03, T10 |
| Re-commit to an already-used point | `commit_action` → `False` | Same recorded regression: second commit to the same point rejected on a started thermal scheduler |
| Synchronous invocation inside a running event loop | caller-side preflight error before acquisition (carried from `execute`) | T08 thermal fixtures |
| Borrowed/global scheduler as the owned executor | unsupported; never stop, drain or close resources the client did not create | T04, M02 plan |

**Isolation caveat:** the T02 fixture also asserts chained and threaded
rejections, but against a scheduler that was **never started**; those `False`
results do not isolate the chained/threaded rejection modes from not-started
rejection and are cited above only as not-started evidence. The isolating
evidence for chained, threaded and re-commit rejection is the recorded
regression cited in the table. W02 must additionally prove in its own suite
that each rejection occurs **before any action side effect**; the recorded
evidence establishes rejection outcomes, and no claim is made here that the
T02 fixture independently proves side-effect ordering.

Any widening (chains, threads, borrowed loops) requires separate
justification and tests; it is not implicit in this boundary.

### 3.3 Failure policy

- Action exception → `wait_point` returns `False`; `last_error` /
  `last_exception` retain the type and identity of the cause; no fabricated
  row. (T07)
- Partial results are truthful: completed rows and final boundary are recorded
  independently; boundary may exceed row count when a read fails after a
  successful advance; no rollback. (T07 thermal faults)
- Primary cause and cleanup failures are recorded independently; an
  obstructed cleanup step never hides the primary cause and never suppresses
  remaining cleanup attempts. (T09)
- Timeout of a bounded wait → `None`, distinct from failure (`False`) and
  unknown point (`False`). Note this seam **deliberately differs** from the
  broken generic `_CtrlPoint` timeout behavior (F-02): the seam keeps the
  thermal executor's documented three-valued outcome.

### 3.4 Cleanup invariants

- On every terminal path (completed, failed, cancelled): stop the owned
  scheduler, drain pending work on the owned loop, close the owned loop, and
  evidence each step (`work_drained`, `loop_closed`, `cleanup_errors`).
  (T09)
- Borrowed/global resources remain usable afterwards; the module-global
  singleton identity is unchanged. (T04)
- No cleanup is claimed for forcibly terminated processes (results-report
  limitation; unverifiable by design).

### 3.5 Stop/reset/repeat policy

Three distinct behaviors are characterized under the word "reset". They are
different contracts and must not be conflated:

1. **Legacy `Process.reset()` — unchanged public compatibility.** It restores
   process run eligibility; invoked while a body is running, it signals
   cooperative abort **without preemption** (T08). It has **no
   terminal/drained/closed precondition** and does not clear the record:
   legacy count/scan repeat via `run_process`/`Process.reset()` **appends**
   rows to the existing record (F-06; T03 reset/repeat-append; T10
   append-versus-clear). The seam must preserve this contract verbatim — no
   new guard, no record clearing, no behavior change.
2. **Thermal caller-owned post-run model reset (`reset_after_run`) — caller
   policy, not an executor contract.** This example helper requires
   terminal/drained/closed execution evidence (it raises otherwise), resets
   only the retained coordinator/model, and never rewrites the terminal
   snapshot. It is **not** a `Scheduler.reset()` API: it does not restore
   executor eligibility, and the executor itself carries no reset behavior.
3. **Repeat = fresh-run construction.** Repeat runs in the thermal client are
   achieved by constructing fresh executions — fresh group identities with
   deterministic traces excluding `timestamp`, after both success and
   interruption (T10) — not by resetting an executor.

Cooperative stop is orthogonal to all three: a request is not an
acknowledgment; acknowledgment occurs only at an acquisition boundary; a
request before the first cycle yields zero steps; a late request after
completion changes nothing. No preemption of running work is claimed. (T08)

**W02 obligation:** the seam defines **no scheduler reset API**. W02 must keep
legacy `Process.reset()`/abort and append-on-repeat semantics unchanged
(obligation from item 1). Post-run model reset remains caller-owned policy
(item 2 stays with the caller, as in the thermal example). Any scheduler-level
reset or further lifecycle refinement is **W03 scope**, not a W02 acceptance
behavior.

### 3.6 Baseline debt dispositions (AC06, unchanged)

- **R4-BASE-SCHED-01**: confirmed at runtime (F-01). The seam avoids the
  broken completion-routing path by owning dispatch; a generic singleton
  repair remains outside M02 scope.
- **R4-BASE-META-01**: characterization did not touch metadata
  construction/hydration; repair stays outside execution extraction.
- **R4-BASE-HDF5-01**: disposable-copy reads preserved (F-04 replay separated
  by record identity); storage repair separately scoped.
- **OBS-004/005/006, DEFECT-2**: untouched by the selected path; dispositions
  stand.
- Historical pandas/Actions warnings: not observed in the recorded
  environment; recorded as not observed, not waived.

## 4. Open items for W02

1. **Module placement and public name** of the executor within `huo` — W02
   detailed design, with Architect review. The `examples/` prototype is not a
   supported API and must not be imported by production code.
2. **Acquisition-loop extraction depth**: the demonstrated shared piece is the
   executor. Whether the bounded iteration loop itself (boundary check,
   stop-check ordering, row/boundary snapshot) becomes part of the seam or
   stays caller-owned is a W02 design question; both characterized clients
   currently own their loops, and the stop-check-before-commit ordering is
   part of the demonstrated semantics.
3. **Migration of the thermal example** to the seam is W04 scope; W02 proves
   the seam through the synthetic client and a bounded integration fixture
   without changing existing count/scan semantics (per the M02 plan).
4. **Python 3.9 verification** is pending; W02 candidate CI supplies that
   evidence. Nothing in this decision relies on 3.13-only behavior.
5. **F-02/F-01 remain open baseline debt** with recorded dispositions; W02
   must not depend on the generic `_SchedulerImpl` private-ownership path.
6. **W02 must carry all inseparable cleanup and truthful failure handling**
   for its supported scope (per AC05 and the M02 plan); W03 extends fault
   coverage and lifecycle refinement only.
7. This decision requires **Architect boundary review and Product Owner scope
   agreement** before W02 starts; independent review/testing, acceptance,
   exact-candidate CI, integration and promotion remain pending release gates
   (AC07, AC08).

## 5. Review

- **Architect boundary review**: PENDING
- **Product Owner scope agreement**: PENDING
