# M02-W01 characterization test plan

Owner: Tester (sw-mike role) | 2026-10-30 Asia/Shanghai

Status: **PREDEVELOPMENT — Developer implementability review approved**.
This is a written plan based on source and existing test inspection; no new
runtime tests have been written or run. All results and acceptance gates below
remain pending. Existing M01 tests are useful inputs, not fresh W01 evidence.
The user-requested pause prohibits executable tests, production changes and new
runtime experiments until resumed.

## Execution protocol after resume

Use the unchanged production baseline and record its Git revision, working-tree
status, Python and dependency versions, operating system, commands, exit codes,
skips and actual warnings. Use synthetic parameters, a temporary directory, no
instrument and no new dependency. Characterization may add tests and fixtures,
but must not alter production behavior to make them pass. Review assertions
against observed baseline behavior: record defects and unexpected outcomes as
findings, not desirable future guarantees. A test cannot be marked passed merely
because its plan predicts a result. Bound asynchronous cases with timeouts and
release test-owned actions, schedulers and event loops in `finally`; preserve and
verify the singleton and any borrowed loop. Never run simultaneous cases that
mutate singleton state.

For T04, execute each real singleton or concrete private scheduler completion
probe in its own child process. The parent enforces a subprocess timeout because
legacy `run_process` and `Process.join` call `wait_point` without a timeout.
The child uses `finally` to drain and close only resources it owns; on timeout,
the parent terminates and reaps the child and reports the timeout as an observed
failure, not a pass; correlate callback and point traces with R4-BASE-SCHED-01
before attributing its cause. Forced termination may prevent the
child's `finally` from running: process isolation contains the impact, but child
cleanup is then unverifiable. Normal cooperative paths still execute `finally`.
Do not patch completion callbacks to
route through the singleton and then claim the unmodified behavior passed. Use
the thermal serial scheduler for T02 and T03; T08's `_CtrlPoint` cancellation
is a private characterization seam, not a public `Scheduler.cancel` API.

Use event traces written by independent synthetic setters, getters, hooks and
record wrappers. Explicit numeric expected values and predeclared trace lists
are the oracle; do not derive expected outcomes from the implementation under
test. Ignore wall timestamps when comparing deterministic values. Record whether
a call raises, returns false, records diagnostics, or completes; do not assume
these outcomes are interchangeable. Use disposable copies if existing HDF5
loading is involved. No physical safety or cancellation guarantee is inferred.

## Planned assertions

| ID / AC | Preconditions and stimulus | Oracle and evidence |
| --- | --- | --- |
| T01 / 01, 02 | Inventory callers using repository search for `commit`, `join`, `reset`, `run_process`, `count`, `scan`, and scheduler entry points; classify thermal and integration callers. | Document file/function, owned or borrowed loop/scheduler/action/callback/record and supported paths; distinguish direct source facts, runtime observations and hypotheses. Review completeness against search output. |
| T02 / 02, 05 | Count two synthetic readings with explicit before/after hooks and a supplied record; exercise public imports and `run_process` with a caller-owned serial scheduler. | Assert ordered hook/get events, exactly two values and rows, column order, `(bool, elapsed)` shape, completion, and scheduler still usable before caller cleanup. Assert legacy invalid count/getter inputs and commit/join transitions against the observed baseline. |
| T03 / 02, 05 | Scan a synthetic setter over two known values, with getter and four hooks; use existing public `scan` and record. | Assert set and get values, order of hook/set/get events and rows against independently declared trace; assert reset/repeat behavior and existing import paths. Document any extra terminal dry-run behavior separately. |
| T04 / 01, 02, 06 | Compare default singleton with thermal private serial scheduler in isolated, sequential fixtures; exercise start, point/action, completion, stop and loop ownership, with bounded waits. | Assert identity and lifecycle effects on each resource and whether unrelated borrowed state remains usable. Capture the singleton completion-callback issue as R4-BASE-SCHED-01 if observed; do not assert a generic private scheduler is safe without evidence. |
| T05 / 02, 03 | Use deterministic thermal configuration for coordinated and direct adapters. Instrument advance, measurement observations and row append with spies that retain the original effects. | Assert declared command/advance/read/row order, one advance per completed row, previous-boundary versus current-boundary source ticks, and independent numeric traces. Count observations to expose direct adapter's additional pure callback. Compare existing v1/v2 behavior without asserting stateful repeated observation is safe. |
| T06 / 03, 04 | Build a distinct synthetic signal-monitor client: configure fixed source once, advance a synthetic clock, read consumptive signal and reference once each, compute their difference and commit only after both succeed. Use a separate counting double to expose reads. | For three boundaries, assert the predetermined complete event sequence, three paired readings and differences, exact read counts and no thermal heater policy. Compare client-owned acquisition/derived-value work and repeated lifecycle orchestration with T05; record concrete reuse and non-shared work, including amount and ownership. A double alone is insufficient evidence of a second client. |
| T07 / 02, 03, 05 | Inject first-read and second-read exceptions independently in the monitoring client, and thermal advance/read exceptions before and after a boundary. | Assert actual error/diagnostic path, last observed boundary, committed rows and trace; a failed second read must not produce a complete row in the client. Detect any changed consumptive-source state and do not assert rollback or equate boundary with row count. Document legacy behavior separately from future contract. |
| T08 / 02, 05 | Request thermal stop before first cycle, after a known row, and after completion; separately characterize reset of a running legacy process and cancellation of a scheduler point. | Assert request and acknowledgment separately where thermal exposes both, committed rows and no further thermal cycle after acknowledgment; late request does not rewrite completed outcome. For legacy operations assert only observed return, state and exception; distinguish request from preemption and physical termination. |
| T09 / 02, 05 | Fault scheduling, acquisition and independent cleanup operations, including simultaneous primary and cleanup failures in thermal execution. | Assert observed primary cause and cleanup diagnostics independently, partial rows and last boundary, scheduler stopped and owned loop closed when possible; capture cleanup obstruction rather than fabricating success. Assert borrowed/global resources remain usable. A failure in one cleanup should not obscure observed other cleanup actions. |
| T10 / 02, 05 | Exercise invalid commit/join/reset transitions, reset during active work, reset after terminal state, failed reset, and two fresh deterministic runs after success and supported interruption/failure. | Assert observed status, diagnostics and reset eligibility; preserve any terminal snapshot; compare numeric traces excluding UUID/time and verify fresh identities where exposed. Record legacy append-versus-clear behavior without prescribing it as the new design. No implicit retry or rollback is presumed. |
| T11 / 04, 05, 06 | Compare T01–T10 evidence for existing helper reuse, a minimal additive seam and retained local execution. Inspect each PRD debt item for actual relevance. | Decision record cites concrete traces/ownership and tests, supported/rejected modes, cleanup and failure obligations and tracked debt. Product Owner and Architect revise scope if useful reuse remains unproven. W02 must carry inseparable safety; this plan does not approve an API. |
| T12 / 07, 08 | Tester records results and closes test failures; independent reviewer closes review findings. Product Owner maps acceptance to evidence; coordinator verifies frozen candidate CI, promotion and required promoted checks. | Link actual results, environment, outstanding issues and CI/revisions in one closure record. Do not claim pass for unrun checks or promote before required gates. Clean worktree and acceptance are future release conditions. |

## Planned commands and reporting

After authorization to resume, from the repository root and an environment with
the project installed, use the following; the first test filename is reserved
for the new suite and must be created during characterization. Capture stdout,
stderr and exit status, including warnings, rather than suppressing them.

```sh
python --version
python -m pip list
git status --short
git rev-parse HEAD
python -m unittest discover -s tests -p 'test_m02_w01_characterization.py'
python -m unittest discover -s tests -p 'test_huo_execution_contracts.py'
python -m unittest discover -s tests -p 'test_thermal_execution.py'
python -m unittest discover -s tests -p 'test_thermal_adapters.py'
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q softlab
python -c "import softlab; print(softlab.__version__)"
git diff --check
```

Selectively rerun affected checks after changes or failures; full discovery,
compile and import are required for Python changes. CI's Python 3.9 and 3.13
matrix counts only when an actual candidate run succeeds. Historical pandas and
Actions warnings are not fresh observations. Report each test case, actual
outcome and artifacts, including skipped/blocked cases and applicable existing
debt, with links to the exact revision. No CI, acceptance or release closure is
claimed by this plan.

## Acceptance traceability and review request

AC01: T01, T04; AC02: T02–T04, T07–T10; AC03: T05–T07;
AC04: T06, T11; AC05: T02–T03, T07–T11; AC06: T04, T11;
AC07: T12 and test/review ownership; AC08: T12 and candidate CI/promotion.
The independent [Developer review](../reviews/test-plan-review.md) approved
implementability and closed DR-TP-01 after revision. This approval concerns the
written plan only; execution and release acceptance remain pending.
