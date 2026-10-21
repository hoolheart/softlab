# M01-W02-001 execution characterization

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Subject: d7b73a6 on codex/m01-w02-controlled-execution, from dev e0b5a135.
Verdict: **PASS for legacy characterization/baseline only**. No W02 feature,
design, code-review, integration or release-acceptance gate is certified.

## Source/callers and committed assertions

Read Process/run_process, SimpleProcess/Wrapper, SweepProcess, AtomJob/Counter,
Scheduler and impl_scheduler, the thermal example's private executor/acquisition/
run cleanup, existing compatibility and thermal tests, Coordinator reset/failure
assertions, pyproject configuration and approved setup envelope. Relevant callers
include count/scan, run_process users in tests/test_compatibility.py and the
thermal example. Seven new tests in tests/test_huo_execution_contracts.py assert:

| Observed contract | Evidence and bounded consequence |
| --- | --- |
| run_process resets before running; success returns (bool, elapsed seconds); caller retains scheduler ownership | Same SimpleProcessWrapper runs twice, executes exactly once each run, cannot commit again when finished, no pending work. |
| SimpleProcess.reset while running only raises cooperative aborting flag | Body continues through an await after reset; join succeeds, finally clears running/aborting, has_more remains true. Reset does not prove body stopped. |
| Failed body does not imply successful completion or automatic retry | Private W01 executor returns False and diagnostic; one attempt, process not pending but has_more true. |
| Sweep reset preserves its DataRecord | Same count process runs twice, appending 2 then 4 rows; before/get/after order repeats. Fresh repeat needs a fresh record/process. |
| Legacy control-point status is timing-dependent on failure | Already-failed point reports False; waiter pending before failure receives original ValueError. Test observes both paths. |
| Legacy cancelled control-point status raises | CancelledError from status and wait despite documented False status description; assertion preserves current behavior without repairing it. |
| Scheduler.stop requests cancellation but does not drain or close loop | Captured task remains pending immediately after stop; explicit gather runs its finally and confirms cancellation. Registry clears, stopped flag false, borrowed running loop stays open; restart has empty points. Default singleton is unchanged. |

Tests instantiate a private legacy scheduler only inside an explicitly owned test
loop, so constructor selects the running loop without modifying event-loop policy.
They drain cancellation explicitly, close owned test loops, capture expected
legacy logger warnings with assertLogs and never install a replacement singleton.
The bounded W01 executor is reused for process tests, preserving tracked global
callback defect R4-BASE-SCHED-01 rather than silently repairing or bypassing its
scope disposition. Private imports are intentional compatibility characterization,
not a new supported public API.

Existing test_thermal_example.py retains 19 observable W01 tests including the
eight-row independent numerical oracle/schema/reopen/PNG, collision protection,
model/sample failure without retry, source-file integrity, backend/copy failures,
plot diagnostics, owned/borrowed loop/global/figure cleanup and CLI behavior.
Existing test_tu_coordination.py asserts failed tick publication, successful reset
and reset factory atomicity. Their full-suite execution below supplies baseline
evidence; W02 partial-result/request/acknowledgment assertions are future red tests
under the approved design and [test plan](M01-W02-001-plan.md).

## Actual execution and environment

All commands run from repository root using .venv/bin/python, Python 3.13.15
(conda-forge, Clang 19.1.7). Libraries: NumPy 2.5.3, SciPy 1.18.1,
pandas 3.0.6, Matplotlib 3.11.2, h5py 3.16.0, PyVISA 1.16.2,
pyvisa-sim 0.7.1. Import smoke version: softlab 0.3.0.

- Focused `-m unittest discover -s tests -p test_huo_execution_contracts.py -v`:
  7 tests PASS (0.013s), no skips.
- Full `-m unittest discover -s tests -p 'test_*.py'`: 236 tests PASS (8.428s),
  no skips, no Python warning reported.
- `-m compileall -q softlab`: exit 0; import/version smoke exit 0.
- `-W error -m unittest discover -s tests -p 'test_*.py'`: 236 tests PASS
  (8.355s), no skips or Python warnings. Same unchanged source bytes were
  committed as d7b73a6.
- Replayed focused tests against committed d7b73a6 with `-W error`: 7 tests
  PASS (0.013s); warning-as-error compileall and import smoke exit 0.
- `git diff --check`: exit 0; changes are test source and release_5 records only.

Writable /tmp/softlab-w02-mpl and /tmp/softlab-w02-cache caches with
MPLBACKEND=Agg were used for successful checks. Initial unconfigured-cache run
reported unwritable /Users/edward/.matplotlib plus Fontconfig cache diagnostics;
no dependency was missing. Initial count fixture incorrectly passed a single
Parameter to getters instead of a sequence, producing one test error; Tester
corrected its fixture to [reading] and closed that test-authoring failure after
successful focused/full reruns. No production behavior changed to fix the test.
Cache initialization's informational font-cache message is not a Python warning;
named warning checks do not certify general system/lint warnings absent.

## Baseline debt and limits

No runtime library defect is fixed or closed: R4-BASE-SCHED-01,
R4-BASE-META-01, R4-BASE-HDF5-01, OBS-004/005/006/DEFECT-2 remain tracked. The
cancelled-point exception and stop/drain distinction above are legacy limitations
for design to respect, not newly introduced regressions. Existing baseline code
with added characterization passes. No required library change follows from this
evidence; any new prerequisite needs Architect/PO disposition.
Only local Python 3.13 was executed; configured candidate Python 3.9/3.13 CI is a
later integration gate. No real devices, durable resume, partial persistence,
concurrent reset or physical stop were tested. Feature test plan/developer review,
detailed design/Architect review, feature red tests and implementation are pending.
