# M01-W02-001 implementation evidence

Owner: sw-tom (Developer). Date: 2026-10-22 Asia/Shanghai.
Preimplementation subject `7e7b85f`; approved plan/developer review d7036c4,
design 2bec5a3/Architect be26e7e, feature red tests b331c13.
Implementation `1ba603d`; guide `6a0e6d3`; both pushed before this handoff.
Status: **Developer self-checks PASS; independent review/testing pending**.

## Delivered scope and lifecycle

Changed only examples/thermal_experiment.py and its user guide. Example-local
StopRequest separates request/ack; RunResult preserves complete original rows,
actual Coordinator boundary, original exception and independent acquisition,
persistence, plotting and cleanup outcomes. run preserves successful reopened
DataGroup return and raises RuntimeError-compatible RunFailure with result/cause
on operational failure. Validation/collision still raise before acquisition.

Acquisition uses existing count/AtomJob with one reset then bounded commit/join,
checking completion before stop acknowledgment. A synchronous in-flight cycle
finishes; failure dominates a same-cycle request. Completed row callbacks run
once; cancel/failed acquisition never persists partials. Scheduler keeps existing
last_error and additionally retains original exception identity. Final tick is
snapshotted from Coordinator, not inferred from row count. Teardown never resets
or steps; independent stop/drain/close/backend attempts retain cleanup errors.
BaseException is reraised after cleanup. No reusable Process is exposed.

Explicit reset requires terminal acquisition/drained work/closed loop; Coordinator
reset exceptions propagate while saved terminal evidence remains unchanged.
Fresh repeats allocate fresh topology/process/record/stop/resources. CLI
--stop-after accepts 0 through N, exits unsuccessfully for cancellation and
announces final outputs only on full success. Guide includes executable partial
inspection/reset/two-repeat journey with temporary synthetic files.

No tests, public library, dependency, Python range, configuration or storage
format changed. No prerequisite library defect or test-fixture obstruction was
found. R4-BASE-SCHED-01/META-01/HDF5-01 and historical OBS/DEFECT debt remain open;
existing bounded compatibility measures and seams are retained.

## Executed Developer checks

Repository root; .venv/bin/python 3.13.15; MPLBACKEND=Agg,
MPLCONFIGDIR=/tmp/softlab-w02-mpl, XDG_CACHE_HOME=/tmp/softlab-w02-cache.
The same implementation bytes tested were committed; log/guide changes do not
alter those bytes. Commands/results:

- `-W error -m unittest discover -s tests -p 'test_thermal*.py'`: 38 PASS
  (13.662s), new 19 plus unchanged W01 19; no skips.
- `-W error -m unittest discover -s tests -p 'test_huo_execution_contracts.py'`:
  7 PASS (0.013s), no skips.
- `-W error -m unittest discover -s tests -p 'test_*.py'`: 255 PASS (15.260s),
  no skips; full suite includes all 45 focused contracts after final source cleanup.
- `-W error -m compileall -q softlab examples`: exit 0.
- `-W error -c 'import softlab; print(softlab.__version__)'`: exit 0, 0.3.0.
- Extracted and executed the guide's complete Python heredoc in this same
  environment: exit 0; asserted three-row/tick cancellation, acknowledgment,
  no persistence, explicit reset snapshot and two fresh successful numeric-equal
  repeats with distinct UUIDs. TemporaryDirectory removed all generated files.
- `git diff --check`: exit 0; no test/source/config changes outside named scope.

No Python warnings arose in named warning-as-error checks. HDF5 informational
connection messages remain normal output. Optional Black is not installed
(`python -m black --version` reports no module); it is not a required runtime or
configured CI dependency, and no formatting tool pass is claimed. No unconfigured
lint/static-analysis, local Python 3.9, physical stop or hardware claim is made.
Exact candidate 3.9/3.13 CI and Tester independent results remain later gates.
Developer does not approve own code or close Tester/reviewer findings.

Handoff: Designer independent review on committed implementation/guide; board
moves to Code Review. This evidence/board update commits and pushes immediately
with docs(log), with final hash/upstream cleanliness reported separately.
