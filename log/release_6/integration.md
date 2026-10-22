# M01-W03-001 task integration

Integrator: sw-tom. 2026-10-22 Asia/Shanghai.
State: **BLOCKED: exact candidate Python3.9 CI failure; no dev merge**.

Coordinator all11 readiness PASS atffbf85c follows Reviewer69045ec approval and
CR-W03-01 closure plus independent Tester273 full/13 focused warning-as-error,
compile/import and guide evidence, with all scoped Tester failures closed.
Baseline debt remains open with bounded measures; UI N/A. This is delegated
verdict recording, not Developer issue closure/review/test rerun. Exact final
candidate both Python3.9/3.13 success is required before dev integration; completion
records then pass candidate CI/reconciliation. Actual architecture, release
principles and PO acceptance remain closure gates. No main promotion, branch
deletion or source/test/config changes by integrator.

## Exact candidate failure — integration stopped

Candidatecbb72fbe368dfee5cc2c9321ff046b7bbea729b5:
[CI39786554366](https://github.com/hoolheart/softlab/actions/runs/39786554366)
completed/failure. Refreshed jobs API: Python3.9 completed/failure;
Python3.13 completed/success. gh run watch --exit-status exited1. Dev remains
6d332ce3754077ce3ab468586ba157690328c222; no merge/main change occurred.

Python3.9.25 resolved Matplotlib3.9.4/pyparsing3.3.3. Regression executed273 tests
with two failures (16.584s): absent-legacy subprocess and complete typed
fresh-process inspection. Both explicit -W error subprocesses fail during
Matplotlib import: PyparsingDeprecationWarning, 'oneOf' deprecated - use 'one_of'.
The CLI never reaches inspection in those subprocesses. Parent CI also prints
existing pandas empty/all-NA concat FutureWarnings. Runner Node20 forced24 and
Ubuntu26 migration notices remain visible. No warning-free CI claim is made.

Integrator reported concrete logs/environment to Coordinator for Tester/Architect
scope disposition. No production/test/dependency/config repair or failure closure
is performed here. Local273 green results remain actual local3.13 evidence and
cannot replace failed3.9 CI. Candidate integration stays blocked pending reviewed
resolution and successful exact replacement-candidate CI on both versions.
