# M01-W01-001 independent code review — iteration 1

Reviewer: sw-celeste (Designer), 2026-10-18. Candidate `982c091`;
implementation `6f8b26d`, guide `864891a`. Verdict: **CHANGES_REQUESTED**.
Static inspection only; no tests executed. Developer self-check results in
implementation.md remain Developer evidence, not independent Tester closure.

## Scope and positive evidence

Read the complete example, guide, acceptance tests, approved design/review,
activated PRD and existing count/record/backend contracts. The example uses a
fresh Station/body/control/measurement graph, count's before-get hook and plain
Parameters. Default previous-boundary delivery, exact step/row checks, finite
validation, alphabetic schema and string metadata match the approved design.
The bounded public Scheduler extension does not access the global scheduler or
set the thread loop. Its serial Action dispatch is appropriately local.

Writer release precedes copy-only legacy loading. Required metadata is read
from that copy before destructive load, never repaired from original values;
identity/schema/value comparison and returned delivered backend path are correct.
Successive-copy tests preserve source digest/reserved attrs. Plot uses the
reopened record and closes new figures even after partial creation. No library
source/export/dependency/Python-range change appears in the candidate diff.
Tracked PNG inspection finds only preexisting softlab/logo.png, no generated
thermal HDF5/PNG. Guide accurately discloses legacy defects and W01 limitations.

## Required finding (Reviewer closure only)

| ID | Priority | Evidence and impact | Required correction | State |
| --- | --- | --- | --- | --- |
| CR-01 | P2 | examples/thermal_experiment.py:448–457 puts task enumeration/cancellation/draining and owned-loop close in one try. An exception from an earlier cleanup operation (for example run_until_complete while draining owned tasks) jumps directly to the except and never attempts loop.close. Thus one cleanup failure can skip a separately owned resource-release operation, contrary to AC10 and the approved independent-cleanup contract. Existing tests only obstruct scheduler.stop, which is in a different try and does not exercise this path. | Keep drain diagnostics, but independently attempt owned loop close even when enumeration/cancel/drain raises; preserve primary plus both cleanup errors and continue writer cleanup. Do not close a running/borrowed loop or add cancellation features. Tester must add a focused injected drain-error assertion proving close is attempted and command stays unsuccessful, plus close-error evidence showing other cleanup still executes. | OPEN |

No other actionable production-code finding identified in this iteration.
This is not an instruction to expand the scheduler or fix baseline library debt.
Developer owns the small cleanup correction; Tester owns executable assertions
and failure closure. Reviewer alone closes CR-01 after inspecting committed fixes.
No implementation/test/CI/principle gate is passed by this review.
