# M01-W02-001 independent code review — iteration 1

Reviewer: sw-celeste (Designer). Date: 2026-10-22 Asia/Shanghai.
Subject: committed `639860f` (implementation `1ba603d`, guide `6a0e6d3`)
on codex/m01-w02-controlled-execution. Verdict: **CHANGES_REQUESTED**.

Reviewed examples/thermal_experiment.py, guide, W01 caller assertions,
tests/test_thermal_execution.py and legacy characterization against the approved
[design](../design/M01-W02-001-design.md), [PRD](../prd.md),
[plan](../test/M01-W02-001-plan.md) and Architect approval. Static source and
diff inspection only; Reviewer executed no tests and changed no production/test
source. Developer's 255 green self-checks are reported evidence, not independent
Tester certification.

## Open issue

### CR-W02-01 — P2 — Preserve structured helper cleanup evidence

Status: **OPEN**. Locations: examples/thermal_experiment.py `_reload_group`
lines 429–445, `_plot_record` lines 479–493, and `execute` exception/result
handling. The new RunResult promises independent cleanup_errors and original
cause, but these helpers flatten their cleanup obstructions into a generic
RuntimeError. execute assigns that wrapper to result.cause while its local
errors list remains empty. For example, successful plot generation followed by
plt.close failure yields plotting=failed and cleanup_errors=[], even though the
owned figure was not released; failed savefig plus failed figure close similarly
hides the primary error identity behind a combined string. Reader disconnect or
scratch removal obstructions have the same classification gap. Overall failure
is visible in prose, but callers using the new cleanup field cannot distinguish
these resource obstructions from ordinary persistence/plot faults.

Required correction: carry helper cleanup diagnostics structurally into the
owning execute RunResult, while retaining existing direct helper RuntimeError
compatibility and useful combined text. Preserve the original primary operation
exception in result.cause when one exists, alongside ordered cleanup_errors;
cleanup-only failure must also populate cleanup_errors and remain unsuccessful.
Use a bounded example-local carrier or equivalent explicit mechanism, not parsing
messages or modifying library backends. Continue independent cleanup attempts and
borrowed-resource protection. No change to the W01 copy-only reader, backend
format or library debt disposition is requested.

Verification required from Tester: inject real figure-close and reader/scratch
cleanup obstructions beneath their helpers, both alone and alongside a primary
operation failure; assert nonempty structured cleanup_errors, preserved primary
cause where present, unsuccessful overall result, correct acquisition/storage
classification, and remaining independent cleanup attempts. Existing feature
tests currently cover scheduler-stop obstruction, and W01 helper diagnostics
cover text, but do not assert this new result contract for nested cleanup faults.
Tester owns any test additions/results/failure closure; Developer owns the code
fix. Only this Reviewer may close CR-W02-01 after inspecting the committed fix.

## Other reviewed behavior

The local commit/join loop preserves real count/getter ordering, completion-before-
late-stop priority, in-flight request handling and exception-before-stop priority.
Early stop retains an empty group; sample/model failures retain separate row/tick
accounting without retry or teardown step/reset. Default run success remains a
verified reopened DataGroup; partials skip persistence/plot. Reset guards terminal,
drained and closed work and preserves terminal snapshots; fresh repeats allocate
fresh topology/records/stop/resources. No public library/dependency/version change
was found. Guide commands match the selected local surfaces and explain software
limits, no resume and existing backend debt. No other actionable issues raised.

R4-BASE and historical OBS/DEFECT debt stays open. This review neither reopens
accepted W01 nor approves integration/release acceptance. Remain in Code Review;
Developer fix and Reviewer closure precede Tester verification, principles and
exact-candidate CI. Maximum three review/fix iterations applies. This report and
board update require immediate docs(log) commit/push before handoff.
