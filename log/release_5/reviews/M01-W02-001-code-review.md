# M01-W02-001 independent code review — iteration 1

Reviewer: sw-celeste (Designer). Date: 2026-10-22 Asia/Shanghai.
Subject: committed `639860f` (implementation `1ba603d`, guide `6a0e6d3`)
on codex/m01-w02-controlled-execution. Initial verdict: **CHANGES_REQUESTED**; superseded by iteration 2 below.

Reviewed examples/thermal_experiment.py, guide, W01 caller assertions,
tests/test_thermal_execution.py and legacy characterization against the approved
[design](../design/M01-W02-001-design.md), [PRD](../prd.md),
[plan](../test/M01-W02-001-plan.md) and Architect approval. Static source and
diff inspection only; Reviewer executed no tests and changed no production/test
source. Developer's 255 green self-checks are reported evidence, not independent
Tester certification.

## Open issue

### CR-W02-01 — P2 — Preserve structured helper cleanup evidence

Status: **CLOSED by sw-celeste in iteration 2**. Original locations: examples/thermal_experiment.py `_reload_group`
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


## Iteration 2 — APPROVED

Reviewer: sw-celeste. Date: 2026-10-22 Asia/Shanghai.
Subject: `8af418d`, including Developer repair `27d8a17` and Tester regression
source `9cfd570`/red evidence `6061ebe`. Verdict: **APPROVED**.

CR-W02-01 is **CLOSED by this Reviewer** based on committed source inspection:
_HelperFailure subclasses RuntimeError, copies ordered helper cleanup diagnostics
and retains the original primary exception object. Reader/scratch and figure
helpers raise that carrier with their original cause chain, preserving direct
helper compatibility and combined human-readable diagnostics. execute catches
it separately, appends cleanup diagnostics to the shared ordered errors list and
assigns the original primary to result.cause. Later scheduler/backend cleanup
attempts append to the same list independently. Cleanup-only failures have no
invented primary exception, but nonempty cleanup_errors and unsuccessful stage
status keep overall execution unsuccessful. Reader cleanup continues to fail
persistence before verification; plot cleanup preserves already verified storage.
No message parsing or backend/library behavior change was introduced.

Inspected regression source for real figure-close, reader-disconnect and scratch
removal obstructions alone and with primary operation failures. Assertions check
structured diagnostics, primary identity, stage accounting, owned-resource
fallback/remaining cleanup and borrowed figures. This review does not execute
those tests or close Tester-owned failures. Developer-reported 40 focused/257 full
green self-checks remain self-check evidence until independent Tester execution.

Rechecked the scoped repair against the original review/design: stop acknowledgment,
row/tick precedence, partial retention, default run compatibility, explicit reset
resource guards and fresh repeats are unaffected. No additional actionable issues
found. All scoped code-review issues are now closed; independent Tester gate,
principle inspection, exact candidate CI, integration and PO release acceptance
remain pending. Board advances only to Testing. No production/test source was
changed and no tests executed by Reviewer. This closure and board handoff require
immediate docs(log) commit/push.
