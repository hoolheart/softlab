# M01-W02 release acceptance

Product Owner: sw-camille. Date: 2026-10-22 Asia/Shanghai.
Verdict: **PASS — bounded M01-W02 product accepted**.
Accepted runtime/guide: integrated dev `10b2d7d3ff08f28488f0bcfded009a02de293ab8`.
Closure evidence reviewed at `4f6de05` on codex/m01-w02-release-closure;
runtime/test/guide/config bytes are unchanged from the independently tested
Designer-approved subject `2095c08` and integrated dev.

## Product outcome and method

The [activated PRD](prd.md) promised controlled interruption and truthful partial
inspection of the existing synthetic thermal experiment. The delivered
[guide](../../docs/user-guide/thermal-experiment.md) gives a normal headless
command, deterministic stop command, in-memory partial inspection, deliberate
post-terminal reset and two fresh repeats. Request and acknowledgment have
different meanings; the retained row count and committed simulation boundary
correctly distinguish model failure from sampling failure. Acquisition,
persistence, plotting and cleanup results remain separately inspectable.

PO reviewed requirements, guide, implementation evidence, approved detailed
design/Architect review, independent code review and issue closure, owned test
results, actual architecture, release principles and integration records.
PO did not implement, design, execute tests or close technical findings.
Runtime conclusions rely on [independent results](test/M01-W02-001-results.md)
and [final closure validation](test/M01-W02-CLOSURE-validation.md), including
actual 257-test warning-as-error runs, compile/import and independently executed
guide journeys. Historical phase-pending prose is superseded only by the later
owned evidence linked here, not by invented retroactive passes.

## Criterion-by-criterion acceptance

| Criterion | Verdict | Accepted observable evidence |
| --- | --- | --- |
| AC-01 | PASS | W01 19 tests preserve eight-row delayed Decimal oracle, schema/units, fresh identity, verified HDF5/PNG and successful run return; full compatibility suite passes. |
| AC-02 | PASS | Seven preimplementation legacy characterization tests establish cancellation/status, reset versus termination, retained rows on reuse and explicit drain/borrowed-loop ownership. |
| AC-03 | PASS | Separate idempotent StopRequest acknowledgment and real in-flight evolve/getter requests tested; guide explains current-cycle completion and acknowledgment before another cycle. |
| AC-04 | PASS | Zero-tick early stop, exact three-row/three-tick cancellation, independent no-post-ack step observation through cleanup and eight-row late-request completion priority pass. |
| AC-05 | PASS | Injected real model evolution failure retains three rows/tick three and original cause; no fabricated row, retry, partial save/plot or teardown tick. Failure wins over same-cycle stop. |
| AC-06 | PASS | Injected sample failure retains three rows/tick four and original cause; no partial row/rollback/retry, and request cannot erase failure. |
| AC-07 | PASS | Save/reload failure leaves acquisition completed/persistence failed; plotting faults preserve separately verified storage. Nested-helper primary identity and structured cleanup errors are asserted. |
| AC-08 | PASS | Empty/nonempty partial groups remain inspectable via execute or RunFailure.result; cancelled/failed acquisition has not_attempted storage/plot and no artifacts; CLI fails visibly. |
| AC-09 | PASS | Reset requires terminal, drained, closed execution; live/undrained/unclosed guards reject. Successful reset restores initial model boundary without acquisition; injected factory failure preserves prior committed state and terminal snapshot. |
| AC-10 | PASS | Two fresh runs after cancellation/model/sample/reset and reset-failure journeys have equal finite numeric traces, different UUIDs, N rows and reopenable fresh outputs. |
| AC-11 | PASS | Scheduler/task/loop/backend/scratch/figure cleanup, nested fault matrix, independent cleanup attempts and borrowed-state preservation pass; cleanup obstruction prevents overall success and remains structured beside the primary cause. No cleanup reset/extra tick. |
| AC-12 | PASS | Tester independently ran normal/early/mid/late CLI, stored inspection and the guide's verbatim partial/reset/two-repeat snippet with temporary synthetic outputs. Product inspection confirms timing/outcomes/no-resume/resource limits are explained. |

## Independent gates and remaining integration checks

[Code review](reviews/M01-W02-001-code-review.md) is APPROVED in iteration 2;
sw-celeste closed CR-W02-01. Tester closed feature/red and nested cleanup failures
through actual green execution; PO preserves those owners' closure authority.
[Closure document review](reviews/M01-W02-CLOSURE-review.md) is APPROVED with no
findings. Actual [architecture](../../arch.md) and
[release architecture](release-architecture.md) describe delivered W02 and retain
W03/W04 as future scope. Final local 257 tests under `-W error`, compile/import,
links/whitespace/scope checks pass with no skips or Python warning diagnostics.

[Integration](integration.md) records exact implementation candidate `fa553f2`
[CI 39693855365](https://github.com/hoolheart/softlab/actions/runs/39693855365)
passing both Python 3.9/3.13 before dev merge `8a0cf54`. Reconciled dev
`10b2d7d` [CI 39694251153](https://github.com/hoolheart/softlab/actions/runs/39694251153)
also explicitly passed both versions. These are implementation-state checks,
not CI certification for this new acceptance/closure commit.

Coordinator supplied the formal all-eleven-principle release-readiness PASS for
clean HEAD/upstream `4f6de05`; the delegated record is in
[principle inspection](principle_compliance_report.md). This product PASS accepts
the delivered runtime and documentation. Exact final closure candidate CI,
closure dev reconciliation, target-state checks and main promotion remain
required integration actions and are not asserted executed here. Reopen affected
acceptance if runtime, requirements or guide scope changes.

## Debt disposition and limits

Accept the tested example-local measures while retaining unfixed library debt:
R4-BASE-SCHED-01 uses the private serial executor with characterized legacy
limits; R4-BASE-META-01 uses explicit text hydration; R4-BASE-HDF5-01 uses
copy-only loading with source-integrity checks. OBS-004/005/006 and DEFECT-2
remain historical out-of-scope debt under their recorded disposition. No broad
repair, silent waiver or debt closure is implied. CI Node20/forced24 and Ubuntu26
platform notices remain disclosed; named warning-as-error checks do not certify
unconfigured linters, all platforms or a warning-free external CI service.

Cancellation is cooperative software behavior, not blocking-call preemption or
physical stop. Partials remain in memory and are not persisted/resumed; reset is
deliberate post-terminal model work, not cleanup or general Process reuse. No
library/dependency/Python-support/storage-format change, public run framework,
hardware/UI, W03 manifest, W04 adapters, whole-M01 acceptance, package version/tag
or publication is accepted by this weekly verdict.
