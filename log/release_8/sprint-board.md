# Sprint board — M02-W01 (process release_8)

Updated: 2026-10-30 Asia/Shanghai. Integration branch: `dev`.
Current task branch: `codex/m02-w01-preparation`.
Release status: **OPEN — RESUMED**, on the user's explicit instruction to
continue and finish release 8. Preparation was committed/pushed at `1935fab`;
the remaining approved sequence is characterization → evidence decision →
independent checks → closure per [tasks](tasks.md).

| ID | Scope | Owner / next handoff | Phase | Started | Expected completion | Blockers / remaining gates | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M02-W01-001 | Characterize existing count execution and decide minimal justified seam | Tester (sw-mike) / Developer implementability review | Characterization after resume | 2026-10-30 | Release date unset | Executable characterization, evidence decision, independent review/testing, acceptance, CI/integration/promotion all pending | None |

Exactly one task is active. W02 implementation has not started. M01 is
[CLOSED](../release_7/closure.md); the M02 milestone planning reviews approve
planning only and do not pass any W01 runtime gate.

## Current evidence and handoff

The [PRD](prd.md) and [task phases](tasks.md) define the approved preparation
scope. [Architect requirements review](architecture-review.md),
[Developer test-plan review](reviews/test-plan-review.md) and
[Architect design review](reviews/design-review.md) approve source/document
preparation. DR-TP-01 and AR-DD-01 were closed by their respective reviewers.
No executable test, runtime result, code review, extraction decision, release
acceptance or CI outcome is claimed by this board.

The task is now **PAUSED BEFORE DEVELOPMENT** at the approved test-plan/design
boundary; see [handoff](predevelopment-handoff.md). Coordinator records actual
document validation and commit/push facts in the final handoff. Resume only on
user instruction. This pause keeps W01 OPEN; it does not integrate a completed
release or authorize W02.

Existing R4-BASE-SCHED-01/META-01/HDF5-01, OBS-004/005/006, DEFECT-2 and
historical pandas/Actions notices retain their PRD dispositions. Their current
runtime impact is not yet established. No hardware, UI, dependency, storage
repair, global scheduler repair or package publication is in scope.
