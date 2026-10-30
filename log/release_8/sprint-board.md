# Sprint board — M02-W01 (process release_8)

Updated: 2026-10-30 Asia/Shanghai (AR-ED-01…03 closed by Designer). Integration
branch: `dev`. Current task branch: `codex/m02-w01-preparation`.
Release status: **OPEN — CLOSURE (MINIMAL GATES, USER-APPROVED)**. The user chose
Option A (finish this release now) with explicitly cut checks. Preparation was
pushed at `1935fab`; characterization at `d300c7e`; decision + reviews + scope
agreement through `b47fb3d`. Remaining minimal finish line: AC01 caller-inventory
check → Product acceptance → principle inspection → frozen exact-candidate CI →
`dev` merge → `main` promotion and verification → clean worktree → one closure
summary → CLOSED. Cut by user authorization: the independent full review round
of the characterization suite (zero production-code changes in this release;
suite developer-reviewed at plan level; full 306-test regression evidence
reusable). Applicability and this scope reduction are recorded in the closure
summary and principle compliance report.

| ID | Scope | Owner / next handoff | Phase | Started | Expected completion | Blockers / remaining gates | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M02-W01-001 | Characterize existing count execution and decide minimal justified seam | Coordinator / closure sequence per minimal finish line | Closure — minimal gates | 2026-10-30 | 2026-10-30 | Acceptance, exact-candidate CI, `dev`/`main` integration and promotion, closure summary pending; W02 unauthorized | None |

Exactly one task is active. W02 implementation has not started. M01 is
[CLOSED](../release_7/closure.md); the M02 milestone planning reviews approve
planning only and do not pass any W01 runtime gate.

## Current evidence and handoff

The [PRD](prd.md) and [task phases](tasks.md) define the approved preparation
scope. [Architect requirements review](architecture-review.md),
[Developer test-plan review](reviews/test-plan-review.md) and
[Architect design review](reviews/design-review.md) approve source/document
preparation. DR-TP-01 and AR-DD-01 were closed by their respective reviewers.

Executable characterization has now run against the unchanged baseline
(revision `35e6e9b`, Python 3.13.15): the new suite
`tests/test_m02_w01_characterization.py` (25 tests) passes, the full
discovery run passes 306 tests with no observed warnings, and
`compileall`/import smoke checks succeed; see
[characterization results](test/characterization-results.md) for commands,
per-assertion traceability to W01-AC01–AC08, baseline findings
(R4-BASE-SCHED-01 confirmed at runtime, timeout/`CancelledError` behavior,
persistence replay of `add_rows`, direct-adapter extra observation,
legacy append-on-repeat) and explicit limitations. No code review,
extraction decision, release acceptance or CI outcome is claimed by this
board; the evidence decision (T11) is the next phase.

The task is now **PAUSED BEFORE DEVELOPMENT** at the approved test-plan/design
boundary; see [handoff](predevelopment-handoff.md). Coordinator records actual
document validation and commit/push facts in the final handoff. Resume only on
user instruction. This pause keeps W01 OPEN; it does not integrate a completed
release or authorize W02.

Existing R4-BASE-SCHED-01/META-01/HDF5-01, OBS-004/005/006, DEFECT-2 and
historical pandas/Actions notices retain their PRD dispositions. Their current
runtime impact is not yet established. No hardware, UI, dependency, storage
repair, global scheduler repair or package publication is in scope.
