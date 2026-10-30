# Sprint board — M02-W01 (process release_8)

Updated: 2026-10-30 Asia/Shanghai (Product Owner acceptance **PASS**). Integration
branch: `dev`. Current task branch: `codex/m02-w01-preparation`.
Release status: **OPEN — CLOSURE (MINIMAL GATES, USER-APPROVED)**. The user chose
Option A (finish this release now) with explicitly cut checks. Preparation was
pushed at `1935fab`; characterization at `d300c7e`; decision + reviews + scope
agreement through `b47fb3d`. Completed minimal gates: AC01 caller-inventory
check **COMPLETE** ([verdict](reviews/caller-inventory-check.md)); Product Owner
acceptance **PASS** ([record](acceptance.md)). Remaining minimal finish line:
task-completion principle/document checks → frozen exact-candidate CI → `dev`
merge → `main` promotion and verification → clean worktree → one closure summary
→ CLOSED. Cut by user authorization: the independent full review round of the
characterization suite (zero production-code changes in this release; suite
developer-reviewed at plan level; full 306-test regression evidence reusable).
Applicability and this scope reduction are recorded in the acceptance record and
remain required in the closure summary and principle compliance report.

| ID | Scope | Owner / next handoff | Phase | Started | Expected completion | Blockers / remaining gates | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M02-W01-001 | Characterize existing count execution and decide minimal justified seam | Coordinator / post-acceptance closure sequence | Closure — product accepted | 2026-10-30 | 2026-10-30 | Product acceptance: **PASS** ([record](acceptance.md)); task-completion principle/document checks, frozen exact-candidate CI, `dev`/`main` integration and promotion verification, closure summary and clean worktree pending; W02 unauthorized | None |

Exactly one task is active. W02 implementation has not started. M01 is
[CLOSED](../release_7/closure.md); the M02 milestone planning reviews approve
planning only and do not pass any W01 runtime gate.

## Current evidence and handoff

The [PRD](prd.md) and [task phases](tasks.md) define the approved scope.
[Architect requirements review](architecture-review.md),
[Developer test-plan review](reviews/test-plan-review.md) and
[Architect design review](reviews/design-review.md) approve their respective
preparation gates. DR-TP-01 and AR-DD-01 were closed by their respective
reviewers.

Executable characterization ran against the unchanged baseline (revision
`35e6e9b`, Python 3.13.15): `tests/test_m02_w01_characterization.py` passes 25
tests, full discovery passes 306 tests with no observed warnings, and
`compileall`/import smoke checks succeed. See
[characterization results](test/characterization-results.md) for commands,
per-assertion traceability, baseline findings F-01…F-07 and explicit limits.
The [evidence decision](design/evidence-decision.md) selects candidate 3 and has
[Architect approval](reviews/evidence-decision-review.md), with AR-ED-01…03
confirmed closed. The independent AC01 inventory check is COMPLETE and the
[Product Owner acceptance](acceptance.md) is PASS. The user-authorized cut of
the independent full characterization-suite review is explicitly not represented
as an independent-review pass. No exact-candidate CI, integration, promotion or
closure outcome is claimed.

The task is now in **POST-ACCEPTANCE CLOSURE** under the minimal finish line.
The coordinator owns the remaining principle/document checks, final candidate
freeze and commit/push facts, exact-candidate CI, integration/promotion,
promoted-state verification, clean-worktree check and authoritative closure
summary. W01 remains OPEN until those gates complete; W02 has not started and
remains unauthorized.

Existing R4-BASE-SCHED-01/META-01/HDF5-01, OBS-004/005/006, DEFECT-2 and
historical pandas/Actions notices retain their PRD and acceptance dispositions.
R4-BASE-SCHED-01 is confirmed at runtime and avoided rather than repaired;
historical warnings are not observed in the recorded environment, not waived.
No hardware, UI, dependency, storage repair, global scheduler repair or package
publication is in scope.
