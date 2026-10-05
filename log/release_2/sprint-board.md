# Sprint board — Release 2

Updated: 2026-10-05 | Integration branch: dev | Execution: resumed by user

| ID | Scope | Owner | Phase | Started | Expected completion | Blocker | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SIM-001 | Deterministic simulation foundation, bridge example and verification | sw-mike (test plan) → sw-celeste (design) → sw-tom (impl) → sw-celeste (review) → sw-mike (test) | Detailed design | 2026-10-05 | After serial gates per tasks.md | — | — |

User resumed task execution on 2026-10-05; the preparation pause is lifted.
Exactly one task is active on branch `codex/sim-001-simulation-foundation`
(cut from dev @ f04e783). Completed so far: fresh baseline recorded
(99 tests OK, exit 0, zero warnings; `log/release_2/test/sim-001-baseline.md`),
test plan approved after one revision round (commits 86cddb4, 5caabc7;
implementability review 313ae9f with all issues closed at f1bbcd2, verdict
APPROVED). Required behavior is `evolve(inputs, previous_states) -> next_states`;
time/dt is optional model input/context.

See [tasks](tasks.md), [PRD](prd.md), [technical review](reviews/prd.md),
[test plan](test/sim-001-test-plan.md) and
[plan review](reviews/sim-001-test-plan-review.md). Remaining serial order per
tasks.md: detailed design (sw-celeste) → design review (sw-jerry) →
implementation (sw-tom) → code review (sw-celeste) → test execution (sw-mike) →
principle inspection → CI → integration into `dev` → architecture record →
release acceptance (sw-camille). UI and real hardware gates are N/A.
Done requires gated integration into `dev`; `main` additionally needs acceptance.

OBS-004, OBS-005, OBS-006 and DEFECT-2 remain tracked Release 1 debt. Baseline
cache/font diagnostics are recorded without a zero-warning claim or waiver.
