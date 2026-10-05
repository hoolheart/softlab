# Sprint board — Release 2

Updated: 2026-10-05 | Integration branch: dev | Execution: resumed by user

| ID | Scope | Owner | Phase | Started | Expected completion | Blocker | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SIM-001 | Deterministic simulation foundation, bridge example and verification | sw-mike (test plan) → sw-celeste (design) → sw-tom (impl) → sw-celeste (review) → sw-mike (test) | Test planning | 2026-10-05 | After serial gates per tasks.md | — | — |

User resumed task execution on 2026-10-05; the preparation pause is lifted.
Exactly one task is active. `codex/tu-simulation-foundation` carries the
preparation artifacts (PRD `547e97c`, plan `1124aab`, technical review APPROVED
`7d6bb4d`, preparation inspection `f3a33bc`, CI run #162 success) and is ready
for integration into `dev` before the task branch is cut. Required behavior is
`evolve(inputs, previous_states) -> next_states`; exact API remains undecided,
and time/dt is optional model input/context.

See [tasks](tasks.md), [PRD](prd.md), [technical review](reviews/prd.md) and
[preparation inspection](principle_compliance_report.md). Baseline checks started
before the pause are recorded there honestly. Serial order per tasks.md: fresh
baseline + test plan (sw-mike) → test-plan implementability review (sw-tom) →
detailed design (sw-celeste) → design review (sw-jerry) → implementation (sw-tom)
→ code review (sw-celeste) → test execution (sw-mike) → principle inspection →
CI → integration into `dev` → architecture record → release acceptance (sw-camille).
UI and real hardware gates are N/A.
Done requires gated integration into `dev`; `main` additionally needs acceptance.

OBS-004, OBS-005, OBS-006 and DEFECT-2 remain tracked Release 1 debt. Baseline
cache/font diagnostics are recorded without a zero-warning claim or waiver.
