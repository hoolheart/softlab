# Sprint board — Release 2

Updated: 2026-10-05 | Integration branch: dev | Execution: paused by user

| ID | Scope | Owner | Phase | Started | Expected completion | Blocker | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SIM-001 | Deterministic simulation foundation, bridge example and verification | Unassigned until resumed | Backlog | — | After user resumption and serial gates | User requested pause before first task | — |

No task is active. `codex/tu-simulation-foundation` exists as the preparation
branch from `80b0b05`; it does not signify task execution authorization.
Clarified PRD pushed at `547e97c`, aligned plan at `1124aab`, and revised technical
review APPROVED at `7d6bb4d`. Required behavior is
`evolve(inputs, previous_states) -> next_states`; exact API remains undecided,
and time/dt is optional model input/context.

See [tasks](tasks.md), [PRD](prd.md), [technical review](reviews/prd.md) and
[preparation inspection](principle_compliance_report.md). Baseline checks started
before the pause are recorded there honestly; no test-plan, design, production
or new test files were created. Test planning, developer review, detailed design,
implementation, code review, task verification, completion principle checks,
candidate CI and acceptance remain pending. UI and real hardware gates are N/A.
Done requires gated integration into `dev`; `main` additionally needs acceptance.

OBS-004, OBS-005, OBS-006 and DEFECT-2 remain tracked Release 1 debt. Baseline
cache/font diagnostics are recorded without a zero-warning claim or waiver.
