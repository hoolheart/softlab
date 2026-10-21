# M01-W01 serial task backlog

Owner: sw-jerry (Architect). Date: 2026-10-18.
[Activated PRD](prd.md) · [Architecture plan](architecture-plan.md).

| Order / ID | Deliverable | Coverage | State |
| --- | --- | --- | --- |
| 1 / M01-W01-001 | Executable headless thermal reference example, guide, assertions, persistence/plot/failure/cleanup evidence | AC-01–12, NFR-01–05 | Design: setup complete; test plan pending |

This single vertical slice keeps acquisition, persistence, truthful outcomes and
cleanup together. Role phases are not separate concurrent tasks. Branch:
`codex/m01-w01-thermal-example`, from dev `e034825`. No other task is started.

Required serial handoffs: Tester baseline/characterization and assertion test
plan → Developer implementability review → Designer detailed design → Architect
review → Developer implementation → Designer independent code review and closure
→ Tester execution/failure closure → principle inspection → candidate CI → dev
integration. Each completed log update is immediately committed/pushed with
`docs(log):` before handoff. Implementation cannot bypass earlier gates.
After integration: actual architecture verification, release principle inspection
and PO acceptance; main promotion is a distinct release action. No package
publication/version task is authorized. UI and hardware gates are N/A.

Forecast: one focused working week including reviews, subject to evidence and
capacity; no fixed calendar deadline. Prerequisite defects trigger reviewed
replanning, not scope creep. Current runtime results, review verdicts, CI and merge
are pending; start readiness does not certify them.
