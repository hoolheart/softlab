# M01-W01 serial task backlog

Owner: sw-jerry (Architect). Date: 2026-10-18.
[Activated PRD](prd.md) · [Architecture plan](architecture-plan.md).

| Order / ID | Deliverable | Coverage | State |
| --- | --- | --- | --- |
| 1 / M01-W01-001 | Executable headless thermal reference example, guide, assertions, persistence/plot/failure/cleanup evidence | AC-01–12, NFR-01–05 | Done: dev merge c89ffe6689484f1ea1eaf7088e8e2592fa21908d (2026-10-21) |

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
replanning, not scope creep. Runtime/testing, code review and task principle evidence are complete; the
approved task is integrated into dev with successful merge-head CI. See
[integration evidence](integration.md) for exact commits/runs and the candidate
per-job status inconsistency. PO release acceptance and final closure-document
integration remain pending; task completion does not certify release acceptance.
