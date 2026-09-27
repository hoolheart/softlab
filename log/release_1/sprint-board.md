# TU extension board

User authorization: 2026-09-27, "continue to finish remaining tasks".
Scope: TU-001 through TU-008 from release_0/tasks.md. Integration: dev.
No main promotion, real hardware, or mu/huo implementation is included.

| Task | State | Owner | Evidence |
| --- | --- | --- | --- |
| TU-001 | Done | Product owner, architect, developer, tester, reviewer | tests b5599ff; review 1c2886e APPROVED; dev integrated 6f39f78 after CI 36302785650 |
| TU-002 | Design | Tester, developer, designer, architect | codex/tu-002-descriptions from d5b7df9; tests and design gates pending |
| TU-003 | Backlog | Unassigned | Explicit operations |
| TU-004 | Backlog | Unassigned | Optional lifecycle/capabilities |
| TU-005 | Backlog | Unassigned | Measurement semantics |
| TU-006 | Backlog | Unassigned | VISA operation contracts |
| TU-007 | Backlog | Unassigned | Theory contracts |
| TU-008 | Backlog | Unassigned | Integration/documentation |

Only one task may advance beyond Backlog before its predecessor integrates.

## TU-001 integration evidence

Candidate `6f39f78066021a048da82364756bff79294da7d9` passed both Python 3.9
and 3.13 jobs in [run 36302785650](https://github.com/hoolheart/softlab/actions/runs/36302785650).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched. This completion-record commit must also pass
CI before its fast-forward into `dev`.
