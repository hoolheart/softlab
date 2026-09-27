# TU extension board

User authorization: 2026-09-27, "continue to finish remaining tasks".
Scope: TU-001 through TU-008 from release_0/tasks.md. Integration: dev.
No main promotion, real hardware, or mu/huo implementation is included.

| Task | State | Owner | Evidence |
| --- | --- | --- | --- |
| TU-001 | Done | Product owner, architect, developer, tester, reviewer | tests b5599ff; review 1c2886e APPROVED; dev integrated 6f39f78 after CI 36302785650 |
| TU-002 | Done | Developer, tester, reviewer, designer, architect | tests 86a6063 33/33 green; review b613f2a APPROVED; dev integrated 8794cb7 after CI 36306104192 |
| TU-003 | Design | Tester, developer, designer, architect | codex/tu-003-operations from 8adc2c5; test cases first |
| TU-004 | Backlog | Unassigned | Optional lifecycle/capabilities |
| TU-005 | Backlog | Unassigned | Measurement semantics |
| TU-006 | Backlog | Unassigned | VISA operation contracts |
| TU-007 | Backlog | Unassigned | Theory contracts |
| TU-008 | Backlog | Unassigned | Integration/documentation |

Only one task may advance beyond Backlog before its predecessor integrates.

## TU-002 integration evidence

Candidate `8794cb7e958e32dbdba54d806eec72fafe28e0ab` passed both Python 3.9
and 3.13 jobs in [run 36306104192](https://github.com/hoolheart/softlab/actions/runs/36306104192).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched.

## TU-001 integration evidence

Candidate `6f39f78066021a048da82364756bff79294da7d9` passed both Python 3.9
and 3.13 jobs in [run 36302785650](https://github.com/hoolheart/softlab/actions/runs/36302785650).
`dev` was fast-forwarded and pushed to that candidate. No merge commit was
created; `main` was untouched. This completion-record commit must also pass
CI before its fast-forward into `dev`.
