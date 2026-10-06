# Sprint board — Release 3 preparation

Updated: 2026-10-05 | Integration branch: dev | Execution: paused before first task

| ID | Scope | Owner | Phase | Started | Expected completion | Blocker | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| R3-001 | Scoped Release 2 corrections and closure reconciliation | Unassigned until resumed | Backlog | — | — | USER PAUSE; preparation PASS | — |
| R3-002 | Object builders and station membership | Unassigned | Backlog | — | — | R3-001 integration | — |
| R3-003 | Shared clock, delayed graph, mock signals and atomic coordination | Unassigned | Backlog | — | — | R3-002 integration | — |
| R3-004 | Connected guide/example and final delivery evidence | Unassigned | Backlog | — | — | R3-003 integration | — |

No task active. Preparation branch: `codex/release-3-preparation`, baseline
`603ba85`. PRD `9586b2b`; technical review APPROVED `ed2276a`; planned architecture
`2052b60`; serial backlog `5175fed`, all committed/pushed before this board.
See [PRD](prd.md), [review](reviews/prd.md), [architecture plan](architecture-plan.md)
and [tasks](tasks.md). [Preparation principle inspection](principle_compliance_report.md)
PASS at `8ef37df` (committed/pushed), for readiness only. USER PAUSE remains;
this does not authorize the first task.

No test plan, detailed design, implementation or runtime checks have been started
in Release 3 preparation. All task review/test/principle/CI/integration gates and
release acceptance are pending. UI/hardware gates are N/A. Each resumed task
starts from updated dev after the predecessor's gated integration. Done requires
dev integration; main promotion requires acceptance and separate authorization.
The future concrete huo process is excluded. Existing Release 1 debt remains open.
