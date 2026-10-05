# Release 3 task backlog — PLANNED

Owner: sw-jerry | Date: 2026-10-05 | Execution: not authorized
Baseline: `603ba85` | [PRD](prd.md) | [Architecture plan](architecture-plan.md)

| Order / ID | Bounded deliverable | Acceptance coverage | Prerequisite | Phase |
| --- | --- | --- | --- | --- |
| 1 / R3-001 | Correct Release 2 standalone bridge readback, malformed callback-key errors and reset claims; reconcile closure records against commits | R3-AC-07, standalone portion of 08 | User resumes execution; preparation gate | Backlog |
| 2 / R3-002 | Object builder/registry and station object membership, explicit lookup, naming, build failure safety and independent instances | Membership/builder portion of R3-AC-04; 08 | R3-001 integrated into dev | Backlog |
| 3 / R3-003 | Cohesive coordinated simulation: opt-in mock endpoints, connections, ownership, shared sim_dt, tick-zero preparation, delayed stepping and atomic reset | R3-AC-01–06; compatibility portion of 08 | R3-002 integrated into dev | Backlog |
| 4 / R3-004 | Executed connected example and user guide; final architecture, integration and release evidence | Remaining R3-AC-08; release-wide acceptance of 01–08 | R3-003 integrated into dev | Backlog |

R3-003 deliberately keeps graph, clock, ownership and transaction publication
in one task: independently shipping partial edge delays or reset rollback would
leave an unusable/inconsistent public network contract. Per-task documentation
and verification are required throughout, not deferred wholesale to R3-004.
R3-001 includes existing docstrings/guide/architecture wording corrections and
Release 2 board reconciliation; it must not invent past evidence or promotion.

## Serial execution when explicitly resumed

For each task, create `codex/<task>` from the then-current updated `dev` only
after its predecessor is integrated. Use test plan/characterization (sw-mike)
→ developer review (sw-tom) → detailed design (sw-celeste) → architect review
(sw-jerry) → implementation (sw-tom) → independent review (sw-celeste) → testing
(sw-mike) → principle inspection → candidate CI → integration into `dev`.
All review issues/test failures retain reviewer/tester closure ownership.
Commit and push every completed process update immediately with `docs(log):`.
The preparation branch is not authority to bypass task branches or gates.

Fresh baseline, warning/environment evidence, compatibility checks and concrete
tests are execution work and remain pending. UI/hardware gates are N/A. After
all tasks: actual architecture verification, release principle inspection and
Product Owner acceptance precede any separately authorized `main` promotion.
Only one task may be active. Current task-start permission is absent: stop here.

## Scope and open constraints

All production work stays in `tu`; tests/docs/process records may change outside
it. No concrete `huo` simulation process or automatic scheduler work is included.
No unresolved product blocker was identified. Detailed design must prove snapshot
publication, ownership/reentrancy, safe detach, model-dt binding and numeric clock
limits before R3-003 implementation. These are required design gates, not passed
checks. Existing Release 1 OBS-004/005/006 and DEFECT-2 remain tracked separately;
no unrelated fixes, warning waivers or dependency changes are authorized.
