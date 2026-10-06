# Sprint board — Release 3 execution

Updated: 2026-10-06 | Integration branch: dev | Execution: **Mode D confirmed by user — all four tasks, strict serial**

| ID | Scope | Owner | Phase | Started | Expected completion | Blocker | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| R3-001 | Scoped Release 2 corrections and closure reconciliation | sw-tom (implementation) | Implementation | 2026-10-06 | 2026-10-09 | — | — |
| R3-002 | Object builders and station membership | Unassigned | Backlog | — | — | R3-001 dev integration | — |
| R3-003 | Shared clock, delayed graph, mock signals and atomic coordination | Unassigned | Backlog | — | — | R3-002 dev integration | — |
| R3-004 | Connected guide/example and final delivery evidence | Unassigned | Backlog | — | — | R3-003 dev integration | — |

## Execution authorization

User confirmed Mode D (selective development — all four planned tasks) on
2026-10-06, lifting the recorded USER PAUSE. Preparation artifacts remain as
planned: PRD `9586b2b`; technical review APPROVED `ed2276a`; architecture plan
`2052b60`; serial backlog `5175fed`; preparation principle inspection PASS
`8ef37df`. See [PRD](prd.md), [review](reviews/prd.md),
[architecture plan](architecture-plan.md), [tasks](tasks.md) and
[principle report](principle_compliance_report.md).

## Step 0 — preparation integration (DONE, formally reviewed)

- `codex/release-3-preparation` merged into `dev` as `2dee90e`
  (`docs(log): integrate release 3 preparation records`, `--no-ff` per repo
  convention) and pushed.
- CI run 37434708422 on `dev`: **SUCCESS** (Python 3.9/3.13 matrix green).
- Preparation branch deleted (local + remote).
- Task branch `codex/r3-001-corrections` created from updated `dev` (`2dee90e`)
  and pushed; all R3-001 artifacts belong on this branch.

## Active task: R3-001

Pipeline per [tasks.md](tasks.md): test plan/characterization (sw-mike) →
developer review (sw-tom) → detailed design (sw-celeste) → architect review
(sw-jerry) → implementation (sw-tom) → independent review (sw-celeste) →
testing (sw-mike) → principle inspection → candidate CI → dev integration.
Review issues are closed by sw-celeste; test failures by sw-mike. Every process
record is committed/pushed immediately with `docs(log):` on
`codex/r3-001-corrections`.

Progress:
- ✅ Test plan + characterization baseline committed `b1bed62`; review
  `5ffda3a` issued one minor issue; closed by sw-mike `b5f2202`; reviewer
  re-confirmed, verdict APPROVED at `d657e13`. 23 test cases (15 automated
  + R3-TC-07t wording pin + 5 manual record checks + CHK-08-1 scope checklist).
- ✅ Detailed design `46a17fe`; architect review APPROVED `24f129f` (C-6
  ACCEPT: arch.md:291 in scope); 2 minor issues closed by sw-celeste `88ca37d`.
- ▶ Implementation (sw-tom) in progress next.

R3-001 scope (from tasks.md): correct Release 2 standalone bridge readback,
malformed callback-key errors and reset claims; reconcile closure records
against commits; includes existing docstring/guide/architecture wording
corrections. Acceptance coverage: R3-AC-07, standalone portion of R3-AC-08.
It must not invent past evidence or promotion.

Done requires dev integration; main promotion requires release acceptance and
separate authorization. UI/hardware gates are N/A. Existing Release 1 debt
(OBS-004/005/006, DEFECT-2) remains tracked separately and is out of scope.
