# Sprint board — Release 12 (M03 reproducible run record)

Owner: sw-prod (coordinator) | Updated: 2026-11-01 Asia/Shanghai
Engagement mode: **A — Full Release** (user directive 2026-11-01: "continue and
finish release 12 due to verified prd"; this is the explicit PRD confirmation that
lifts the kickoff hold in [kickoff.md](kickoff.md) and [prd.md](prd.md)).
Subagents: sw-camille (PO/acceptance), sw-jerry (architect), sw-celeste
(detailed design/code review), sw-tom (implementation), sw-mike (testing).
No UI work; UI gates do not apply.

Serial rule: ONE phase/task active at a time; next starts only after the current
one is integrated to `dev` with evidence committed/pushed. Branch flow:
`codex/<task>` → candidate CI → `dev`; single accepted promotion to `main` and
one closure at release end ([release checklist](../templates/release-checklist.md)).

## Backlog

| Task | Description | Assigned | Gate before start |
| --- | --- | --- | --- |
| P2 — Schema and encoding | Bounded field/type, version, missing/null/unknown and failure contract; justified codec/construction seam only | sw-celeste → sw-tom → sw-mike | Phase 1 boundary decision approved |
| P3 — Storage and inspection | Selected evidenced storage paths, snapshot stages, fault/original-byte evidence, fresh-process inspection | sw-celeste → sw-tom → sw-mike | Phase 2 integrated |
| P4 — Reference comparison and acceptance | Both thermal modes + second client, oracles/tolerances, walkthroughs, acceptance, single closure | sw-mike → sw-camille | Phase 3 integrated |

## In progress

| Task | Description | Assigned | Status |
| --- | --- | --- | --- |
| P1 — Characterize and decide | Characterization plan + executable characterization, HDF5 disposable-copy metadata inspection, two-client inventory, Product/Architect boundary decision, independent review | sw-mike → sw-camille/sw-jerry | Starting; branch `codex/m03-p1-characterize` from `dev` |

## Phase plan (from PRD; four serial gated phases, one release)

| Phase | Status |
| --- | --- |
| 1 — Characterize and decide | NEXT after RG integrates: characterization plan/tests (sw-mike), runtime/source characterization, HDF5 disposable-copy metadata inspection, two-client inventory, Product/Architect common-versus-separate decision, independent review. No schema implementation before the decision is approved. |
| 2 — Schema and encoding | NOT STARTED |
| 3 — Storage and inspection | NOT STARTED |
| 4 — Reference comparison and acceptance | NOT STARTED |

## Done

| Task | Integrated revision | Date |
| --- | --- | --- |
| RG — Requirements gate | Candidate `08c0858` CI [41256246749](https://github.com/hoolheart/softlab/actions/runs/41256246749) passed; `dev` merge `8cdea23` | 2026-11-01 |

## Gate log

- 2026-11-01: Mode A confirmed by user; PRD hold lifted. No test/design/runtime
  result claimed yet; unrun gates remain pending.
- 2026-11-01: Requirements gate integrated to `dev` (`8cdea23`); phase 1 starts.
