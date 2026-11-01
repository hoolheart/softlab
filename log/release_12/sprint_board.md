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
| P2 — Schema and encoding | Bounded field/type, version, missing/null/unknown and failure contract; justified codec/construction seam only | sw-celeste → sw-tom → sw-mike | Awaiting P1 integration, then branch `codex/m03-p2-schema` from `dev` |

## Phase plan (from PRD; four serial gated phases, one release)

| Phase | Status |
| --- | --- |
| 1 — Characterize and decide | DONE — decision: minimal common JSON-text envelope on HDF5, acquisition-snapshot-only, HDF5-only, thermal v1/v2 preserved (`design/p1-boundary-decision.md`, PO concurrence, independent review ACCEPTED). |
| 2 — Schema and encoding | NEXT: detailed design per boundary decision binding constraints → architecture review → implement → code review → tests. |
| 3 — Storage and inspection | NOT STARTED |
| 4 — Reference comparison and acceptance | NOT STARTED |

## Done

| Task | Integrated revision | Date |
| --- | --- | --- |
| RG — Requirements gate | Candidate `08c0858` CI [41256246749](https://github.com/hoolheart/softlab/actions/runs/41256246749) passed; `dev` merge `8cdea23` | 2026-11-01 |
| P1 — Characterize and decide | Candidate `f6797cd` CI [41263124079](https://github.com/hoolheart/softlab/actions/runs/41263124079) passed 3.9+3.13; principle inspection PASS; boundary decision option (a) concurred; independent review ACCEPTED | 2026-11-01 |

## Gate log

- 2026-11-01: Mode A confirmed by user; PRD hold lifted. No test/design/runtime
  result claimed yet; unrun gates remain pending.
- 2026-11-01: Requirements gate integrated to `dev` (`8cdea23`); phase 1 starts.
- 2026-11-01: Phase 1 complete — 47 characterization tests (405 total suite OK),
  boundary decision concurred (option a), independent review ACCEPTED, CI green
  on 3.9+3.13 after tester-owned failure closure; principle inspection PASS.
