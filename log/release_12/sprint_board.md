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
| P4 — Reference comparison and acceptance | Both thermal modes + second client, oracles/tolerances, fresh-environment + clean-process walkthroughs, product acceptance, single release closure (candidate CI → `dev` → promote `main`) | sw-mike → sw-camille | Phase 3 integrated |

## In progress

| Task | Description | Assigned | Status |
| --- | --- | --- | --- |
| P3 — Storage and inspection | Storage test plan written (`f25c497`); next: developer plan review → detailed design → architecture review → implementation → code review → tests | sw-mike → sw-tom → sw-celeste → sw-jerry → sw-tom → sw-celeste → sw-mike | Branch `codex/m03-p3-storage` from `dev` (`81f269c`) |

## Phase plan (from PRD; four serial gated phases, one release)

| Phase | Status |
| --- | --- |
| 1 — Characterize and decide | DONE — decision: minimal common JSON-text envelope on HDF5, acquisition-snapshot-only, HDF5-only, thermal v1/v2 preserved (`design/p1-boundary-decision.md`, PO concurrence, independent review ACCEPTED). |
| 2 — Schema and encoding | DONE — codec + group seam implemented, reviewed, tested (459 tests OK). |
| 3 — Storage and inspection | NEXT: storage integration per decision: owned HDF5 writes, disposable-copy preflight/hydration reads, snapshot stages, fault/original-byte evidence, fresh-process inspection. |
| 4 — Reference comparison and acceptance | NOT STARTED |

## Done

| Task | Integrated revision | Date |
| --- | --- | --- |
| RG — Requirements gate | Candidate `08c0858` CI [41256246749](https://github.com/hoolheart/softlab/actions/runs/41256246749) passed; `dev` merge `8cdea23` | 2026-11-01 |
| P1 — Characterize and decide | Candidate `f6797cd` CI [41263124079](https://github.com/hoolheart/softlab/actions/runs/41263124079) passed 3.9+3.13; principle inspection PASS; boundary decision option (a) concurred; independent review ACCEPTED | 2026-11-01 |
| P2 — Schema and encoding | Candidate `cb2c8ca` CI [41302911516](https://github.com/hoolheart/softlab/actions/runs/41302911516) passed 3.9+3.13; design APPROVED (10 findings closed); plan APPROVED; code review zero defects; 459 tests OK; principle inspection PASS; `dev` merge `81f269c` | 2026-11-01 |

## Gate log

- 2026-11-01: Mode A confirmed by user; PRD hold lifted. No test/design/runtime
  result claimed yet; unrun gates remain pending.
- 2026-11-01: Requirements gate integrated to `dev` (`8cdea23`); phase 1 starts.
- 2026-11-01: Phase 1 complete — 47 characterization tests (405 total suite OK),
  boundary decision concurred (option a), independent review ACCEPTED, CI green
  on 3.9+3.13 after tester-owned failure closure; principle inspection PASS.
- 2026-11-01: Phase 2 complete — codec + group seam (`a437dc3`), design and plan
  APPROVED after review loops, code review zero defects, 459 tests OK, CI green
  on 3.9+3.13 at `cb2c8ca`; integrated to `dev` (`81f269c`); Phase 3 started.
