# Sprint Summary — Release 3

Date: 2026-10-16 | Integration branch: `dev` at `ed51609` (+ inspection `d49947d`)

## Sprint goal and achievements

Deliver the Release 3 connected-simulation scope per
[PRD](prd.md): correct Release 2 closure gaps, object builders and station
membership, cohesive coordinated simulation (shared clock, delayed graph,
mock endpoints, atomic coordination), and an executed connected example with
final delivery evidence. **All four tasks Done; release-wide acceptance
matrix R3-AC-01–08 all PASS** ([r3-004 results](test/r3-004-test-results.md)
§09m5).

## Tasks completed vs planned

| Task | Merged to dev | Gates |
| --- | --- | --- |
| R3-001 Scoped Release 2 corrections + closure reconciliation | `6dc4d36` | plan APPROVED `d657e13`; design `88ca37d`; review 0 issues `90d5bca`; tests 21/21 `79cdec8`; principles `ba61b52` |
| R3-002 Object builders and station membership | `0bd97d5` | plan `5a982d5`; design `fefe992`; review 0 issues `8d20542` (5/5 mutation kills); tests 24/24 `99386c9`; principles `a4d3ad1` |
| R3-003 Coordinated simulation (clock, graph, mocks, atomic coordination) | `2991a63` | plan `8b34583` (41 cases); design `f5d8680` (5 proofs, 6 rulings); review `7a4a30b` (6 kill-checks); tests 41/41 + CHK-03-1 `5ecebf7`; principles `ceea4c6` |
| R3-004 Executed connected example + delivery evidence | `86ba510` | plan `c17abdd`; design `4ed0ccc`; review `b82f5ec`; tests 14/14 incl. 09m1–09m6 `673a021`; principles `4bcb05f` |

Strict serial execution: one active task throughout; each task branched
`codex/r3-00X-*` from the then-current `dev`, integrated `--no-ff` after
candidate CI, branch deleted. Dev CI green at every integration
(38829754958 etc.).

## Quality metrics

- Test pass rate: **100%** — 210 tests at head (135 at release start → +75),
  zero failures at every gate; release-wide matrix 8/8 PASS.
- Review issues: 20 raised across 12 review records (1 test-plan + 2 design
  R3-001; 2 test-plan + 1 design R3-002; 4 test-plan + 4 design + 3 code
  R3-003; 1 test-plan + 2 design + 2 code R3-004) — **all closed and
  reviewer-reconfirmed; zero open issues, zero waivers**.
- Build warnings: **0** — `-W error::Warning` full-suite green at every
  task completion and at release completion; compileall clean.
- CI: every candidate commit and every dev-integration commit green
  (Python 3.9/3.13 matrix).
- Files generated: 4 detailed designs, 12 review records, 5 test artifacts
  (plan/baseline/results ×3 + plan/results R3-004), sprint board, principle
  report (6 sections), plus delivered code/docs (see git log).

## Delivered capability (implemented, on `dev`)

- `softlab/tu/simulation/object.py`: bridge readback correction hooks,
  documented callback-key errors, honest reset claims, coordination guards.
- `softlab/tu/simulation/builder.py`: ObjectBuilder registry (R3-002).
- `softlab/tu/station/station.py`: object membership, build_object,
  shared sim_dt, coordinator binding.
- `softlab/tu/simulation/coordination.py` + `mock.py`: Coordinator
  (snapshot publication, delayed edges, tick-zero, atomic reset, numeric
  clock limits), MockControl/MockMeasurement endpoints.
- Docs: user guide §12–§13 executed connected example (byte-reproduced),
  arch.md Release 3 records with claim-by-claim traceability.

## Issues encountered and resolutions

- Every review cycle found only minor/editorial issues; all were fixed at
  the responsible role and re-confirmed by the reviewer (no issue bypassed).
- R3-003 was deliberately one task (per backlog decision): partial shipment
  of edge delays/reset rollback would have left an inconsistent contract.
- Two user pauses (2026-10-06, 2026-10-11/15) recorded transparently on the
  board; execution resumed from the exact paused gate.

## Lessons learned

- Vacuous-fail [PLAN] pinning + mutation kills proved the strongest
  correctness signal for the coordination core.
- The fill-commit convention (candidate CI certifies an ancestor) keeps
  architecture records honest about what is proven where.

## Outstanding (owned, by design)

- R3-TC-09m6 scope-gate ratification: snapshot/describe simulation node
  deferred beyond Release 3 — signature with sw-prod/sw-camille pending.
- Release 1 debt (OBS-004/005/006, DEFECT-2): tracked separately, out of scope.
- Product Owner release acceptance and `main` promotion: separate user
  authorization required.

## Plans for next step

Product Owner acceptance review of the release against the PRD (sw-camille),
then — if authorized — `main` promotion; otherwise Release 3 remains
delivered on `dev`.
