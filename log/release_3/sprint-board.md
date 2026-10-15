# Sprint board — Release 3 execution

Updated: 2026-10-11 | Integration branch: dev | Execution: **Mode D confirmed by user — all four tasks, strict serial**

| ID | Scope | Owner | Phase | Started | Expected completion | Blocker | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| R3-001 | Scoped Release 2 corrections and closure reconciliation | sw-mike (testing) | Done | 2026-10-06 | 2026-10-07 | — | `6dc4d36` |
| R3-002 | Object builders and station membership | sw-mike (testing) | Done | 2026-10-07 | 2026-10-11 | — | `0bd97d5` |
| R3-003 | Shared clock, delayed graph, mock signals and atomic coordination | sw-mike (testing) | Done | 2026-10-11 | 2026-10-15 | — | `2991a63` |
| R3-004 | Connected guide/example and final delivery evidence | sw-celeste (review) | Code review | 2026-10-15 | 2026-10-18 | — | — |

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
- ✅ Implementation complete: `148959b` callback-key fix, `7c63685` reset
  docstrings, `7657eda` bridge before_get rebind + R3-TC-07a–07m tests,
  `bf7ee44` guide/arch wording, `a26608b` release 2 closure reconciliation.
  Self-test: 147 tests OK (12 new), compileall clean, import smoke 0.3.0,
  `-W error::Warning` gate green. One documented deviation: SIM-TC-06a test
  docstring wording neutralized to "authoritative input store" (assertion
  unchanged) — recorded in `7657eda`.
- ✅ Independent code review APPROVED, zero issues, `90d5bca` (CHK-08-1
  scope checklist discharged by the review).
- ✅ Testing PASS (21/21 cases), `79cdec8`; principle inspection PASS,
  `ba61b52`; candidate CI 37590504447 SUCCESS.
- ✅ Integrated into `dev` as `6dc4d36`; dev CI 37590725020 SUCCESS;
  task branch deleted. R3-001 complete.

## Active task: R3-002

Object builder/registry and station object membership, explicit lookup,
naming, build failure safety and independent instances (membership/builder
portion of R3-AC-04; R3-AC-08). Task branch `codex/r3-002-builders` from dev
`6dc4d36`. Pipeline identical to R3-001.

Progress:
- ✅ Test plan + baseline `89d6f62` (24 cases; 18 new-capability cases
  vacuous-fail on baseline as required); review CHANGES-REQUESTED `fa40165`
  (2 minor); closed by sw-mike `6b34844`; re-confirmed APPROVED `5a982d5`.
- ✅ Detailed design `59c97e7`; architect review APPROVED `6973d9b` (rulings:
  add_device guard / device-only snapshot / RuntimeError all ACCEPT); 1 minor
  issue closed by sw-celeste `fefe992`. Implementation authorized when resumed.
- ✅ Implementation complete (TDD red/green: `1ff6d6b` red → `90282ba`/`06cd8b3`
  green): builder registry `90282ba`, station membership `06cd8b3`, fixture
  fixes `a4b717a`, device-side guard coverage `ceec02f` (companion ruling
  disposition: implemented, passing). Self-test: 165 tests OK, zero warnings
  under `-W error::Warning`, compile/import clean. No deviations.
- ✅ Independent code review APPROVED, zero issues, `8d20542` (5 targeted
  production mutations all killed by the designed tests; CHK-04-1 discharged,
  docstring citation station.py:234).
- ✅ Testing PASS (24/24 cases), `99386c9`; principle inspection PASS,
  `a4d3ad1`; candidate CI 38126716631 SUCCESS.
- ✅ Integrated into `dev` as `0bd97d5`; dev CI 38126816882 SUCCESS; task
  branch deleted. R3-002 complete.

## Active task: R3-003

Cohesive coordinated simulation: opt-in mock endpoints, connections,
ownership, shared sim_dt, tick-zero preparation, delayed stepping and atomic
reset (R3-AC-01–06; compatibility portion of R3-AC-08). Deliberately one task —
graph, clock, ownership and transaction publication ship together or not at
all. Task branch `codex/r3-003-coordination` from dev `0bd97d5`. Design must
prove snapshot publication, ownership/reentrancy, safe detach, model-dt binding
and numeric clock limits before implementation (required design gates per
tasks.md).

Progress:
- ✅ Test plan + baseline `6fcdb4e` (41 automated cases after review fixes,
  covering R3-AC-01–06 + R3-AC-08 compatibility; coordinated capability
  confirmed absent on baseline); review CHANGES-REQUESTED `88a4e5b` (4 minor);
  closed by sw-mike `9ee0394`; re-confirmed APPROVED `8b34583`.
- ✅ Detailed design `92eaa62` (1267 lines; 5 CHK-03-1 proofs); architect
  review APPROVED `e8d9841` (all 6 rulings ACCEPT; proofs withstood attack;
  golden traces re-derived exactly); 4 minor issues closed `f5d8680`
  (sim_dt `_busy` guard now normative; endpoint resolution rule explicit).
- ✅ Implementation complete (TDD red `5d8fc6a` → green, 7 atomic commits):
  mock endpoints `9a65f0f`, coordinator `a1015e0`, object candidate/commit
  helpers + guards `f4ab4d0`, station sim_dt/coordinator integration
  `f8535d9`, test greening `63251c2`, interval-rule single-sourcing
  `8199d2b`. Self-test: 206 tests OK (165 + 41), zero warnings under
  `-W error::Warning`, compile/import clean. No deviations; 6 documented
  ambiguity choices.
- ✅ Independent code review: CHANGES-REQUESTED `ee4339a` (1 medium + 2 low,
  docstring/test-cosmetic), fixed by sw-tom `1aa961f`, re-confirmed
  APPROVED `7a4a30b` (6 kill-mutations verified; greening commit audited
  genuinely fixture-side).
- ✅ Testing PASS (41/41 + CHK-03-1 + 04e extension), `5ecebf7`; principle
  inspection PASS, `ceea4c6`; candidate CI 38819966184 SUCCESS.
- ✅ Integrated into `dev` as `2991a63`; dev CI 38820136267 SUCCESS; task
  branch deleted. R3-003 complete.

## Active task: R3-004 (final)

Executed connected example and user guide; final architecture, integration
and release evidence (remaining R3-AC-08; release-wide acceptance of
R3-AC-01–08). Task branch `codex/r3-004-delivery` from dev `2991a63`.
Carry-over scope note from R3-003 design review: the additive
simulation-description node deferred from R3-002 (snapshot/describe) is to be
considered in R3-004 planning against the PRD — it is NOT pre-authorized.
Release acceptance by the Product Owner and any `main` promotion require
  separate user authorization after this task integrates.

Progress:
- ✅ Test plan + baseline `72fcfbf` (8 automated + 6 manual cases; scope gate:
  snapshot/describe simulation node OUT of scope for Release 3, recorded as
  ratification item R3-TC-09m6); review CHANGES-REQUESTED `df29394` (1 minor);
  closed by sw-mike `5e88dd2`; re-confirmed APPROVED `c17abdd`.
- ✅ Detailed design `f24b38f` (783 lines; §13 guide example w/ golden trace
  verified by live execution; zero production changes planned; arch.md A1–A10);
  architect review APPROVED `0cabcb9` (5 rulings ACCEPT); 2 minor issues
  closed `4ed0ccc`.
- ✅ Implementation complete (TDD red `0c9bd34` → green): failing cases
  `0c9bd34`, guide §12–§13 `baa751f`, arch.md capability record `f32b796`,
  candidate-point fill `45d4244`, board note `789e9ca`. 210 tests OK (207+3),
  zero warnings, scope guard EMPTY production diff verified. Candidate CI
  38826003029 (f32b796) and fill-commit CI 38826175913 (45d4244) both SUCCESS.
- ▶ Independent code review (sw-celeste) in progress next.
- ✅ Implementation complete (TDD red `0c9bd34` → green `45d4244`, 4 atomic
  commits): failing delivery cases `0c9bd34` (R3-TC-09a/09b/09h red);
  guide §12 coordinated simulation + §13 executed connected example
  (byte-reproduced output verified by live execution against the design
  golden trace) `baa751f`; arch.md Release 3 records A2–A9 `f32b796`;
  Stage-1 candidate-point fill per the Issue-1 fill-commit convention
  `45d4244`. Self-test: 210 tests OK (207 + 3), zero warnings under
  `-W error::Warning`, compileall + import smoke clean, production diff
  empty (softlab/ untouched). Candidate CI 38826003029 SUCCESS at
  `f32b796`; fill-commit CI 38826175913 SUCCESS at `45d4244`.
  Scope-gate ratification record (R3-TC-09m6) remains with
  sw-prod/sw-camille.

R3-001 scope (from tasks.md): correct Release 2 standalone bridge readback,
malformed callback-key errors and reset claims; reconcile closure records
against commits; includes existing docstring/guide/architecture wording
corrections. Acceptance coverage: R3-AC-07, standalone portion of R3-AC-08.
It must not invent past evidence or promotion.

Done requires dev integration; main promotion requires release acceptance and
separate authorization. UI/hardware gates are N/A. Existing Release 1 debt
(OBS-004/005/006, DEFECT-2) remains tracked separately and is out of scope.
