# Code Review — R3-004: Executed connected example, user guide, final architecture and release evidence

Reviewer: sw-celeste | Date: 2026-10-16
Branch: `codex/r3-004-delivery` | Head reviewed: `a6bb006` (original
round) | Fix re-reviewed: `3b8e42c` (in sync with
`origin/codex/r3-004-delivery`; `git diff 940bfd8..3b8e42c` touches
`arch.md` and `log/release_3/sprint-board.md` only)
(content head `789e9ca` — commit `a6bb006` touches
`log/release_3/sprint-board.md` only, verified
by `git diff 789e9ca..a6bb006 --stat` = 1 log file)
Range reviewed: `0c9bd34` (red tests), `baa751f` (guide §12–§13),
`f32b796` (arch.md capability record), `45d4244` (candidate-point fill),
plus log records `789e9ca`/`a6bb006`.
Binding specification:
[r3-004-detailed-design.md](../design/r3-004-detailed-design.md)
(APPROVED at `0cabcb9`, issues closed at `4ed0ccc` — including the
NORMATIVE A1 Stage-1 fill-commit convention and the E2 enroll-by-name
clarification) | Test plan:
[r3-004-test-plan.md](../test/r3-004-test-plan.md) (Revision 2) +
[r3-004-baseline.md](../test/r3-004-baseline.md) | Convention:
[r3-003-code-review.md](r3-003-code-review.md)

## Verdict: APPROVED

- **Re-confirmation (2026-10-16, fix commit `3b8e42c`)**: both Low
  record-hygiene issues are fixed exactly as recommended; issues 1–2
  CLOSED (confirmations inline below). The original round verdict was
  CHANGES-REQUESTED (2 Low, both OPEN); the fix commit
  `3b8e42c` resolves both with zero behavioral surface.
- **Total issues: 2** (critical: 0, high: 0, medium: 0, low: 2) — both
  CLOSED, both record-hygiene; zero issues in the deliverable substance
  (guide, tests, arch.md content).
- The implementation is faithful to the approved design
  element-for-element: the §13 example matches §E exactly, the recorded
  output is genuine executed output (independently re-run by this
  reviewer, byte-for-byte identical), §12 carries every G5 required
  statement, arch.md A1–A10 are each applied as specified, the A9
  traceability anchors resolve and name the right symbols (32
  spot-checked), and the Issue-1 fill-commit convention is honored
  precisely. The two issues are a duplicated sprint-board progress entry
  and one dangling prose reference in arch.md — both trivially fixable
  by sw-tom.

## What was executed (commands + results)

All commands from the repository root (or scratch worktrees under the
system temp dir, removed after use) using the project `.venv` (Python
3.13), matching AGENTS.md verification §1:

| Command | Result |
| --- | --- |
| `git fetch origin`; HEAD check | HEAD at `a6bb006`, working tree clean, in sync with `origin/codex/r3-004-delivery`; content head `789e9ca` |
| `git diff dev...HEAD --stat -- softlab/` (and `softlab/tu` alone) | **empty** — production diff is EMPTY (scope-gate §2 satisfied by construction; no `snapshot()`/`describe()` schema change possible) |
| `git diff dev...HEAD --stat -- pyproject.toml setup.py` | **empty** — no dependency/config changes |
| `git diff dev...HEAD --stat -- tests/` | exactly two NEW files: `tests/test_sim_connected_guide.py` (+202), `tests/test_arch_links.py` (+72); **zero edits to existing test files** (R3-TC-09g satisfied exactly as design T3 specifies) |
| `git diff dev...HEAD --check` | clean, exit 0; full diff confined to arch.md, docs/user-guide/simulation.md, the 2 new test files, and `log/` records |
| `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'` | **Ran 210 tests, OK** (207 baseline + 3 new — matches the design's expected arithmetic) |
| `.venv/bin/python -W error::Warning -m unittest discover -s tests -p 'test_*.py'` | **Ran 210 tests, OK, zero warnings** |
| `.venv/bin/python -m compileall -q softlab`; `import softlab` smoke | exit 0; version `0.3.0` prints |
| Eight-name export identity (`SimulatedObject`, `ObjectBuilder`, `register_object_builder`, `get_object_builder`, `MockControl`, `MockMeasurement`, `Coordinator`, `Endpoint` reachable as `softlab.tu.simulation.<name>`) | pass — matches the A9/G1 export-surface claim |
| Red-commit verification (scratch worktree at `0c9bd34`, run both new test modules) | **3 tests fail** (2 errors in `test_sim_connected_guide` — §13 heading absent, extraction `IndexError`; 1 failure in `test_arch_links` — no `log/release_3/` target) — the TDD-red claim is truthful |
| Independent re-execution of the guide §13 program (own extraction script, two runs) | run 1 == run 2 byte-identical; run 1 == the guide's recorded `text` block byte-for-byte (same `rstrip` discipline as SIM-TC-08b); output ends with exactly one newline; §13 code and text blocks are pure ASCII; **the recorded block is genuine executed output**, and it equals the design's §E5 golden trace verbatim |
| 09b failure-sensitivity demonstration (scratch worktree at `789e9ca`; edit the recorded block `D.u=2.0` → `D.u=9.9`) | exactly **one** failure (`test_r3_tc_09b_byte_reproduction`); 09a still passes — the designed derivation/recording split; **09b is non-vacuous** |
| 09h failure-sensitivity demonstration (scratch worktree; break one Markdown link `log/release_3/prd.md` → `nonexistent.md`) | 09h **fails**, naming the missing path; **09h is non-vacuous** |
| 09h collection audit (real arch.md) | 52 reference targets collected (non-empty), 4 under `log/release_3/` (Release 3 record cites its own evidence trail); all resolve |
| CI evidence (`gh run view`) | candidate run **38826003029**: `success`, headSha `f32b796` on `codex/r3-004-delivery`, both matrix jobs (3.9, 3.13) success; fill-commit run **38826175913**: `success`, headSha `45d4244`; dev run **38820136267**: `success`, headSha `2991a63` — every SHA cited in the arch.md header matches the commit it certifies |
| Fill-commit convention check (`git show f32b796:arch.md` vs `git show 45d4244 -- arch.md`) | `f32b796` (content head) carries **no** Release-3 header and **no** "210 green" clause; `45d4244` introduces the Stage-1 candidate header *after* run 38826003029 existed, cites that run and head SHA, and self-discloses "the cited candidate run certifies an ancestor of this fill commit" — **the Issue-1 convention is honored exactly**; the "210 green" clause never precedes the suite run |
| Runtime probes of §12 claims (clean interpreter) | `Decimal` → `TypeError`, `bool` → `TypeError`, non-positive → `ValueError`, non-finite → `ValueError`, `0.5` accepted; `enroll(obj)` (object, not name) raises `KeyError`, `enroll('X')` succeeds — the E2 clarification and §12 validation-order claims are live, not prose |
| A9 traceability anchor spot-check (32 anchors scripted against delivered files) | all 32 resolve and name the right symbols, incl. **`object.py:947` = `_validate_callback_result`** (checklist-mandated), `coordination.py:171` = `class Coordinator`, `:967` = `def prepare`, `:1037` = `def step`, `:1165` = `def reset`, `:858` = `def bind_interval`, `:750` = `def connect`, `:1264` = `def _guard_set_input`, `:58` = `def _validate_interval`, `mock.py:33` = `class MockControl`, `mock.py:142` = `class MockMeasurement`, `station.py:105` = `sim_dt` property, `station.py:324-331` = the `rm_object` ownership guard, `object.py:390/521-522/586/715` = claim slot + three guards, `__init__.py:3-15` = the eight-name export block |
| Guide hygiene greps | zero matches for `only public name`, `planned Release 3`, `not stated here`, `planned work`; no CJK characters anywhere in the guide; §13 code/text blocks pure ASCII; line length ≤ 80 in both new test files; `default_station` never used in the guide (§13 constructs a fresh `Station('connected-demo')`) |

## Design compliance (element by element)

### Guide §13 (design §E1–E6)

- [x] **E1 scenario**: objects `A`(1.0)/`B`(10.0)/`C`(100.0)/`D`(0.0),
  shared callback shape (`evolve` = `x + u`, `observe` = `y = x`),
  inputs `{'u': 0.0}`, outputs `['y']`; `drive = MockControl('drive',
  2.0)`; `probe = MockMeasurement('probe', -1.0)`; **fresh**
  `Station('connected-demo')` (never the module-global default);
  `sim_dt = 0.5`. ✔
- [x] **E2 operation sequence**: `add_object` ×4 → `Coordinator(station)`
  → `enroll` **by member name** (`'A'`…`'D'`, the closed Issue-2
  clarification; runtime-probed: passing the object raises `KeyError`) →
  `add_control`/`add_measurement` → `connect` edges 1–5 **in design
  order** → `prepare` → `step` → mid-run `drive.write(3.0)` with the
  readback line → `step` → `reset` → `prepare`+`step` re-run. ✔
- [x] **E3 print formats**: all floats via `:.1f`; `tick` as exact int;
  `prepared` via `str(bool)`; literal A,B,C,D enumeration (no dict
  iteration); two-space indents; every E3 line template present with the
  exact wording. ✔
- [x] **E4 golden trace**: the recorded values match the independently
  derived table cell-for-cell — including the two discriminating cells
  (`C.u=10.0`, **not** 11.0, at 0→1; `C.u=11.0` at 1→2) and the
  reset-restores-`2.0`/`-1.0` row. Re-derived independently in this
  review. ✔
- [x] **E5 recorded block**: byte-equal to my own execution of the §13
  program (genuine executed output, not transcribed). ✔
- [x] **E6 structure**: heading exactly `## 13. Executed connected
  example`; §11-formula intro sentence; exactly one `python` fence and
  one `text` fence; §13 is the last section (file ends with the closing
  fence). ✔

### Guide updates (design G1–G4, G6)

- [x] **G1 intro**: coverage sentence gains the connected topics; the
  stale "(the only public name of the subpackage)" claim is **gone**
  (grep-verified), replaced by the full eight-name export list. ✔
- [x] **G2 §8 pointer**: the "no clock API … in the object" statement is
  kept and one pointer sentence appended (station-owned `sim_dt`,
  coordinator `tick`/`sim_time`, `bind_interval` → §12). ✔
- [x] **G3 §9 deferral resolution** (09m2): the "planned Release 3 work
  … intentionally not stated here" paragraph is replaced by the
  standalone-vs-connected split carrying all three mandatory statements:
  standalone = authoritative input store (R3-TC-07t pins intact);
  connected = pending-source control readback / delivered-sample
  measurement readback; "Both patterns are implemented and both are
  stated here." No "planned"/"not stated here" wording survives
  (grep-verified). ✔
- [x] **G4 §10 exclusions**: reworded to "automatic wall-clock
  advancement and automatic/background multi-object scheduling (explicit
  coordinated stepping with a shared `sim_dt` is covered in §12)"; all
  other exclusions unchanged; the "limitation" token intact (SIM-TC-08a
  green). ✔
- [x] **G5 §12 required statements**: all seven topic rows present in
  design order (composition/ownership, shared explicit clock, delayed
  edges and feedback, mock interaction, interval binding, coordinated
  reset, connected vs standalone), each carrying the mandated
  statements — validation **order** bool→non-real→non-finite/non-positive
  with correct error classes (runtime-probed), mid-run rejection,
  coercion via `float()` with `Decimal` rejected (runtime-probed),
  multiplication clock policy, `S_k` semantics with no same-tick
  propagation, tick-zero measurement initial, busy-guard `RuntimeError`,
  single-driver extension to bindings, external-side-effect limit. ✔
- [x] **G6 honesty guards**: no "equals fresh construction" claim
  (explicitly disclaimed at §12 line ~473); no reads/writes-advance-clock
  claim ("Reads and writes never tick"); no "planned Release 3" wording;
  mock endpoints stated as coordinator-owned ports, **not** devices. ✔
- [x] **§1–§11 preserved heading-for-heading**: the guide diff shows
  hunks only at the intro (G1), §8 tail (G2), §9 paragraph (G3), §10
  bullet (G4) and the appended §12/§13; **no §11 hunk exists** — the
  extraction split literal `## 11. Executed example` is byte-stable and
  SIM-TC-08b is green in the 210-test run. ✔

### arch.md (design A1–A10)

- [x] **A1 Stage-1 header**: Release 3 candidate point recorded with
  merges `6dc4d36`/`0bd97d5`/`2991a63`, dev run 38820136267, candidate
  head `f32b796`, candidate run 38826003029, "210 tests green", and the
  explicit ancestor-certification disclosure; Stage 2 correctly left to
  the integration commit. Header references updated to "Releases 2–3". ✔
- [x] **A2** `tu` row extended with the three corrections, membership/
  builders, and the coordinated-simulation capability ("additive, zero
  new dependencies, no changes outside `tu`"). ✔
- [x] **A3** exports sentence now lists all eight names; the import-graph
  claim is corrected to the true one-way direction
  (simulation-independent-of-station; `station.py:17-24` imports the
  simulation package). ✔
- [x] **A4** "Coordinated simulation (Release 3, implemented)" subsection
  inserted after the bridge paragraph and before "Deliberate exclusions",
  with all seven planned paragraphs including the §2 deferral record. ✔
- [x] **A5** bridge-readback correction appended after the two-gate
  sentence (authoritative input store; cross-reference to the connected
  mode); existing wiring content preserved. ✔
- [x] **A6** exclusions reworded so no excluded item contradicts
  delivered capability; Release 3 remaining exclusions and the deferred
  snapshot/describe node appended. ✔
- [x] **A7** Release 3 paragraph added with the PRD link, the four-task
  summary, the `softlab/tu/` confinement statement, the four added test
  files, and the suite-size/CI anchors. ✔
- [x] **A8** `[PLANNED]` block kept; the exclusions sentence now covers
  Releases 2–3 with the A6 wording; the deferred node joins the tracked
  future-work list with its deferral reference. Verified by enumeration:
  no `[PLANNED]` entry describes delivered R3 capability, and no
  delivered capability is described as absent. ✔
- [x] **A9** traceability table present with all 19 design rows; 32
  anchor endpoints spot-checked against the delivered code (table above)
  — every anchor resolves to the named symbol. ✔
- [x] **A10** link rules: all new-file references are Markdown links with
  existing repo-root-relative targets (09h green over 52 targets);
  file:line citations keep the plain `path:line` form (correctly excluded
  from the path-extension regex by ending in `:N`/`:N-M`); **no link to
  the not-yet-existing `r3-004-test-results.md`** (grep-verified —
  referenced nowhere in arch.md). ✔

### Tests (design T1–T3)

- [x] **T1 mechanics**: `GUIDE_PATH` resolution mirrors
  `test_sim_user_guide.py:18-19`; extraction splits on the literal §13
  heading then the single `python`/`text` fences; `exec` under
  `contextlib.redirect_stdout`; comparison discipline `rstrip('\n')`
  equality plus `endswith('\n')`. ✔
- [x] **09a independence** (test-plan 09a note): the expectation source
  is `_ReferenceRun` — a hand-written semantic model driven by declared
  constants (initials, edges, `SIM_DT`, control/measurement initials)
  implementing the documented coordination semantics; **nothing is
  copied from implementation output**. I re-derived the trace
  independently and it matches both the design's E4 table and the test's
  reference model (including the discriminating `C.u=10.0`-not-`11.0`
  cell). ✔
- [x] **09b non-vacuous**: demonstrated live — editing the recorded
  block fails exactly 09b while 09a stays green; run-1 == run-2
  byte-identity asserted. ✔
- [x] **09h non-vacuous**: demonstrated live — a broken Markdown link
  fails the test naming the path; the non-empty-collection and
  `log/release_3/` anchors are asserted. Note (design-conformant): per
  approved design A10 rule 2, `path:line` citation spans are
  deliberately **not** 09h targets (they end in `:N`/`:N-M` and never
  match the path-extension regex); citation accuracy is therefore
  covered by this reviewer's anchor spot-check (32/32 verified) rather
  than by 09h — exactly as the design specifies. ✔
- [x] **T3**: zero edits to existing test files; the 09g `tests/` diff
  lists exactly the two new paths. ✔
- [x] Style: stdlib-only imports (`contextlib`, `io`, `unittest`,
  `pathlib`, `re`), `unittest` classes, ≤80 columns, English docstrings
  — matches the existing harness conventions. ✔

## Issues found

### [Low] Issue 1: duplicated "Implementation complete" progress entry in sprint-board.md

- **Location**: `log/release_3/sprint-board.md:146-163`
- **Description**: `789e9ca` appended a full "✅ Implementation complete"
  entry (lines 152-163) but left the stale "▶ Implementation (sw-tom) in
  progress next" line in place; `a6bb006` then replaced *that* line with
  a second, shorter "✅ Implementation complete" entry (lines 146-150)
  plus "▶ Independent code review (sw-celeste) in progress next." (line
  151). The record now carries **two overlapping implementation-complete
  entries** sandwiching the review-phase line — the same commits
  (`0c9bd34`, `baa751f`, `f32b796`, `45d4244`), the same CI runs
  (38826003029, 38826175913) and the same 210-test/scope-guard facts are
  recorded twice with slightly different wording.
- **Impact**: process-record hygiene only — no deliverable content is
  affected, and both copies are individually accurate. But the sprint
  board is the release's process ledger; duplicated entries invite
  divergence on future edits and read as an oversight.
- **Recommendation**: keep one entry (the fuller `789e9ca` wording,
  lines 152-163, which also carries the R3-TC-09m6 scope-gate
  disposition), delete the shorter duplicate (lines 146-150), and keep
  "▶ Independent code review …" as the single trailing pointer.
- **Owner**: sw-tom
- **Status**: CLOSED — confirmed by sw-celeste at `3b8e42c`
  (2026-10-16): the shorter duplicate is deleted; exactly one
  "✅ Implementation complete" entry remains in the R3-004 section
  (sprint-board.md:147, the fuller `789e9ca` wording), carrying the
  R3-TC-09m6 scope-gate disposition (line 157); the "▶ Independent code
  review (sw-celeste) in progress next." line is intact (line 146). The
  other three "Implementation complete" lines on the board (49, 77, 110)
  belong to R3-001/R3-002/R3-003 and are untouched. Non-blocking
  cosmetic note: the review pointer now precedes rather than trails the
  implementation entry; content and chronology remain unambiguous, no
   action required.

### [Low] Issue 2: dangling prose reference "the Release 2 guide errata" in arch.md

- **Location**: `arch.md:390` (A4 "R3-001 corrections recap" bullet)
- **Description**: the bullet closes with "…the reset-claim and
  bridge-readback corrections are recorded in the bridge paragraph above
  and the Release 2 guide errata." No record named or describable as
  "Release 2 guide errata" exists anywhere in the repository
  (grep-verified across `arch.md`, `docs/`, `log/release_2/`,
  `log/release_3/` — the only occurrence of "errata" is this line
  itself). Because it is prose rather than a Markdown link, 09h cannot
  catch it (by design, A10 rule 2); the approved design's A4 item 6
  specifies the recap as "malformed callback-key results …
  (`object.py:947-`); reset-claim and bridge-readback corrections
  (§A5)" — i.e. the corrections are recorded in the bridge paragraph
  (A5) and the guide itself, with no "errata" artifact in the design.
- **Impact**: this task's clause-4 acceptance is precisely
  "architecture describes implemented capability only" with claim-level
  traceability; an unresolvable named-reference undermines that
  discipline in the very section that demonstrates it. Severity is low
  (one clause, no behavioral or structural consequence).
- **Recommendation**: replace the dangling phrase with a resolvable
  reference, e.g. "…are recorded in the bridge paragraph above and in
  guide §9 ([docs/user-guide/simulation.md](docs/user-guide/simulation.md))"
  (a Markdown link keeps it inside 09h's mechanical net), or cite the
  R3-001 results document if a process record is intended.
- **Owner**: sw-tom
- **Status**: CLOSED — confirmed by sw-celeste at `3b8e42c`
  (2026-10-16): the dangling phrase is replaced by "…recorded in the
  bridge paragraph above and in guide §9
  ([docs/user-guide/simulation.md](docs/user-guide/simulation.md))"
  (arch.md:390-391) — exactly the recommended resolvable form. The
  token "errata" no longer occurs anywhere in arch.md (grep-verified);
  the link target exists, and as a Markdown link it is inside 09h's
  mechanical net — `tests.test_arch_links` passes over the updated
  arch.md.

## Non-blocking observations (no action required for approval)

1. **09a/09b exercise the example through `exec` of the guide's own code
   block** — the guide code block *is* the executed example (09b identity
   clause satisfied by construction, single source of truth). There is
   no `examples/` copy to drift, per the design's single-source ruling.
2. **The §13 example deliberately omits `bind_interval`** (the declared
   `u` inputs are edge-driven, not interval-bound). Clause-2 coverage is
   complete without it (design Q5, ACCEPT ruling); §12 documents the
   binding contract for readers who need it.
3. **Candidate-vs-integration labeling is precise**: the header says
   "candidate point" and names the Stage-2 reconciliation as an
   integration-time checklist item; no integration fact is claimed
   prematurely (09m3(a) preview clean).

## Quality gates

- [x] No compiler errors/warnings; zero warnings under
  `-W error::Warning` (210 tests)
- [x] Regression: 210 tests OK = 207 pre-existing (byte-untouched) + 3
  new; SIM-TC-08b (§11 extraction split) green
- [x] Compile (`compileall -q softlab`) and import smoke clean; version
  `0.3.0`; eight-name export identity holds
- [x] `git diff --check` clean; scope confined to the four intended
  areas (guide, arch.md, two new test files) plus `log/` records;
  production diff EMPTY; packaging untouched
- [x] TDD red verified at `0c9bd34` (2 extraction errors + 1 assertion
  failure, exactly the documented red mode for a docs-delivery task)
- [x] Recorded output proven genuine by independent re-execution
  (byte-for-byte), not trusted from the diff
- [x] CI citations verified against live runs (38826003029 @ `f32b796`,
  38826175913 @ `45d4244`, 38820136267 @ `2991a63` — all `success`,
  both matrix jobs); Issue-1 fill-commit convention honored
- [x] No new dependencies; stdlib + the delivered public API only, per
  the design's empty dependency table

## Simplicity audit

- [x] Zero third-party dependencies introduced (design table empty;
  diff confirms).
- [x] No over-engineering: exactly one worked example in exactly one
  place; two ticks (not three) per the design's completeness argument;
  the test reference model is the smallest semantic model that derives
  the trace.
- [x] No entity without justification: the two new test files map 1:1
  to the two acceptance surfaces (guide reproduction, arch reference
  integrity); the Release 2 harness is deliberately untouched rather
  than extended.
- [x] Data structures minimal: `_ReferenceRun` is a plain state holder
  with four operations; no abstraction beyond the design's T1 sketch.

## Approval

- [x] All issues resolved — **2 CLOSED** (both Low, both record-hygiene;
  fixed in `3b8e42c`, confirmed by this reviewer 2026-10-16)
- [x] Design faithfully implemented (element-for-element: §E, G1–G6,
  A1–A10, T1–T3, fill-commit convention)
- [x] Test plan faithfully realized (09a/09b/09h automated cases
  non-vacuous and live; 09c–09g gates green; manual-check previews
  09m1/09m2/09m3 clean)
- [x] Code meets quality and simplicity standards
- [x] **APPROVED** — re-confirmed 2026-10-16 at `3b8e42c` (in sync with
  `origin/codex/r3-004-delivery`). The original round ended
  CHANGES-REQUESTED on two Low record-hygiene items; both are fixed
  exactly as recommended. This task may proceed to sw-mike's testing
  phase.

## Re-confirmation record (2026-10-16, fix commit `3b8e42c`)

Fix range: `940bfd8..3b8e42c` — touches `arch.md` (+2/−1) and
`log/release_3/sprint-board.md` (−5) only; no production, test, or
packaging surface. All commands from the repository root using the
project `.venv`:

| Command | Result |
| --- | --- |
| `git fetch origin`; HEAD check | HEAD `3b8e42c`, in sync with `origin/codex/r3-004-delivery`, working tree clean |
| `git diff 940bfd8..3b8e42c` | exactly the two recommended fixes, nothing else |
| Issue 1: board inspection | single R3-004 "Implementation complete" entry (line 147, fuller `789e9ca` wording, 09m6 disposition line 157); review pointer intact (line 146); duplicate gone |
| Issue 2: `grep -n 'errata' arch.md` | no matches — dangling reference eliminated |
| Issue 2: link target | `docs/user-guide/simulation.md` exists; arch.md:390-391 carries the Markdown link |
| `.venv/bin/python -m unittest tests.test_arch_links` | 1 test OK (R3-TC-09h green over the updated arch.md) |
| `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'` | **Ran 210 tests, OK** |
| `.venv/bin/python -W error::Warning -m unittest discover -s tests -p 'test_*.py'` | **OK, zero warnings** |
| `.venv/bin/python -m compileall -q softlab`; import smoke | exit 0; version `0.3.0` |
| `git diff --check` | clean |
