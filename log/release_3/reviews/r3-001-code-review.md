# Code Review — R3-001: Release 2 gap corrections and closure reconciliation

Reviewer: sw-celeste | Date: 2026-10-06
Branch: `codex/r3-001-corrections` | Range reviewed:
`abc6cea..f28bc88` (implementation commits `148959b`, `7c63685`,
`7657eda`, `bf7ee44`, `a26608b`; phase-record commit `f28bc88` touches
`log/release_3/sprint-board.md` only)
Binding specification:
[r3-001-detailed-design.md](../design/r3-001-detailed-design.md)
(APPROVED at `24f129f`, minors closed at `88ca37d`) | Test plan:
[r3-001-test-plan.md](../test/r3-001-test-plan.md) (Revision 2) |
Baseline: [r3-001-baseline.md](../test/r3-001-baseline.md)

## Verdict: APPROVED

- **Total issues: 0** (critical: 0, high: 0, medium: 0, low: 0)
- The single documented deviation (SIM-TC-06a test-docstring wording
  neutralized to "authoritative input store", recorded in `7657eda`)
  was verified: the assertion `drive() == 1.0` and every other
  assertion in the test are untouched; the wording now matches the
  corrected contract. Approved as recorded.

## What was executed (commands + results)

All commands from the repository root using the project `.venv`
(Python 3.13), matching AGENTS.md verification §1:

| Command | Result |
| --- | --- |
| `git fetch origin` + `git reset --hard origin/codex/r3-001-corrections` | HEAD at `f28bc88`, up to date |
| `git diff --stat abc6cea..f28bc88` | 9 files: `softlab/tu/simulation/object.py` (fix + C-1/C-2 docstrings), `tests/test_tu_simulation.py` (+171), `tests/test_tu_simulation_integration.py` (+90/−5), `docs/user-guide/simulation.md` (§5 C-3/C-4, §9 wiring + readback), `arch.md` (C-5/C-6), `log/release_2/{acceptance,sprint-board,sprint_summary}.md` (REC-1/2/3), `log/release_3/sprint-board.md` (phase record) — exactly the design's file-level change list |
| `git diff abc6cea..f28bc88 --stat -- pyproject.toml setup.py softlab/huo softlab/jin softlab/shui softlab/mu` | **empty** — no dependency/config changes; production changes confined to `softlab/tu` (CHK-08-1 satisfied) |
| `git diff abc6cea..f28bc88 --check` | clean, exit 0 |
| `awk 'length > 80 …'` over `object.py` and both test files | no Python line exceeds 80 chars |
| `python -m compileall -q softlab` | exit 0 |
| Import smoke + class identity `softlab.tu.simulation.SimulatedObject is softlab.tu.simulation.object.SimulatedObject` | pass, version `0.3.0` (R3-TC-08c pattern) |
| `python -m unittest discover -s tests -p 'test_*.py'` | **Ran 147 tests, OK** (135 baseline + 12 new) |
| `python -W error::Warning -m unittest discover -s tests -p 'test_*.py'` | **Ran 147 tests, OK, zero warnings** |
| Baseline defect re-execution (baseline `object.py` extracted via `git show abc6cea:` into a scratch dir outside the repo) | mixed-key evolve raises incidental `TypeError: '<' not supported between instances of 'str' and 'int'` — the R3-TC-07e defect is real and the fix addresses the root cause |
| Vacuous-pass check (baseline wiring *without* `before_get`, scratch script) | R3-TC-07a scenario reads `1.0` (expects 5.0), 07b reads `3.0` (expects 0.0), 07c reads `1.0` (expects 2.0) — all three would genuinely **fail** without the fix; the new tests are not vacuous |
| Byte-pin verification (scratch, pre-fix vs post-fix messages) | single non-string extras render byte-identically: `extra [2]`, `extra [None]`, `extra [('a',)]`; mixed `{'z', 2}` now renders deterministically as `extra ['z', 2]` (apostrophe U+0027 precedes digit U+0032), matching the design's analysis |
| `git log --oneline -1` on `c69272a`, `cd90ff6`, `9408c79`, `f04e783`, `80b0b05` | all exist with messages matching the records' descriptions |
| `gh run view 37298431248` / `gh run view 37298623628` | run 37298431248: branch `codex/sim-001-simulation-foundation`, head `cd90ff6`, success, created 2026-10-05T10:43:54Z (pre-merge); run 37298623628: branch `dev`, head `9408c79`, success (post-merge) — the reconciled records cite both correctly |
| `git log main..dev --oneline` | no `main` promotion exists; no record claims one (R3-TC-07s) |

## Design compliance

- [x] **Area 2 fix** (`object.py:780–791`): exactly the designed logic —
  `missing = sorted(expected - actual)` unchanged; explicit
  `all(isinstance(key, str) for key in extra)` check; `sorted(extra)`
  for the all-string path (byte-identical documented rendering);
  `sorted(extra, key=repr)` fallback; message format unchanged
  (`missing {missing}, extra {rendered_extra}`). No helper extracted,
  per the design's implementation note. The non-mapping `TypeError`
  (lines 772–775) still fires **before** any key handling
  (R3-TC-07i). The `ValueError` raises inside
  `_validate_callback_result`, before `evolve_once`'s commit — the
  build-then-swap contract is structurally untouched (R3-TC-07j).
- [x] **Area 1 bridge fix**: zero production code, as designed. The
  `before_get=lambda stored: obj.get_input('u')` closure is added to
  the guide §9 wiring block, the `build_bridge` fixture, the SIM-TC-06a
  fixture and the new R3-TC-07a–07d tests. The pinned `Parameter.set`
  order is untouched; no validator runs on the get path.
- [x] **Area 3 docstring corrections**: sites C-1 (object.py reset
  docstring, success claim) and C-2 (failed-reset claim) are
  byte-faithful to the design's drafted replacement sentences;
  surrounding sentences (injection point, no-rollback rationale,
  external side effects) unchanged.
- [x] **Guide corrections**: C-3 (§5 success claim) and C-4 (§5
  failed-reset appended sentence) byte-faithful; §9 wiring block gains
  the `before_get` line; the read-back sentence is replaced by the
  designed text stating the **authoritative input store** semantics;
  the new **Standalone vs connected readback** paragraph defers
  connected semantics to planned Release 3 work without stating them
  (R3-TC-07t(b)). The two-gate paragraph (now lines 265–275) and the
  §6 error table are unchanged — R3-TC-07t(c) conditional check passes,
  as the design predicted.
- [x] **arch.md corrections**: C-5 (line 279 area) and C-6 (line 291
  area, architect-APPROVED scope extension) byte-faithful to the
  designed replacements.
- [x] **Area 4 reconciliation**: REC-1 (board line 10 + summary line 22
  CI references now distinguish pre-merge 37298431248 @ `cd90ff6` from
  post-merge 37298623628 @ `9408c79`), REC-2 (board line 3 header:
  ACCEPTED at `c69272a`, `main` promotion unrecorded/pending), REC-3
  (summary line 4 baseline corrected to `f04e783` with `80b0b05`
  explained as an earlier dev ancestor). Each record carries the
  designed dated reconciliation note (2026-10-06, R3-001). The
  acceptance.md verdict text is untouched — the correction is a pure
  append-only addendum. Acceptance and promotion are distinguished in
  every corrected record; nothing is invented (all references
  independently re-verified above).

## Test-plan traceability (walked against the code)

| Case | Implemented? | Basis |
| --- | --- | --- |
| R3-TC-07a | ✓ | `test_r3_tc_07a_readback_follows_direct_input_changes`: `sets == [1.0]` pins exactly one `set_input` per set; read-after-direct-`set_input` returns 5.0; `calls['evolve'] == 0` pins no evolution on set/read. Vacuous-pass check: fails without `before_get` |
| R3-TC-07b | ✓ | `test_r3_tc_07b_readback_follows_reset`: evolve-away + `drive(3.0)` + `reset()` → read 0.0; `calls['evolve'] == 1` pins reset evolving nothing. Fails without the fix |
| R3-TC-07c | ✓ | `test_r3_tc_07c_readback_follows_second_controller`: both controllers read 2.0, matching `obj.get_input('u')`. Fails without the fix |
| R3-TC-07d | ✓ | `test_r3_tc_07d_rejected_set_keeps_stores_consistent`: `TypeError` from `ValNumber` before the bridge; both stores stay 1.0; no evolution |
| R3-TC-07e | ✓ | mixed `{'z', 2}` extra on evolve → `ValueError` naming `'z'` and `2`; committed state unchanged. **Not** byte-pinned on ordering, per design/test-plan risk 3. Baseline re-execution confirms it detects the incidental `TypeError` defect |
| R3-TC-07f | ✓ | mixed extra on observe → `ValueError` naming `'z'`; states and inputs untouched |
| R3-TC-07g | ✓ | single non-string extra → `extra [2]` asserted; verified byte-identical to the pre-fix rendering |
| R3-TC-07h | ✓ | pure-string missing (`'x'`) and extra (`'z'`) named in `ValueError` |
| R3-TC-07i | ✓ | list result on evolve and int result on observe → `TypeError` "must return a mapping" |
| R3-TC-07j | ✓ | armed second evolution raises `ValueError`; committed state bit-identical to snapshot; a later valid evolution succeeds (usability) |
| R3-TC-07k | ✓ | SIM-TC-04a/05i re-run as part of the suite — 147 tests OK |
| R3-TC-07l | ✓ | factory call counter: 1 at construction, +1 per reset; committed input tracks the factory's current result (10.0 → 20.0 → 30.0) |
| R3-TC-07m | ✓ | armed factory raises on reset: `assertIs` exception identity, `__cause__ is None`, inputs/states equal the pre-reset snapshot, later reset succeeds and observations resume |
| R3-TC-07n | ✓ (manual, this review) | no unqualified "behaviorally identical to a newly constructed one" remains; failed-reset wording promises owned inputs/states only and states the deterministic-`observe` condition in all three documents |
| R3-TC-07o | ✓ (manual, this review) | all cited hashes exist with matching messages |
| R3-TC-07p | ✓ (manual, this review) | `gh run view` re-verifies both runs; records distinguish pre-merge from post-merge |
| R3-TC-07q | ✓ (manual, this review) | board header reconciled with ACCEPTED at `c69272a`; promotion still unrecorded |
| R3-TC-07r | ✓ (manual, this review) | summary baseline corrected to `f04e783` with `80b0b05` explained |
| R3-TC-07s | ✓ (manual, this review) | `git log main..dev` shows no promotion; no record claims one |
| R3-TC-07t | ✓ (manual, this review) | (a) §9 states authoritative-input-store readback, overpromise gone; (b) standalone vs connected paragraph defers to R3-003 without stating connected semantics; (c) two-gate paragraph and §6 error table verified unchanged and still accurate against the implemented `Parameter.set` order |
| R3-TC-08a–08d | ✓ | 147 tests OK; compileall exit 0; import smoke + class identity pass; `pyproject.toml`/`setup.py` untouched |
| CHK-08-1 | ✓ (reviewer-owned) | production diff confined to `softlab/tu/simulation/object.py`; recorded here for the test-results document |

## Assertion-integrity audit (full `tests/` diff)

- [x] No existing assertion weakened or removed. The only modification
  to a pre-existing test is SIM-TC-06a: the fixture gains the designed
  `before_get` closure and the docstring wording is neutralized (the
  documented deviation in `7657eda`); every assertion — `sets ==
  [1.0]`, `calls['evolve'] == 0`, `obj.get_input('u') == 1.0`,
  `drive() == 1.0`, the validator-rejection block — is intact.
- [x] `build_bridge` gains only the `before_get` line; SIM-TC-06b/06c
  and the huo-integration assertions are untouched and still pass.
- [x] No byte-pinning of the mixed-key rendering ordering anywhere
  (07e/07f assert membership of key names, not order) — per plan risk 3.

## Conventions (AGENTS.md)

- [x] English docstrings in the repo style on all new test classes and
  methods, citing the R3-TC case IDs
- [x] Production change keeps the existing type-annotated,
  docstring-complete style of `object.py`; no signature changed
- [x] Black line width 80 on all touched Python files (verified
  mechanically)
- [x] Validators signal failure by raising — untouched
- [x] No new dependencies; no `pyproject.toml`/`setup.py` change
- [x] No unrelated changes; Release 1 debt (OBS-004/005/006, DEFECT-2)
  untouched
- [x] No compiler or lint warnings (`compileall` clean; suite clean
  under warnings-as-errors)

## Simplicity audit

- [x] No third-party dependency added — the fix uses only stdlib
  features and the existing `Parameter.before_get` hook, per the
  design's dependency table (empty)
- [x] No over-engineering: the production fix is the designed 4-line
  inline check; no helper extracted (explicitly recommended against in
  the design); no bridge support class introduced — the existing
  documented hook carries the whole Area 1 fix
- [x] Data structures unchanged; no new public API surface
- [x] Test fixtures reuse `make_accumulator` and plain closures; no new
  test infrastructure

## Quality gates

- [x] No compiler errors / warnings
- [x] Regression: 147 tests OK (135 baseline + 12 new), zero warnings
  under `-W error::Warning`
- [x] Import smoke and class-identity check
- [x] CHK-08-1 verified by `git diff` inspection (this reviewer's
  checklist item — recorded here for the test-results record)

## Approval

- [x] Implementation faithful to the approved detailed design (only the
  documented `7657eda` deviation, verified assertion-intact)
- [x] Implementation faithful to the approved test plan (Revision 2)
- [x] Code meets quality and simplicity standards
- [x] **APPROVED — ready for testing by sw-mike** (R3-TC execution
  phase). CHK-08-1 is discharged by this review and should be recorded
  as verified in the test-results document.
