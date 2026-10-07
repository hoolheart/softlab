# R3-001 test results — Release 2 gap corrections and closure reconciliation

Tester: sw-mike | Date: 2026-10-07
Branch: `codex/r3-001-corrections` (HEAD `08955cd`, synced with
`origin/codex/r3-001-corrections`; execution adds only this record)
Plan: `log/release_3/test/r3-001-test-plan.md` (Revision 2, approved)
Requirements: `log/release_3/prd.md` (R3-AC-07; standalone portion of R3-AC-08)
Implementation under test: commits `abc6cea..3210a35` (implementation
`148959b`..`a26608b`, code review APPROVED at `90d5bca`)

## Verdict

**PASS — all 21 planned case IDs pass (15 automated + 6 manual record
checks). Full suite 147 tests, OK, exit code 0, zero warnings (including
under the `-W error::Warning` gate).** Nothing below is marked passed that
was not run. CHK-08-1 (scope gate) is reviewer-owned and was discharged by
the code review at `90d5bca`; the tester independently re-verified its
evidence (section "Scope gate", below).

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env, per AGENTS.md) |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |
| New test dependencies | none (stdlib + NumPy only) |
| Unverifiable items | none |

## Commands and verbatim results

### 1. unittest full suite (R3-TC-07a–07m, R3-TC-08a–08c)

Command:

```
python -m unittest discover -s tests -p 'test_*.py' -v
```

Result (final confirmation run):

```
----------------------------------------------------------------------
Ran 147 tests in 0.496s

OK
Connected to sqlite3 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmpvaeo65xa/readings.db.
Connected to HDF5 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmpvaeo65xa/readings.hdf5.
```

- Test count: **147** (baseline 135 + 12 new R3-001 tests: 4 bridge
  readback integration + 6 malformed callback keys + 2 reset
  characterization), all 147 verbose lines end in `... ok`.
- Result: **OK**, exit code **0**.
- The two backend lines are informational prints from the data-backend
  tests, not warnings.

### 2. Warning gate (R3-TC-08b)

Command:

```
python -W error::Warning -m unittest discover -s tests -p 'test_*.py'
```

Result: **Ran 147 tests … OK**, exit code **0** — every warning class is
an error under this gate, so a green run proves zero warnings. No
`DeprecationWarning`/`UserWarning`/other warning fired; no Release 1 debt
warning (OBS-004/005/006, DEFECT-2) fired — none observed, nothing waived
silently.

### 3. compileall (R3-TC-08c)

Command: `python -m compileall -q softlab`
Exit code **0**, no output.

### 4. import smoke (R3-TC-08c)

Command: `python -c "import softlab; print(softlab.__version__)"`
Output: `0.3.0`, exit code **0**.

### 5. Import identity (R3-TC-08c)

Command:

```
python -c "
import softlab.tu.simulation as s
import softlab.tu.simulation.object as o
assert s.SimulatedObject is o.SimulatedObject
import softlab; print('import identity OK, version', softlab.__version__)"
```

Output: `import identity OK, version 0.3.0` — public class identity holds;
no circular-import error.

### 6. No new required dependencies (R3-TC-08d)

Command: `git diff dev...HEAD -- pyproject.toml setup.py | wc -l`
Output: `0` — packaging metadata untouched; optional extras untouched.

### 7. Scope gate (CHK-08-1, reviewer-owned — discharged at `90d5bca`)

Tester-side independent re-verification:
`git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui softlab/mu`
→ empty, exit 0.
`git diff dev...HEAD --stat -- softlab/` →
`softlab/tu/simulation/object.py | 26 ++++++++++++++++++--------`
(1 file, +18/−8) — production runtime changes confined to
`softlab/tu/simulation/object.py`, exactly as the design's scope guard
states. **Reviewed evidence; PASS.**

## Per-case results

| Case ID | Method | Verdict | Evidence |
| --- | --- | --- | --- |
| R3-TC-07a | automated | PASS | `tests/test_tu_simulation_integration.py` `BridgeReadbackCorrectionTests.test_r3_tc_07a_readback_follows_direct_input_changes` |
| R3-TC-07b | automated | PASS | `test_r3_tc_07b_readback_follows_reset` |
| R3-TC-07c | automated | PASS | `test_r3_tc_07c_readback_follows_second_controller` |
| R3-TC-07d | automated | PASS | `test_r3_tc_07d_rejected_set_keeps_stores_consistent` |
| R3-TC-07e | automated | PASS | `tests/test_tu_simulation.py` `MalformedCallbackKeyTests.test_r3_tc_07e_mixed_extra_keys_evolve_value_error` |
| R3-TC-07f | automated | PASS | `test_r3_tc_07f_mixed_extra_keys_observe_value_error` |
| R3-TC-07g | automated | PASS | `test_r3_tc_07g_single_non_string_extra_key_named` |
| R3-TC-07h | automated | PASS | `test_r3_tc_07h_pure_string_key_discrepancies_named` |
| R3-TC-07i | automated | PASS | `test_r3_tc_07i_non_mapping_results_remain_type_error` |
| R3-TC-07j | automated | PASS | `test_r3_tc_07j_failed_evolve_atomicity_under_malformed_keys` |
| R3-TC-07k | automated (regression) | PASS | Existing deterministic fixtures re-ran green in the 147-suite: `test_sim_tc_04a_reset_restores_initial_condition` and `test_sim_tc_05i_failure_atomicity_reset` both `... ok` |
| R3-TC-07l | automated | PASS | `ResetRestorationCharacterizationTests.test_r3_tc_07l_reset_reinvokes_factory_and_tracks_it` |
| R3-TC-07m | automated | PASS | `test_r3_tc_07m_failed_reset_preserves_stores_and_identity` |
| R3-TC-07n | manual record check | PASS | Reset wording: see §"R3-TC-07n — reset documentation wording" below |
| R3-TC-07o | manual record check | PASS | `gh run view` of both CI runs: see §"R3-TC-07o" below |
| R3-TC-07p | manual record check | PASS | Board line 3 vs acceptance verdict: see §"R3-TC-07p" below |
| R3-TC-07q | manual record check | PASS | Baseline `f04e783` + ancestor explanation: see §"R3-TC-07q" below |
| R3-TC-07r | manual record check | PASS | All 20 cited commits exist with matching messages: see §"R3-TC-07r" below |
| R3-TC-07s | manual record check | PASS | No invented promotion evidence: see §"R3-TC-07s" below |
| R3-TC-07t | manual record check | PASS | Guide §9 corrected wording: see §"R3-TC-07t" below |
| R3-TC-08a | automated (baseline) | PASS | Pre-implementation 135-test re-run recorded in `r3-001-baseline.md` (executed 2026-10-06, before the implementation landed) |
| R3-TC-08b | automated | PASS | Section "1" (147 tests OK), "2" (warning gate), "3" (compileall), "4" (import smoke) |
| R3-TC-08c | automated | PASS | Sections "1", "5"; all pre-existing `tests/test_tu_*.py` suites pass unmodified except the SIM-TC-06a fixture rebind documented in the approved plan |
| R3-TC-08d | automated | PASS | Section "6" — zero packaging-metadata changes |

## R3-TC-07n — reset documentation wording (manual)

Checklist verified against the design's drafted sentences
(`log/release_3/design/r3-001-detailed-design.md` "Exact wording
corrections"):

- **Site C-1** — `softlab/tu/simulation/object.py:659–666` (`reset()`
  docstring, success claim): "On success the owned input and state stores
  are restored from their declared sources … This is a restoration of
  owned stores only, not an equivalence to fresh construction: a factory
  with external state may return a different value on each invocation,
  and reproducible behavior additionally requires deterministic callbacks
  and reproducible factories." — matches design C-1 verbatim in substance.
- **Site C-2** — `softlab/tu/simulation/object.py:668–680` (failed-reset
  claim): "… leaves the owned inputs and states exactly as they were
  before the call. Subsequent observations are therefore unchanged under
  a deterministic ``observe``; a stateful ``observe`` follows its own
  external state, which is outside this guarantee." — matches design C-2;
  surrounding sentences (injection point, no-rollback rationale, external
  side effects) unchanged.
- **Site C-3** — `docs/user-guide/simulation.md:131–136` (§5): "On success
  the owned input and state stores are restored from their declared
  sources. This is not an equivalence to fresh construction: a factory
  with external state may return a different value on each reset, and
  reproducible observations require deterministic callbacks and
  reproducible factories." — matches design C-3.
- **Site C-4** — `docs/user-guide/simulation.md:145–148` (§5 failed-reset
  paragraph): appended sentence "Subsequent observations are unchanged
  under a deterministic `observe`; a stateful `observe` follows its own
  external state, which is outside this guarantee." — matches design C-4;
  pre-existing owned-store wording retained.
- **Site C-5** — `arch.md:277–283`: "restoring the owned input and state
  stores from their declared sources; no equivalence to fresh construction
  is claimed, since factories with external state may return different
  values per invocation and reproducible observations require
  deterministic callbacks and reproducible factories." — matches design C-5.
- **Site C-6** — `arch.md:291–297`: "so a failed `set_input`,
  `evolve_once` or `reset()` leaves the owned inputs and states exactly
  as before (observations are therefore unchanged under a deterministic
  `observe`), and the object remains fully usable after a failed
  `reset()` (a later `reset()` may succeed)." — matches design C-6.
- Repo-wide sweep: `grep -rn "behaviorally identical" softlab/ docs/
  arch.md log/release_2/` — zero hits in production code, guide, arch.md
  or Release 2 records; the only remaining hits are baseline/design/review
  documents quoting the *old* text (historical records, correctly left
  untouched per the no-retroactive-repair guardrail).

No unqualified equivalence claim survives; failed-reset wording promises
owned stores only and states the deterministic-`observe` condition;
reproducible-observation condition (deterministic callbacks +
reproducible factories) stated in all three documents. **PASS.**

## R3-TC-07t — guide §9 bridge wording (manual)

- **(a) Corrected read-back sentence** — `docs/user-guide/simulation.md:240–255`:
  the input-parameter wiring block now includes
  `before_get=lambda stored: sim.get_input('u')` (line 245), and the text
  states "`Parameter.set` order is permission → validate → decode →
  `before_set` → store → `after_set`; `Parameter.get` order is permission
  → `before_get` → encode → return. Exactly one `set_input` per parameter
  set; no evolution occurs on set or read. Parameter read-back via `get()`
  returns the object's **authoritative input store** (`before_get`'s
  return replaces the stored value), so the read reflects the object's
  current input after direct `set_input` changes, `reset()` and writes
  through another controller sharing the same object." The old overpromise
  ("equals the object's current (pending) input") is gone.
- **(b) Standalone vs connected** — `docs/user-guide/simulation.md:257–263`:
  "This section documents the standalone pattern: every control read
  follows the object's authoritative input store. A coordinated
  multi-participant simulation (planned Release 3 work) defines different,
  pending-source and delayed-edge readback semantics for connected
  participants; those semantics are specified by that work and are
  intentionally not stated here." — the connected half is deferred to
  R3-003 without stating its semantics.
- **(c) Conditional re-verification** — the approved design kept the wiring
  pattern unchanged in mechanism (extended closures, no production wiring
  change), so the §6 error table (guide lines 208–213: `ValueError`
  naming the discrepancy for missing/extra keys, `TypeError` if not a
  mapping) and the two-gate validation asymmetry paragraph (lines 265–274)
  were re-verified against the implemented order in
  `softlab/tu/station/parameter.py:396–409`: `set()` is permission
  (`settable` check) → `validator.validate` → decode → `before_set` →
  store → `after_set`; `get()` is permission → `before_get` (return
  replaces stored value) → encode → return. Guide claims match the
  implementation exactly; no stale ordering or error-class claim survives.
  The mixed-keys row is covered by the table's generic "`ValueError`
  naming the discrepancy" row, now actually true for mixed-type keys.

**PASS** — each checklist item verified with file/line citations; nothing
silently waived.

## R3-TC-07o — CI evidence references (manual)

Commands: `gh run view 37298431248 --json headBranch,headSha,conclusion,jobs`
and `gh run view 37298623628 --json headBranch,headSha,conclusion,createdAt`.

- Run **37298431248**: `headBranch=codex/sim-001-simulation-foundation`,
  `headSha=cd90ff69…`, `conclusion=success`; both matrix jobs
  (`compatibility (3.13)`, `compatibility (3.9)`) `conclusion=success`.
  Matches the corrected board/summary/addendum statements (pre-merge,
  task tip `cd90ff6`).
- Run **37298623628**: `headBranch=dev`, `headSha=9408c79b…`,
  `conclusion=success`, `createdAt=2026-10-05T10:45:40Z` — i.e. the
  post-merge `dev` run. Matches the corrected records' distinction.

The records (`sprint-board.md` line 10, `sprint_summary.md` line 22,
`acceptance.md` addendum) now distinguish the two runs exactly as
verified. **PASS.**

## R3-TC-07p — board acceptance status vs acceptance verdict (manual)

- `log/release_2/acceptance.md` verdict (unchanged historical text):
  "Release 2 is accepted; `main` promotion is unblocked from the Product
  Owner side. **Signed**: sw-camille, Product Owner"; verdict commit
  `c69272a` = `docs(log): issue release 2 acceptance verdict`.
- `log/release_2/sprint-board.md` line 3 (corrected): "release acceptance
  ACCEPTED at c69272a (2026-10-05); main promotion unrecorded/pending —
  a separate user decision".
- Board merge row: SIM-001 Done at merge commit `9408c79`
  (`feat(tu): integrate simulation foundation (SIM-001)`).

Board agrees with the committed acceptance record and still distinguishes
acceptance from `main` promotion. **PASS.**

## R3-TC-07q — sprint summary baseline reference (manual)

Commands: `git log --oneline f04e783 -1` →
`f04e783 chore(log): integrate release 2 simulation preparation`;
`git merge-base --is-ancestor 80b0b05 f04e783` → exit 0
("80b0b05 IS ancestor of f04e783"); `sim-001-baseline.md` line 4 confirms
"Branch: `codex/sim-001-simulation-foundation` (from `dev` @ `f04e783`)".

`sprint_summary.md` line 4 (corrected): "Baseline: `f04e783` (task branch
created from dev @ `f04e783` per sim-001-baseline.md; `80b0b05` is an
earlier dev ancestor)". Both facts verified. **PASS.**

## R3-TC-07r — cited commits exist with matching messages (manual)

All 20 hashes cited across `sprint-board.md`, `sprint_summary.md` and
`acceptance.md` were resolved with `git log --oneline -1 <hash>`; every
one exists with a message matching its record description:

```
0aa3908 docs(log): specify SIM-TC-05i per approved design
313ae9f docs(log): review SIM-001 test plan implementability
3d6cb13 docs(log): review SIM-001 detailed design
470e2c7 docs(log): inspect release 2 completion principles
4eab94d feat(tu): add simulated-object foundation package
5caabc7 docs(log): revise SIM-001 test plan per implementability review
670523b docs(arch): fix release 2 test log path
7ccac0d docs(log): add SIM-001 detailed design
80b0b05 docs(log): release 1 sprint summary
86cddb4 docs(log): add SIM-001 baseline record and test plan
88c140d docs(log): add SIM-001 test results
917cfd7 docs(log): close SIM-001 design review clarifications
9408c79 feat(tu): integrate simulation foundation (SIM-001)
c69272a docs(log): issue release 2 acceptance verdict
cd90ff6 docs(log): inspect SIM-001 task completion principles
d52217c docs(arch): record release 2 simulation implementation
dfa535c docs(log): review SIM-001 implementation
e25a416 test(tu): add simulation contract tests
f04e783 chore(log): integrate release 2 simulation preparation
f1bbcd2 docs(log): close SIM-001 test plan review issues
```

(The two run IDs 37298431248/37298623628 are Actions runs, verified in
R3-TC-07o, not commits.) **PASS.**

## R3-TC-07s — no invented promotion evidence (manual)

- `git log main..dev --oneline | wc -l` → **200** — `dev` is 200 commits
  ahead of `main`, and the list includes all Release 2 content
  (`c69272a`, `9408c79`, `cd90ff6`, …): Release 2 content is on `dev`
  but has **not** been merged to `main`.
- `git branch --contains c69272a` lists `codex/r3-001-corrections`, `dev`
  and their origin trackers — **`main` is not among them**; `main`'s tip
  is `f288b2f fix: support Python 3.13 in the development environment`.
- Record sweep: board line 3 states "main promotion unrecorded/pending";
  `sprint_summary.md` reconciliation note states "`main` promotion is
  unrecorded"; `acceptance.md` addendum states "no `main` promotion is
  recorded or implied" (and its historical verdict text correctly claims
  only that promotion is "unblocked", not performed). No record anywhere
  claims a `dev`→`main` promotion occurred.

Acceptance and promotion stay distinguished in every corrected record.
**PASS.**

## Per-AC coverage summary

| AC | Verdict | Covered by |
| --- | --- | --- |
| R3-AC-07 (bridge readback) | PASS | R3-TC-07a–07d (automated) + R3-TC-07t (manual §9 wording) |
| R3-AC-07 (malformed callback keys) | PASS | R3-TC-07e–07j (automated) |
| R3-AC-07 (reset claims) | PASS | R3-TC-07k–07m (automated) + R3-TC-07n (manual wording) |
| R3-AC-07 (closure evidence) | PASS | R3-TC-07o–07s (manual record checks) |
| R3-AC-08 (standalone portion) | PASS | R3-TC-08a–08d (automated) + CHK-08-1 (reviewer-gate evidence, section "7") |

## Key behavioral pins recorded during execution

- **Corrected bridge readback (R3-TC-07a–07d):** with the documented
  `before_get=lambda stored: sim.get_input('u')` wiring, control reads
  equal the object's authoritative input after direct `set_input`,
  after `reset()`, and after a second controller's write; a rejected
  set (`ValNumber` on `'not-a-number'`) leaves both stores at 1.0 —
  validator fires before the bridge, no partial write.
- **Mixed-key errors (R3-TC-07e–07j):** mixed str/non-str extra keys in
  `evolve` and `observe` results raise the documented `ValueError` (not
  the incidental sorting `TypeError`); single non-str and pure-string
  discrepancies keep named `ValueError`s; non-mapping results remain
  `TypeError`; failed evolve under malformed keys leaves committed state
  bit-identical (build-then-swap survives the fix).
- **Reset semantics (R3-TC-07k–07m):** deterministic fixtures keep
  passing; each `reset()` re-invokes each factory exactly once and the
  committed input tracks the factory's *current* result (R3-TC-07l,
  factory call counter — pins restoration-from-source, not fresh-
  construction equivalence); a factory armed to raise propagates the
  identical exception object (`assertIs`), leaves owned stores exactly as
  before, and a later `reset()` succeeds (R3-TC-07m).

## Warnings observed

**None.** Under `python -W error::Warning` the suite is green (147 tests,
OK, exit 0), which is only possible with zero emitted warnings. No
pre-existing Release 1 debt warning (OBS-004/OBS-005/OBS-006 or
DEFECT-2) fired. No waiver, silent or otherwise, was applied.

## CI reference (integration gate, per AGENTS.md)

Local evidence above was produced on Python 3.13.15. Integration to
`dev` additionally requires the candidate-commit CI
(`.github/workflows/ci.yml`: unittest regression, compile and import
checks on Ubuntu, Python 3.9/3.13 matrix) to pass; that CI run is an
integration-gate step and is **not** claimed as passed here — no CI
result is marked green before it exists.

## Issues found

None. No production-code defects were observed during this execution; no
bug reports to sw-tom arise from this run.
