# Review — R3-001 detailed design

Reviewer: sw-jerry (Software Architect) | Date: 2026-10-06
Verdict: **APPROVED**
Reviewed artifact:
[r3-001-detailed-design.md](../design/r3-001-detailed-design.md) at commit
`46a17fe` on `codex/r3-001-corrections` (verified via `git branch
--show-current` / `git log` after syncing to
`origin/codex/r3-001-corrections`).
Requirements: [prd.md](../prd.md) (R3-AC-07; standalone portion of R3-AC-08) |
Planned architecture: [architecture-plan.md](../architecture-plan.md)
(§"Scoped Release 2 correction and documentation") and
[arch.md](../../../arch.md) | Approved test plan:
[r3-001-test-plan.md](../test/r3-001-test-plan.md) (Revision 2) |
Characterization baseline: [r3-001-baseline.md](../test/r3-001-baseline.md)

## Issues

No blockers. Two minor items, neither of which changes the verdict.

1. **[minor] Area 2 "Preserved renderings" — the mixed-key example output
   is factually wrong.** Location: design §"Area 2 — Behavior contract",
   the bullet claiming mixed `{'z', 2}` renders as `extra [2, 'z']` with
   "repr ordering: `'2'` precedes `"'z'"`". Verified by execution on the
   review branch: `sorted({2, 'z'}, key=repr)` yields `['z', 2]`, because
   `repr('z')` is `"'z'"` whose leading apostrophe (U+0027) precedes the
   digit `'2'` (U+0032) — the corrected message is `extra ['z', 2]`. The
   *mechanism* is correct and deterministic (the explicit all-str check +
   `key=repr` fallback can never raise the incidental `TypeError`, and the
   single-element renderings `[2]`, `[None]`, `[('a',)]` were verified
   byte-identical to today); only the example's predicted ordering is
   wrong. Required change: correct the example in the design document to
   `extra ['z', 2]`. No test-plan change is needed — test-plan risk 3
   explicitly leaves the exact rendering of non-string key names as
   implementation choice, and R3-TC-07e/07f assert only a `ValueError`
   naming the discrepancy; sw-tom must not byte-pin the wrong ordering and
   sw-mike must not assert it. Owner: sw-celeste (document fix); awareness
   sw-tom, sw-mike.
2. **[minor] Area 1 — precedent citation imprecise.** Location: design
   §"Area 1 — Decision", "the same pattern as the existing precedent in
   `tests/test_tu_integration.py`". The exact precedent pattern
   `before_get=lambda stored: obj.observe_outputs()['y']` lives in
   `tests/test_tu_simulation_integration.py` (lines 72, 114, 159, 199);
   `tests/test_tu_integration.py:170` uses a different `before_get` form.
   The architectural claim (an existing, documented hook reused as
   precedent) stands either way. Required change: correct the file
   citation. Editorial only. Owner: sw-celeste.

## C-6 ruling (architect decision, explicit)

**ACCEPT.** `arch.md:291` (sentence spanning lines 289–292: "so a failed
`set_input`, `evolve_once` or `reset()` leaves inputs, states and
observations exactly as before, and the object remains fully usable after
a failed `reset()` (a later `reset()` may succeed)") is **in scope** for
the R3-001 reset-claims correction. Justification:

1. **Same claim family.** PRD gap 3 (`prd.md` lines 118–122) targets two
   overclaims: successful-reset equivalence to fresh construction, and
   failed-reset guarantee of identical future observations. The
   `arch.md:291` sentence contains the second overclaim verbatim
   ("...`reset()` leaves ... observations exactly as before") — the same
   claim class as the baseline-pinned site `object.py:666–669`. The
   baseline pinned sites enumerate where the tester *found* the claims;
   they do not delimit the authorized claim family.
2. **Test-plan consistency forces it.** R3-TC-07n's checklist scopes the
   "arch.md Simulation section" as a whole and requires that "failed-reset
   wording promises owned inputs/states only and states that subsequent
   *observations* require a deterministic `observe`". Leaving line 291
   uncorrected would leave a directly contradictory claim inside a document
   under check; R3-TC-07n could not then be passed honestly.
3. **The correction stays within authorized scope.** The replacement keeps
   the store-preservation promise, adds the deterministic-`observe`
   condition, and touches no other architecture statement. It does not
   alter the planned Release 3 architecture; `architecture-plan.md`
   §"Scoped Release 2 correction" explicitly plans to "narrow reset claims
   in object docstrings, guide and architecture".

The design's companion scoping decision is also confirmed correct:
`object.py:487` and `docs/user-guide/simulation.md:144` describe failed
`set_input`/`evolve_once` effects on *stores* ("leaves the input store,
state and outputs exactly as before") in failure modes where no
observation callback runs and no reset-equivalence claim is made; they are
not the reset claim family PRD gap 3 authorizes correcting, and expanding
to them would exceed the authorized scope without separate authorization.
They stay untouched. (Recorded here so the tester can cite this ruling if
the sites surface during R3-TC-07n.)

## Point-by-point findings

### 1. Architectural compliance — PASS

- The design changes only documented Release 2 behavior/wording. The
  architecture plan's §"Scoped Release 2 correction and documentation"
  pre-authorizes exactly these four corrections: mixed-type callback-key
  diagnostics, standalone bridge readback, narrowed reset claims, and
  Release 2 record reconciliation without rewriting history or inventing
  promotion. The design implements all four and nothing else.
- **Zero-code bridge fix is architecturally sound.** The fix reuses the
  existing `Parameter.before_get` hook instead of adding a bridge support
  class or new `Parameter` behavior — verified in
  `softlab/tu/station/parameter.py:409–417` that `get()` performs
  permission → `self._value = self._before_get(self._value)` → encode →
  return, so `before_get`'s return genuinely replaces the stored value and
  the parameter store converges to the authoritative object input on every
  read. `set()` order (permission → validate → decode → `before_set` →
  store → `after_set`, `parameter.py:396–407`) is untouched, so the pinned
  two-gate asymmetry and R3-TC-07d semantics hold by construction. This is
  the Simplicity Mandate applied correctly: zero new entities where an
  existing documented hook suffices.
- **No public network contract change.** The design states connected-mode
  (pending-source/delayed-edge) readback semantics are deferred to R3-003
  and adds a guide paragraph that distinguishes standalone from connected
  readback *without stating connected semantics* — exactly matching the
  architecture plan ("Standalone bridge control getters read authoritative
  object inputs; connected controllers instead read pending source values…
  Document these distinct roles rather than silently changing existing
  Parameter behavior") and PRD gap 1's characterization requirement.
- **Consistency with the guide's §9 correction and R3-TC-07t.** The
  drafted replacement sentence states the corrected read-back semantics
  (authoritative input store, self-healing after direct `set_input`,
  `reset()`, cross-controller writes) and removes the line-243 overpromise
  — satisfying R3-TC-07t(a) and (b). For the conditional check (c), the
  design correctly reasons that the wiring change alters neither the
  implemented `Parameter.set` order nor any error class, expects the
  two-gate paragraph (guide lines 245–254) and §6 error table to survive
  unchanged, and assigns implementation-time re-verification with
  file/line citations — precisely what R3-TC-07t(c) demands. The new
  §11-execution note (guide example does not use the bridge, output stays
  byte-identical) is also correct.

### 2. Callback-key fix correctness — PASS (with issue 1)

- Root cause accurately diagnosed against `object.py:774–781` (verified by
  direct read): `expected` names are always strings so `missing` sorting
  is always safe; `extra` may mix types, and `sorted()` on a mixed-type
  set raises the incidental `TypeError`. Baseline §B reproduction (b1–b9)
  matches the code exactly.
- The explicit `all(isinstance(key, str))` check + `key=repr` fallback
  guarantees the documented `ValueError` fires for every key discrepancy,
  including homogeneous-but-unorderable key types (e.g. `complex`) that a
  type-homogeneity check would miss — the stated advantage over try/except
  is real and keeps control flow exception-free in a documented-error
  path.
- **Pinned renderings preserved** (verified by execution): pure-string
  `{'z'}` sorts directly → `extra ['z']`; single non-string extras
  `{2}`, `{None}`, `{('a',)}` render `[2]`, `[None]`, `[('a',)]` —
  byte-identical to today, as R3-TC-07g/07h require. The mixed-case
  example's predicted ordering is wrong (issue 1) but the mechanism is
  deterministic and the test plan does not pin the mixed rendering.
- **Error contract preserved:** non-mapping results remain `TypeError`,
  checked before any key handling (R3-TC-07i); message format unchanged;
  `ValueError` raises inside `_validate_callback_result` before
  `evolve_once`'s commit swap, so build-then-swap atomicity (R3-TC-07j) is
  structurally untouched — verified against the call structure at
  `object.py:766–781`.

### 3. Reset wording corrections — PASS

- **Site C-1** (`object.py:660–661`, verified): the replacement keeps the
  anchor facts ("Never invokes ``evolve`` or ``observe``", "including any
  time inputs/states") and replaces the equivalence claim with the
  owned-store restoration promise plus the factory/deterministic-callback
  condition. No fresh-construction equivalence claim survives.
- **Site C-2** (`object.py:666–669`, verified): the replacement preserves
  exception identity and owned-store preservation, and adds the honest
  observation caveat — "unchanged under a deterministic ``observe``; a
  stateful ``observe`` follows its own external state, which is outside
  this guarantee" — matching baseline finding C-2 verbatim evidence
  (stateful observe produced 2.0 ≠ 1.0 after a failed reset).
- **Sites C-3/C-4** (guide §5 lines 131–132 and 134–141, verified): C-3
  mirrors C-1; C-4 correctly *appends* the observation-condition sentence
  to the already owned-store-scoped paragraph rather than rewriting it.
- **Site C-5** (`arch.md:279`, verified) and **C-6** (`arch.md:291`,
  ruling above): both replacements carry the owned-store promise, the
  external-state caveat and the reproducible-observation condition.
- All drafted sentences satisfy the R3-TC-07n checklist: no unqualified
  "behaviorally identical to a newly constructed one" remains; failed-reset
  wording promises owned inputs/states only with the deterministic-observe
  condition; the reproducible-observation condition (deterministic
  callbacks + reproducible factories) is stated at every site. Code
  behavior is unchanged, so the characterization tests R3-TC-07k/07l/07m
  pin the existing (correct) semantics without contradiction.

### 4. Closure reconciliation approach — PASS (evidence integrity preserved)

- The hybrid approach is correct: **living records** (sprint board, sprint
  summary) are corrected in place with dated reconciliation notes, while
  the **historical verdict** (acceptance.md) is left byte-untouched and
  receives a dated addendum. This distinguishes a record that is supposed
  to track current status (board/summary header) from a committed verdict
  whose text is itself evidence.
- **No invented past evidence.** Every corrected reference was verified in
  the baseline (§D) and spot-re-verified in this review: `c69272a` exists
  ("docs(log): issue release 2 acceptance verdict", 2026-10-05 18:58);
  `gh run view 37298431248` returns
  `headBranch=codex/sim-001-simulation-foundation`, `headSha=cd90ff6…`,
  `conclusion=success`, matching the design's REC-1 claims exactly; the
  acceptance.md CI references at lines 34, 41, 89 match the quoted
  "before the `dev` merge" wording; sprint-board lines 3/10 and
  sprint_summary lines 4/22 match the quoted originals.
- **Promotion-unrecorded stays distinguished.** Every corrected record
  states acceptance (`c69272a`, ACCEPTED) separately from `main` promotion
  (unrecorded/pending, separate user decision); the REC guardrails cite
  `git log main..dev` evidence (this review re-ran it: dev is ahead of
  main, no promotion commit exists) and R3-TC-07s pins it. Historical
  test results and verdicts are explicitly not rewritten (R3-TC-07o).
- The board correction fixes the stale "release close pending" status to
  reflect the committed acceptance while the acceptance.md line 33
  statement is correctly retained as accurate-when-committed history,
  cross-referenced by the addendum. Sound.

### 5. Feasibility and simplicity — PASS

- Production diff is confined to `softlab/tu/simulation/object.py`: one
  4-line logic change in `_validate_callback_result` plus the C-1/C-2
  docstring corrections. Everything else is tests, guide, `arch.md` and
  Release 2 records — matching the PRD scope guard ("Runtime production
  changes are restricted to `softlab/tu/`") and CHK-08-1.
- Zero new dependencies; `pyproject.toml`/`setup.py` untouched
  (R3-TC-08d); stdlib + NumPy only. The Third-Party Dependencies table is
  honestly empty.
- The design explicitly resists over-engineering: no bridge support class,
  no helper extraction for a single-use check, no message-format
  "improvement" while touching the error path. The file-level change list
  (9 files) is complete and each row maps to a design area and to test
  cases in the traceability table; every approved test case
  (R3-TC-07a–07t, 08a–08d, CHK-08-1) is enabled by exactly one design
  element and no design element lacks a test.
- The implementation order (callback-key fix TDD-first, then docstrings,
  then fixture rebind + bridge tests, then guide/arch wording, then record
  reconciliation) is executable by sw-tom exactly as specified. The
  SIM-TC-06a fixture rebind (adding the `before_get` closure, keeping
  every existing assertion) matches test-plan risk 2 and weakens nothing —
  verified that SIM-TC-06a's `drive() == 1.0` assertion
  (`tests/test_tu_simulation_integration.py:99`) still passes under the
  corrected wiring because `before_get` returns the just-written
  authoritative input.

### 6. Compatibility guards — PASS

- Public API unchanged; import identity
  `softlab.tu.simulation.SimulatedObject is
  softlab.tu.simulation.object.SimulatedObject` untouched (R3-TC-08c);
  the only behavior changes are the *documented* `ValueError` now firing
  for mixed-type key discrepancies and the *documented* bridge pattern now
  delivering its already-promised read-back semantics — both inside
  R3-AC-08's "except the explicitly corrected gaps" clause. Release 1 debt
  (OBS-004/005/006, DEFECT-2) stays tracked and untouched. ndarray
  copy-boundary contract (SIM-AC-05) preserved via `get_input`'s fresh
  copy.

## What was inspected

- `log/release_3/design/r3-001-detailed-design.md` (full, 552 lines, at
  `46a17fe`)
- `log/release_3/prd.md` (R3-AC-07, R3-AC-08, gap statements lines
  110–126, scope guard)
- `log/release_3/architecture-plan.md` (scoped Release 2 correction
  section; standalone vs connected readback roles)
- `log/release_3/tasks.md` (R3-001 row and scope constraints)
- `log/release_3/test/r3-001-test-plan.md` (Revision 2: R3-TC-07a–07t,
  08a–08e/CHK-08-1, risks 1–5)
- `log/release_3/test/r3-001-baseline.md` (defect reproductions A/B/C,
  closure findings D-1…D-5)
- `log/release_2/reviews/sim-001-design-review.md` (review convention)
- `softlab/tu/simulation/object.py` (lines 479–504 `set_input` docstring;
  650–686 `reset()` docstring; 731–794 `_validate_callback_result`)
- `softlab/tu/station/parameter.py` (`set()`/`get()` hook order,
  lines 396–424)
- `docs/user-guide/simulation.md` (§5 lines 122–147, §6 error table, §9
  lines 224–254)
- `arch.md` (Simulation section lines 267–304, sites 279 and 289–292)
- `tests/test_tu_simulation_integration.py` (bridge fixture lines 60–76,
  SIM-TC-06a line 99) and `tests/test_tu_integration.py:170`
- `log/release_2/sprint-board.md` (lines 3, 10),
  `log/release_2/sprint_summary.md` (lines 4, 22),
  `log/release_2/acceptance.md` (lines 33–34, 41, 89)
- Verifications executed: `sorted({2, 'z'}, key=repr)` rendering check on
  `.venv` Python 3.13; `git log -1 c69272a`; `gh run view 37298431248
  --json headBranch,headSha,conclusion`; `git log main..dev --oneline`.

All source reads were read-only; no production files and no design
document content were modified by this review. The design document's own
"Design Review" section (Status: PENDING, Review Date unfilled) is left
for the coordinator/designer to close against this review.

## Verdict and next steps

**APPROVED.** The design is architecturally compliant with the Release 3
plan (zero-code bridge fix via the existing `before_get` hook; no public
network contract change), correct in the callback-key mechanism and reset
wording, honest in its closure reconciliation, and minimal enough for
sw-tom to implement exactly as specified. The C-6 extension to
`arch.md:291` is ruled **in scope** (same claim family; required for an
honest R3-TC-07n pass). The two minor issues above (wrong mixed-key
example ordering; imprecise precedent citation) are documentation fixes to
the design text and do not block implementation — sw-celeste should apply
them when closing the review section, and sw-tom/sw-mike must not byte-pin
`extra [2, 'z']` (the actual deterministic rendering is `extra ['z', 2]`;
R3-TC-07e asserts only a `ValueError` naming the discrepancy). Per the
serial workflow, sw-tom may proceed with implementation once the minors
are recorded.

## Closure (2026-10-06, sw-celeste)

**Issues 1–2: RESOLVED.** Closed by the commit carrying this entry
(`docs(log): close r3-001 design review issues` on
`codex/r3-001-corrections`).

- **Issue 1 (Area 2 "Preserved renderings")**: the mixed-key example in
  the design document was corrected from the wrong predicted rendering
  `extra [2, 'z']` to the verified deterministic rendering
  `extra ['z', 2]` (`sorted({2, 'z'}, key=repr)`: the leading apostrophe
  of `"'z'"`, U+0027, precedes the digit `'2'`, U+0032). No test-plan
  change: R3-TC-07e asserts only a `ValueError` naming the discrepancy;
  the mixed rendering remains implementation-pinned-by-mechanism, not
  byte-pinned.
- **Issue 2 (Area 1 decision text)**: the precedent citation was corrected
  from `tests/test_tu_integration.py` to
  `tests/test_tu_simulation_integration.py` (lines 72, 114, 159, 199),
  where the `before_get=lambda stored: obj.observe_outputs()['y']` pattern
  actually lives. Editorial only.
- The design document's "Design Review" section was updated in the same
  commit: Status **APPROVED** (verdict at `24f129f`), the C-6 **ACCEPT**
  ruling recorded, and the closure of issues 1–2 referenced.

The verdict (**APPROVED**) and all point-by-point findings above are
unchanged.
