# R3-004 test plan review — implementability

Reviewer: sw-tom (implementer) | Date: 2026-10-16
Branch: `codex/r3-004-delivery` (reviewed at `72fcfbf`, synced with
`origin/codex/r3-004-delivery`)
Input reviewed: `log/release_3/test/r3-004-test-plan.md` Revision 1 (with
`r3-004-baseline.md`, `log/release_3/prd.md` R3-AC-01–08 verbatim,
tasks.md R3-004 row (line 11), `log/release_3/architecture-plan.md`,
`log/release_3/sprint-board.md`, predecessor results docs
(r3-001/002/003-test-results.md), the scope-gate carry-over
(reviews/r3-003-design-review.md:507-519,
reviews/r3-002-design-review.md:96-111), the byte-reproduction
convention (`tests/test_sim_user_guide.py`), the golden-trace derivation
(design/r3-003-detailed-design.md:1097-1131), and the delivered
capability (`softlab/tu/simulation/coordination.py`, `mock.py`,
`softlab/tu/station/station.py`, `docs/user-guide/simulation.md`,
`arch.md`).

**Verdict: APPROVED** (final, 2026-10-15) — issue 1 CLOSED after
re-confirmation (see closure record below). Criterion coverage,
implementability of all 8 automated cases, scope-gate reasoning,
baseline consistency and scope discipline are all sound; every cited
file:line anchor I spot-checked resolves exactly as quoted. This plan
is ready for the detailed-design gate.

## What I inspected

- Test plan (full read, 173 lines) and baseline (full read, 153
  lines); PRD R3-AC-01–08 verbatim (prd.md:139-146) against the
  plan's quoted clause text — the four R3-AC-08 clause quotations
  match prd.md:146 exactly, and the scope-gate table's per-AC
  quotations match prd.md:139-145.
- tasks.md R3-004 row (line 11) and scope paragraph (lines 37-45);
  architecture-plan.md:93-94 (the permissive snapshot clause,
  quoted accurately by the plan).
- Sprint-board carry-over text (lines 128-146): the R3-004 row,
  "NOT pre-authorized" note (131-133) and the separate-authorization
  sentence for PO release acceptance / `main` promotion (134-135)
  all match the plan's §1 authority claims; OBS debt out-of-scope
  statement at 143-145 matches risk note 7.
- Fresh verification on this branch:
  `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'`
  → **Ran 207 tests — OK**, exit 0 — matches baseline §1 and
  R3-TC-09d's "207 baseline" verbatim.
- Code spot-checks against every line citation the plan and
  baseline rely on:
  - `softlab/tu/station/station.py` — `objects` property at
    line 100 exactly (plan §2 AC-04 row); `snapshot()` at 190,
    `describe()` at 201 (legacy shape, untouched by R3-002/003
    per r3-002-code-review.md:118 "byte-identical to the base"
    and :213 "no snapshot/describe schema addition (D1)" — both
    read verbatim);
  - `softlab/tu/simulation/coordination.py` — `participants`
    property at 333 and `connections` at 346 exactly (plan §2
    AC-04 row); phase-2 edge-delivery block at 1123-1126 (plan
    R3-TC-09m2) read verbatim;
  - `softlab/tu/simulation/mock.py` — measurement delivered-sample
    contract at 145-150 and `read()` at 198-204 (plan 09m2) read
    verbatim;
  - `tests/test_sim_user_guide.py:35-100` — extraction at 35-42
    (splits on `'## 11. Executed example'`), byte-equality at
    96-100 — the convention 09b reuses, verified runnable;
  - `design/r3-003-detailed-design.md:1097-1131` — "Determinism
    replay values" table read verbatim: chain A→B→C x-states
    1.0/10.0/100.0; 0→1 delivers B.u=1.0, C.u=**10.0** (not
    11.0); states after publication 1.0/11.0/110.0; feedback
    companion (A.u := 100.0 → A.x 101.0); measurement tick-zero
    −1.0 → 100.0 → 110.0; `sim_time == float(k) * sim_dt`. This
    is exactly the ready-made independent golden-trace source
    09a requires — the plan's characterization is accurate;
  - `docs/user-guide/simulation.md` — 401 lines (ends standalone-
    only); deferral paragraph at 257-263 read verbatim
    ("planned Release 3 work … intentionally not stated here");
    §11 heading at 327, file end at 401 — GAP-1/GAP-2 claims
    confirmed; grep -nic for connected terms = 2, both in the
    deferral paragraph;
  - `arch.md` — 462 lines; header at 5-8 pins the Release 2
    integration point verbatim; `tu` row at 27 ends at
    "Release 2 adds the `simulation` subpackage with
    `SimulatedObject`"; bridge paragraph at 302-321 documents
    the set wiring (`before_set=lambda old, new: sim.set_input`)
    and observation wiring (`before_get=lambda old:
    sim.observe_outputs()['y']`) but the R3-001-corrected
    control readback (authoritative input store) is absent
    ("authoritative" appears only once, in the sim-spec
    validation sentence, not readback) — GAP-3/GAP-4 confirmed;
    the single **[PLANNED]** block at 452-462 covers
    non-R3 future work accurately; grep -c for
    `Coordinator|MockControl|MockMeasurement|sim_dt|build_object`
    = **0** — GAP-3's "entirely absent" claim confirmed;
  - `log/release_3/reviews/r3-003-design-review.md:507-519` —
    the scope note requiring R3-004 to "explicitly pick … up (or
    consciously drop)" the additive simulation-description node,
    read verbatim — the §2 carry-over chain is accurately cited;
  - `log/release_3/reviews/r3-002-design-review.md:96-111` —
    Ruling 2 (snapshot/describe remain device-only, ACCEPT) read
    verbatim;
  - Predecessor results anchors used by the §4 matrix, all
    present: r3-003 per-case table lists 01a–01e, 02a–02f,
    03a–03f (+03b cont.), 04a–04f, 05a–05g, 06a–06e, 08a–08f
    all PASS; r3-001 per-case lists 08a–08d PASS + CHK-08-1 and
    the §"R3-TC-07t" guide-citation section; r3-002 results
    records the 17 `test_tu_station_objects.py` methods.
- Tally: 8 automated cases (09a–09h) + 6 manual record checks
  (09m1–09m6) + §4 matrix (8 rows, R3-AC-01–08) — internally
  consistent with the plan's own numbering and the baseline
  GAP-to-case mapping (GAP-1→09a, GAP-2→09b/09m1, GAP-3→09m3/09h,
  GAP-4→09m3(b), GAP-5→09m4/09m6, GAP-6→09m5).

## Issue list

### 1. [minor] R3-TC-09g's `tests/` diff pin can fail a correct implementation — reword to allow design-bound test-file extensions

- **Location:** §3.1, R3-TC-09g expected result (line 106).
- **Description:** the case pins "`git diff dev...HEAD --stat -- tests/`
  lists only the new example test file(s)". As written, this reads as
  an exhaustive enumeration: any modification to an existing test file
  — however legitimate — would make the stat list more than "new
  example test file(s)" and fail the case. Yet the PRD explicitly
  authorizes test changes outside `softlab/tu/` ("Tests, user guide,
  architecture and process records outside it are necessary…",
  prd.md:150-152), and the natural implementation of R3-TC-09b is
  exactly such a change: the byte-reproduction convention it reuses
  (`tests/test_sim_user_guide.py:35-100`, which pins §11) may be
  extended in place to also pin the new connected section (e.g. a
  `guide_connected_example_and_output()` extractor added next to the
  §11 one), rather than duplicated into a new file. The plan's own
  risk note 1 ("Names/mechanisms rebind to the approved R3-004
  detailed design") covers case mechanics, but 09g's tests/ row is
  phrased as a hard enumeration with no rebind allowance — an
  internal tension. The risk is asymmetric in the safe direction
  (a scope violation would still be caught by the production-diff
  and `pyproject.toml`/`setup.py` pins), so this is a wording fix,
  not a coverage gap.
- **Required change:** reword the `tests/` expectation to permit
  R3-004-scope test edits generally, e.g. "`git diff dev...HEAD --stat
  -- tests/` lists only R3-004-scope test changes (the new example
  test file(s) and, if the approved design extends the guide-byte-
  reproduction convention, corresponding edits to
  `tests/test_sim_user_guide.py`); no unrelated test edits". The
  production-diff pins (`softlab/huo|jin|shui|mu` empty; `softlab/tu`
  empty unless the approved design documents it; packaging metadata
  empty; no snapshot()/describe() schema change) stay exactly as
  written — those are the load-bearing scope-gate assertions.
- **Owner:** sw-mike (tester).
- **Status: CLOSED** (reviewer-confirmed 2026-10-15; see closure
  record below).

## Point-by-point answers

### 1. Clause coverage — all four R3-AC-08 clauses + release-wide matrix R3-AC-01–08

**Clause 1 (compatibility, "existing standalone object/device/theory
behavior and imports remain compatible")** — covered by R3-TC-09c
(standalone guide §11 byte-reproduction regression anchor, deliberately
a targeted re-run inside the full-suite 09d), 09d (full 207+ suite),
09e (warning gate), 09f (compile/import + R3-003 export identity).
Release-level re-verification is explicit in the §4 matrix's
"Release-level re-check" column for every criterion.

**Clause 2 (worked connected example)** — covered by 09a (executed
example with design-derived golden trace), 09b (guide byte-
reproduction of the same program), 09m1 (topic coverage with
file:line citations), 09m2 (standalone/connected split honesty).
The four required demonstrations — shared clock, delayed feedback,
mock interaction, reset — map one-to-one onto 09a's expected result
and 09m1's topic list.

**Clause 3 (evidence gates precede integration)** — covered by 09d/09e/
09f (regression/warning/compile-import) and 09m4 (candidate + dev CI
with SHA recording, per the R3-001 REC-1 lesson). Nothing may be
pre-marked green (risk note 6).

**Clause 4 (architecture describes implemented capability only after
delivery)** — covered by 09h (mechanical link integrity) and 09m3
(citation-by-citation content verification with the (a)-(d)
checklist, including both directions of the planned-vs-implemented
marker check — no **[PLANNED]** describing delivered work and no
delivered work described as absent).

**Release-wide matrix (§4)** — 8 rows, one per criterion R3-AC-01–08;
every "Results doc anchor" I spot-checked resolves to real PASS rows
in the cited predecessor results documents (verified above, including
the corrected 02a–02f / 04a–04f ranges from the r3-003 review
closure). Every criterion has a release-level re-check column. The
matrix is a template with verdicts "fill at execution" — correct
form for a plan.

**Unambiguous coverage:** every AC clause maps to at least one case;
no clause is untestable as specified.

### 2. Implementability of the automated cases — especially 09a, 09b, 09h; [PLAN] genuinely un-runnable

All 8 automated cases are writable with stdlib + NumPy against the
delivered public API:

- **09a (worked example + golden trace):** *where the example lives*
  is deliberately delegated — "a single self-contained example
  program (delivered by R3-004 implementation, per its approved
  design)" — consistent with the workflow (this review precedes the
  sw-celeste detailed design) and protected by risk note 1. Natural
  placements (a `tests/` module whose test method execs the program,
  or an `examples/` script imported by the test) both satisfy "runs
  in the standard suite" via 09d's discover command. *Golden-trace
  derivation:* the independent source exists and is exactly as
  characterized — I read design/r3-003-detailed-design.md:1097-1131
  verbatim (chain, feedback companion, measurement −1.0 → 100.0 →
  110.0, `sim_time == float(tick) * sim_dt`). The plan correctly
  pins "derived from declared semantics, not copied from
  implementation output" and pre-classifies a derivation/execution
  mismatch as a bug report to me, not a guide edit (risk note 3) —
  this is the right disposition and prevents the vacuous-pass trap.
  *Determinism prerequisites* (fixed-precision floats, declared
  iteration order) are stated and mirror the working §11 discipline.
- **09b (guide byte-reproduction):** feasible against the verified
  convention — `test_sim_user_guide.py:35-100` demonstrably extracts
  a fenced code block + recorded output block, execs with
  `redirect_stdout`, and asserts byte-equality plus repeat-run
  identity. The new connected section following §11's format gives
  the extractor the same targets. The added assertion "the guide
  code block is the executed example (identical program to 09a's)"
  is implementable (shared source or cross-check). The extraction
  anchor for the *new* section is a mechanism that may rebind per
  risk note 1 (see issue 1 for the one place this rebind freedom is
  not yet reflected).
- **09h (arch.md link integrity):** mechanical (parse markdown links
  / relative paths, assert resolution against the repo) and
  meaningful for clause 4 — a dangling citation in the final
  architecture would be exactly the kind of unverifiable claim 09m3
  must not contain; 09h automates the cheap half, 09m3 does the
  content half. Runnable as specified once final arch.md lands.
- **09c-09g:** all runnable today as commands (I re-ran the core one:
  207 tests OK at this head). 09e's `-W error::Warning` gate
  re-verified implicitly by the r3-003 results (zero warnings) and
  re-runnable here; 09g's pathspecs are valid (`pyproject.toml`,
  `setup.py` exist).
- **[PLAN] cases genuinely un-runnable now** — confirmed by
  independent inspection, not assumed: 09a has no example artifact
  (no `examples/` directory; guide's only executed example is
  standalone §11); 09b has no connected guide section to extract
  (guide grep for connected API = 2 deferral lines); 09h's target
  (final arch.md with Release 3 content) does not exist (0 grep
  matches for any R3-002/R3-003 connected name). No production code
  exists that could let any [PLAN] case pass vacuously.

### 3. Scope-gate reasoning (§2) — the out-of-scope ruling and the R3-TC-09m6 disposition

**The gate check is sound against every R3-AC text.** The table
quotes each criterion accurately (verified against prd.md:139-146),
and the "No" verdicts hold:

- The R3-AC-04 observability claims are factually correct as cited:
  `Station.objects` exists at station.py:100; `Coordinator.participants`
  at coordination.py:333 and `connections` at coordination.py:346 —
  I read all three. Membership and network visibility are genuinely
  observable at runtime without any snapshot/describe change, and no
  acceptance condition's text mentions snapshot/describe content.
- The byte-stable pin claims are correct: r3-002-code-review.md:118
  ("`snapshot()`, `describe()` are byte-identical to the base") and
  :213 ("no snapshot/describe schema addition (D1)"), plus the
  r3-003 plan's R3-TC-08d ("ordinary device snapshot/describe
  contracts unchanged") — all read verbatim. Adding the node would
  change a surface three prior gates pin stable, with no AC
  demanding the change.
- architecture-plan.md:93-94 is permissive ("any simulation
  description **must be** additive and inert"), not mandatory — the
  plan quotes it with the emphasis preserved. Deferral is therefore
  plan-compliant, not plan-violating.
- The PRD scope discipline (prd.md:148-154) supports the ruling:
  runtime changes restricted to `tu` for *needed* work; nothing in
  the ACs needs this.

**R3-TC-09m6 is the right disposition.** The carry-over text
(r3-003-design-review.md:507-519, read verbatim) demands the node be
"explicitly pick[ed] up (or consciously drop[ped])" in R3-004
planning — i.e., a conscious, recorded decision, not silent
inclusion or silent exclusion. A manual ratification check with
both branches covered (ratify deferral, or override with bounded
design + cases) is precisely the correct instrument, and 09g's
scope-guard ("production diff must NOT contain any snapshot()/
describe() schema change") gives the mechanical enforcement if
deferral is ratified. The decision is framed as a *recommendation*
for sw-prod/sw-camille — appropriately, since the plan author
(sw-mike) is not the authority to finally dispose of a scope
question.

### 4. Baseline consistency — GAP-1..6 spot-check against actual repo state

**All spot-checked gap claims verified, zero mismatches** (details in
"What I inspected"): GAP-1 (no connected worked example: guide
standalone-only through line 401, no `examples/`, connected grep = 2
deferral lines at 257-263) ✓; GAP-2 (byte-reproduction convention
covers §11 only; extractor splits on `'## 11. Executed example'`) ✓;
GAP-3 (arch.md:5-8 Release-2-pinned header, :27 tu row ends at R2,
:210-330 standalone-only simulation section, 0 grep matches for
Coordinator/MockControl/sim_dt/build_object) ✓; GAP-4 (bridge
paragraph arch.md:302-321 documents set/observation wiring but not
the R3-001-corrected control readback; "authoritative" appears only
in the unrelated sim-spec sentence) ✓; GAP-5/GAP-6 (structural:
no candidate CI for this branch, no cross-release matrix — both
consistent with sprint-board state). Environment baseline re-verified
live: 207 tests OK at this head, matching baseline §1 and 09d's
pinned count. The baseline's "no production defect asserted; gaps
are missing deliverable artifacts" verdict is correct — this is a
clean evidence-gap baseline, exactly the right framing for the final
task.

### 5. Scope discipline — no case forces unplanned production changes; manual checks verifiable

- No automated case requires any production edit: 09a's example and
  09b's guide section are docs/tests-scope (PRD-authorized outside
  `tu`); 09h/09m3 target arch.md (records scope, explicitly noted in
  risk note 4); 09c-09g are re-runs and diff checks.
- 09g guards the production diff (huo/jin/shui/mu empty; `tu` empty
  unless the approved design documents it; packaging empty; no
  snapshot/describe schema change) — see issue 1 for the one
  wording tension in the `tests/` row.
- Manual checks are verifiable with exact procedures: 09m4 names the
  command (`gh run view`) and the pass condition (both matrix jobs
  success, SHA match); 09m1/09m2/09m3 are read-and-cite checks whose
  pass conditions require recorded file:line citations (the same
  convention that made r3-001's R3-TC-07t check effective); 09m5/09m6
  are compilation/record checks with unambiguous pass conditions.
- Out-of-task items (PO release acceptance, `main` promotion, Release
  1 debt) are explicitly excluded with sprint-board citations
  (§1, risk note 7) — consistent with the r3-003 review convention.

## Over-testing / under-testing assessment

- **Over-testing:** none found. 09c is a deliberate subset re-run of
  09d (targeted regression anchor for the guide example); the
  [PLAN] markers are conservative rather than inflationary (09h is
  technically executable today but meaningfully [PLAN] until final
  arch.md exists — correctly explained in the notes).
- **Under-testing:** issue 1 is a wording fix, not a coverage gap.
  No asserted-but-unexercised permission of the kind found in the
  r3-003 review (issues 2/3 there) was detected: the golden-trace
  derivation source exists and is mandatory; the byte-reproduction
  convention exists and is mandatory; the scope-gate has both a
  decision record (09m6) and mechanical enforcement (09g).

## Implementability confirmation

Subject to the one minor tester-side rewording (issue 1), every
automated case is writable under `tests/` with stdlib + NumPy only,
synthetic fixtures and the delivered public API; [PLAN] cases fail
today by absence of the delivery artifacts (confirmed independently,
not assumed) and are constructed so that only the actual delivered
example/guide/architecture records can turn them green. The golden
trace has a verified independent derivation source, the byte-
reproduction convention has a verified working precedent, and the
scope gate has both a recorded decision path and a mechanical guard.
Once issue 1 is closed, this plan is ready for the detailed-design
gate and implementation.

## Closure record

### Issue 1 — RESOLVED

- **Resolution:** `log/release_3/test/r3-004-test-plan.md` Revision 2 —
  R3-TC-09g's `tests/` expected result reworded exactly as required: the
  diff pin now reads "lists only R3-004-scope test changes (new example
  test file(s) and, if the approved design extends the guide-byte-
  reproduction convention, corresponding edits to
  `tests/test_sim_user_guide.py`); no unrelated test edits". All
  production-diff pins (`softlab/huo|jin|shui/mu` empty; `softlab/tu`
  empty unless the approved design documents it; packaging metadata
  empty; no snapshot()/describe() schema change) remain byte-identical
  to Revision 1, as instructed. The risk-note tension is resolved by
  the wording itself: risk note 1's rebind allowance now extends into
  the 09g tests/ row via the explicit design-bound extension clause, so
  no risk-note change was needed. No other case, pin, matrix row or
  scope item was modified; the §2 scope-gate decision and R3-TC-09m6
  are untouched. Verdict (CHANGES-REQUESTED) stands as recorded by the
  reviewer and is not re-opened or altered here.
- **Commit:** `docs(log): close r3-004 test plan review issue` — the
  closure commit at the head of branch `codex/r3-004-delivery` at push
  time (hash recorded here by subject/branch rather than inline, to
  avoid a self-referential commit hash).
- **Closed by:** sw-mike (tester).
- **Date:** 2026-10-15.

### Reviewer re-confirmation — 2026-10-15

Re-confirmed by sw-tom (original reviewer) at branch head `5e88dd2`
(synced with `origin/codex/r3-004-delivery`):

- **Rewording verified against my prescription:** the 09g `tests/`
  expectation now reads "lists only R3-004-scope test changes (new
  example test file(s) and, if the approved design extends the
  guide-byte-reproduction convention, corresponding edits to
  `tests/test_sim_user_guide.py`); no unrelated test edits" — the
  design-bound extension clause and the negative guard match the
  required change in substance exactly.
- **Production-diff pins byte-identical to Revision 1:** the
  Revision 1→2 diff (`git diff 72fcfbf 5e88dd2` on the plan file)
  contains exactly two hunks — the revision header and the single
  09g cell. The `softlab/huo|jin|shui|mu` empty pin, the
  `softlab/tu` conditional pin, the `pyproject.toml setup.py` pin,
  and the snapshot()/describe() schema-change prohibition are
  unchanged character-for-character.
- **No case weakened:** the change replaces an over-broad exhaustive
  enumeration with a scope-bounded permission plus an explicit new
  negative guard ("no unrelated test edits"); the §2 scope-gate
  decision, R3-TC-09m6 and all other cases/matrix rows are untouched.
- **Risk-note tension resolved:** risk note 1's rebind allowance now
  reaches the 09g `tests/` row via the "if the approved design
  extends the guide-byte-reproduction convention" clause; no
  risk-note edit was needed, and none was made.

Issue 1 is CLOSED with my confirmation; review verdict finalized as
APPROVED. Plan is cleared for the detailed-design gate.
