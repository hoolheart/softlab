# R3-004 test plan — executed connected example, user guide, final architecture and release evidence

Tester: sw-mike | Date: 2026-10-16 | Revision: 2 (developer review issue 1 addressed:
R3-TC-09g `tests/` expectation reworded to permit design-bound extensions of
`tests/test_sim_user_guide.py`; no other case, pin or scope item changed)
Branch: `codex/r3-004-delivery` (baseline head `92e128d`, from dev `2991a63`)
Requirements: [log/release_3/prd.md](../prd.md) — R3-AC-08 in full (remaining clauses),
release-wide acceptance evidence of R3-AC-01–08
Baseline: [r3-004-baseline.md](r3-004-baseline.md) (this task's baseline, produced
with this plan)
Predecessor evidence: [r3-001-test-results.md](r3-001-test-results.md),
[r3-002-test-results.md](r3-002-test-results.md),
[r3-003-test-results.md](r3-003-test-results.md)

## 1. Scope and authority

R3-004 (tasks.md row 4) is the final task: "Executed connected example and user
guide; final architecture, integration and release evidence." Acceptance
coverage: **remaining R3-AC-08** (the portion not covered by R3-001's
standalone half) **+ release-wide acceptance of R3-AC-01–08**.

R3-AC-08 in full (prd.md:146), decomposed into its four observable clauses:

> 1. "Existing standalone object/device/theory behavior and imports remain
>    compatible except the explicitly corrected gaps."
> 2. "A worked connected example demonstrates the clock, delayed feedback, mock
>    interaction and reset."
> 3. "Regression, compile/import, warning and candidate CI evidence precede
>    integration."
> 4. "Architecture describes implemented capability only after delivery."

Clause 2 is the capability clause (the worked connected example explicitly
deferred from R3-001 guide §9 wording check item (b) and from R3-003 design
Q-d); clauses 1/3/4 are evidence clauses re-verified at release level. No new
production capability beyond the example/guide/architecture records is assumed
by this plan; R3-004 is records/docs/example-heavy, so **automated cases and
manual record checks are explicitly distinguished** throughout.

This plan does not authorize: `main` promotion, Product Owner release
acceptance (both require separate user authorization per sprint-board.md:134-135
and 143-144), real instrument access, or any production change outside the
PRD-authorized surface (tests/docs/process records plus, only if the approved
design requires it, `softlab/tu/`).

## 2. Scope gate — additive simulation-description node in `Station.snapshot()`/`describe()`

**Carry-over:** the R3-003 design review (reviews/r3-003-design-review.md:507-519)
recorded that the additive, inert simulation-description node in
`Station.snapshot()`/`describe()` — deferred from R3-002 (design
r3-002-detailed-design.md:710, 763, 787; ruling 2 in
reviews/r3-002-design-review.md:96-111) and again from R3-003 (design
r3-003-detailed-design.md:1046-1048, 1357) — must be "explicitly pick[ed] up
(or consciously drop[ped])" in R3-004. Sprint-board.md:131-133 repeats: "to be
considered in R3-004 planning against the PRD — it is NOT pre-authorized."

**Gate check against every R3-AC criterion:**

| Criterion | Relevant text (prd.md) | Requires snapshot/describe node? |
| --- | --- | --- |
| R3-AC-01 | "…produce the specified previous-boundary results regardless of object/device registration or iteration order" (139) | No — results are behavioral; runtime order-independence is already pinned by R3-TC-01d/01e. |
| R3-AC-02 | "Tick-zero object outputs and mock initial samples are documented" (140) | No — documented via docstrings (r3-003 results §7) and guide. |
| R3-AC-03 | "Valid explicit `sim_dt` controls all coordinated participants" (141) | No. |
| R3-AC-04 | "Object membership, explicit lookup, naming, builder discovery/construction…" (142) | No — membership listing is already available at runtime via `Station.objects` (softlab/tu/station/station.py:100) and coordinator introspection (`Coordinator.participants`, coordination.py:333; `connections`, 346). No acceptance condition mentions snapshot/describe content. |
| R3-AC-05 | "…single-driver rules…fan-out copies…" (143) | No. |
| R3-AC-06 | "…preserves the prior committed network and clock…" (144) | No. |
| R3-AC-07 | Release 2 gap corrections (145) | No. |
| R3-AC-08 | "A worked connected example demonstrates the clock, delayed feedback, mock interaction and reset" (146) | **No** — example visibility needs executable code + recorded output + guide reproduction; nothing in the clause requires station snapshot schema changes. R3-TC-08d (r3-003 plan) actively pins the legacy device-only snapshot/describe shape. |

The architecture plan's only relevant clause is permissive, not mandatory
(architecture-plan.md:93-94): "Preserve legacy device-only snapshot and
description contracts; **any** simulation description must be additive and
inert." (Emphasis added.)

**DECISION (recorded scope gate): the additive simulation-description node is
OUT OF SCOPE for Release 3; recommend deferral beyond Release 3** as a
separately authorized future change, with this justification: (1) no R3-AC
criterion — including the R3-AC-08 worked-example clause and R3-AC-04
membership — requires it; membership and network visibility are already
observable via `Station.objects` and `Coordinator.participants`/`connections`;
(2) adding it would change a Release-1/2 compatibility surface (legacy
snapshot/describe shape) that R3-TC-08d and the R3-002 code review
(r3-002-code-review.md:118, 213) pin byte-stable, without any acceptance
criterion demanding that change; (3) PRD scope discipline (prd.md:148-154)
restricts runtime production changes and excludes unrelated additions. This
decision is a **recommendation for sw-prod/sw-camille to ratify**; case
R3-TC-09m6 records the ratification (or override) — it is not silently
included or silently excluded. If ratified as out of scope, no R3-004 case
below may sneak it back in (scope-guard case R3-TC-09g asserts the production
diff does not contain it).

## 3. Test cases

Naming: automated cases R3-TC-09a–09h (placeholders; names/mechanisms rebind
to the approved design per the standing test-plan risk-note convention);
manual record checks R3-TC-09m1–09m6. "[PLAN]" = expected-to-fail or
un-runnable until the delivery artifacts land (see baseline GAP items).

### 3.1 Automated cases (unittest)

| ID | Title | Precondition / fixture | Steps | Expected result | AC clause |
| --- | --- | --- | --- | --- | --- |
| R3-TC-09a **[PLAN]** | Executed connected worked example with deterministic golden trace | A single self-contained example program (delivered by R3-004 implementation, per its approved design) building a station with `sim_dt`, enrolling ≥2 connected objects with a feedback edge, one `MockControl` and one `MockMeasurement`, using only synthetic data | Execute the example end-to-end (project installed); capture printed output | Output demonstrates, with exact printed values: shared clock advancing `sim_time == float(tick) * sim_dt` per tick; delayed chain/feedback (each edge delivers the source's previous-boundary value — no same-tick propagation); mock control write staged between ticks with pending readback, delivered at the next transition; measurement tick-zero initialized sample then delivered samples; coordinated reset restoring tick/time/states. Golden values are the design-derived deterministic trace (r3-003-detailed-design.md:1097-1131 derivation convention: chain A→B→C x-states 1.0/10.0/100.0 → after 0→1: 1.0/11.0/110.0 with C.u=10.0 not 11.0, etc.) | Clause 2 |
| R3-TC-09b **[PLAN]** | Guide byte-reproduction of the connected example | Guide contains the connected example as a fenced code block plus a recorded verbatim output block (new guide section; §11 convention of simulation.md:327-401) | Extract code + recorded output from the guide (same extraction convention as `tests/test_sim_user_guide.py:35-100`, which pins §11); execute; execute again | Actual output equals the guide recorded output byte-for-byte; repeated execution byte-identical; the guide code block is the executed example (identical program to R3-TC-09a's) | Clause 2 |
| R3-TC-09c | Standalone guide example still byte-reproduced (regression anchor) | Current tree | Run `tests/test_sim_user_guide.py` (SIM-TC-08a–08d) | All pass: §11 standalone example output unchanged and still byte-reproduced; guide topic checklist still green | Clauses 1, 2 |
| R3-TC-09d | Full regression suite green incl. new cases | Delivery committed on this branch | `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'` | OK, exit 0; count = 207 baseline + new R3-004 automated cases; 0 FAIL/ERROR lines | Clauses 1, 3 |
| R3-TC-09e | Warning gate | Same | `.venv/bin/python -W error::Warning -m unittest discover -s tests -p 'test_*.py'` | OK, exit 0 — zero emitted warnings (no silent waiver; Release 1 debt OBS-004/005/006, DEFECT-2 must not fire) | Clause 3 |
| R3-TC-09f | Compile and import evidence | Same | `python -m compileall -q softlab`; `python -c "import softlab; print(softlab.__version__)"`; import the four R3-003 public exports from `softlab.tu.simulation` with identity asserts (convention: r3-003 results §4) | compileall exit 0 no output; import exit 0, version prints; export identity holds | Clauses 1, 3 |
| R3-TC-09g | Scope guard — production diff and dependencies | Delivery committed | `git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui softlab/mu` (empty); `git diff dev...HEAD --stat -- softlab/tu` (empty unless the approved design documents a `tu` change — none is anticipated); `git diff dev...HEAD --stat -- pyproject.toml setup.py` (empty); `git diff dev...HEAD --stat -- tests/` lists only R3-004-scope test changes (new example test file(s) and, if the approved design extends the guide-byte-reproduction convention, corresponding edits to `tests/test_sim_user_guide.py`); no unrelated test edits; production diff must NOT contain any snapshot()/describe() schema change (scope-gate §2 enforcement) | All diffs as expected; no new dependencies; no UI/hardware content; no silent scope expansion | Clauses 1, 3 + §2 gate |
| R3-TC-09h **[PLAN]** | arch.md reference integrity | Final arch.md (updated by R3-004 delivery) | Parse every relative file path / markdown link in arch.md; assert each resolves to an existing repo file | All references resolve; zero dangling links | Clause 4 |

Notes:
- R3-TC-09a/09b/09h are **[PLAN]** because the delivery artifacts (connected
  example, guide section, final arch.md) do not exist at baseline — see
  baseline GAP-1/2/3. They become runnable when the implementation lands;
  nothing here marks them runnable before that.
- R3-TC-09a golden values are *derived from declared semantics*, not copied
  from implementation output (the r3-003 "Determinism replay values"
  convention, r3-003-detailed-design.md:1097-1131): the test computes
  expectations from declared initial values; the guide's recorded output must
  equal them; recording implementation output as "expected" without
  independent derivation is a test error.
- The example must print with deterministic formatting (fixed-precision
  floats, sorted/declared iteration order) so byte-reproduction is stable —
  same discipline as guide §11 (simulation.md:329-331).

### 3.2 Manual record checks (not unittest)

| ID | Title | Check procedure | Pass condition | AC clause |
| --- | --- | --- | --- | --- |
| R3-TC-09m1 | Guide connected-simulation coverage with citations | Read the delivered guide additions; for each required topic record file:line: shared explicit clock (`sim_dt`, rejection rules, mid-run change rule); delayed edges and feedback (previous-boundary rule, no same-tick propagation, tick-zero initialization); mock interaction (`MockControl` pending staging + readback, `MockMeasurement` tick-zero sample vs delivered samples, reads/writes never tick); coordinated reset (one-operation rebuild + external-side-effect limit) | Every topic present, stated accurately against softlab/tu/simulation/{coordination,mock,object}.py docstrings (citation anchors per r3-003 results §6/§7), and consistent with the PRD behavioral text (prd.md:31-106); no overpromise (e.g. no "equals fresh construction" claim) | Clause 2 |
| R3-TC-09m2 | Guide standalone/connected split remains honest | Re-verify guide §9 (simulation.md:231-311) after connected additions: §9 still documents the standalone pattern only; the deferral paragraph (lines 257-263) is updated to point at the delivered connected section instead of "planned Release 3 work"; connected readback semantics stated in the new section match the implemented pending-source/delayed-edge contract (coordination.py phase-2 delivery, 1123-1126; mock.py:148-202) | Split is explicit and accurate; no stale "planned"/"not stated here" text survives once the connected section exists; R3-TC-07t(a)/(c) pins (r3-001 results §"R3-TC-07t") still hold | Clauses 1, 2 |
| R3-TC-09m3 | Final architecture verification — every claim traces to delivered code | Clause-by-clause read of the final arch.md: (a) status/header reflects the Release 3 integration point (predecessor SHAs, suite size, CI run) with the same discipline as the current Release-2-pinned header (arch.md:5-8); (b) each Release 3 statement (R3-001 bridge-readback/error/reset corrections; R3-002 membership/builders/registry; R3-003 coordinator, clock, ownership, connections, atomicity; simulation scope note incl. the §2 deferral record) cites file:line verified against the actual delivered code at the test head; (c) planned-vs-implemented markers resolved: no **[PLANNED]** text describing delivered R3 capability, and no delivered R3 capability described as future/absent (current gaps: header arch.md:5-8, tu row arch.md:27, simulation section arch.md:210-330 all stop at Release 2; 0 matches for Coordinator/MockControl/sim_dt/build_object — baseline GAP-3/4); (d) Release 1/2 content not regressed | All (a)-(d) hold with recorded file:line citations; "architecture describes implemented capability only after delivery" satisfied — the architecture now describes exactly what R3 delivered, no more, no less | Clause 4 |
| R3-TC-09m4 | Candidate CI + integration CI evidence | `gh run view` of the candidate run for this task branch (head SHA = test head) and, after integration, the dev run; record branch, SHA, conclusion, both matrix jobs | Both runs `success` on Python 3.9/3.13 matrix; recorded SHAs match the commits they certify; no CI result marked green before it exists (R3-001 REC-1 lesson) | Clause 3 |
| R3-TC-09m5 | Release-wide acceptance matrix R3-AC-01–08 | Compile the matrix (§4 below) from the three predecessor results docs plus this task's results; for each criterion confirm: criterion text quoted, covering task(s), results-doc section anchors, per-case verdicts, and release-level re-verification (R3-TC-09c/09d/09e/09f regression evidence) | Every criterion shows PASS with traceable evidence; any criterion relying on a single task's evidence is flagged; nothing marked passed that was not run | Clauses 1-4 (release-wide) |
| R3-TC-09m6 | Scope-gate ratification record | Confirm the §2 decision (simulation-description node deferred beyond Release 3) is explicitly ratified by sw-prod/sw-camille in the process records (sprint-board or review note), or that an override scope decision exists with justification and matching design authority | Decision is explicit in the records — not silent; if overridden, a bounded design + cases exist instead. Closes the carry-over from reviews/r3-003-design-review.md:507-519 | §2 gate + clause 4 |

## 4. Release-wide acceptance matrix (template for R3-TC-09m5)

Evidence anchors are the per-task results documents; release-level
re-verification for compatibility criteria is R3-TC-09c/09d (full suite incl.
all predecessor suites) plus 09e/09f (warning/compile/import gates).

| Criterion (prd.md) | Covered by task | Results doc anchor | Release-level re-check | Verdict (fill at execution) |
| --- | --- | --- | --- | --- |
| R3-AC-01 uniform delayed network | R3-003 | r3-003-test-results.md per-case 01a–01e | suite green (09d) | — |
| R3-AC-02 defined initial state and device timing | R3-003 | 02a–02f + §6/§7 citations | suite green (09d) | — |
| R3-AC-03 shared clock | R3-003 | 03a–03f + §6 items 4-5 | suite green (09d) | — |
| R3-AC-04 station membership and builders | R3-002, R3-003 (connected half) | r3-002-test-results.md; r3-003 04a–04f + 08a/08c | suite green (09d) | — |
| R3-AC-05 valid safe connections | R3-003 | 05a–05g | suite green (09d) | — |
| R3-AC-06 atomic tick/reset | R3-003 | 06a–06e + §7 side-effect citation | suite green (09d) | — |
| R3-AC-07 close Release 2 gaps | R3-001 | r3-001-test-results.md 07a–07t + CHK-08-1 | suite green (09d); guide §9 (09m2) | — |
| R3-AC-08 compatibility and usable delivery | R3-001 (standalone), R3-002, R3-003, **R3-004 (connected example + final evidence)** | r3-001 08a–08d; r3-003 08a–08f + CHK-03-1; **this doc 09a–09h, 09m1–09m6** | 09c–09h + 09m1–09m5 | — |

## 5. Risks and conventions

1. **Names/mechanisms rebind** to the approved R3-004 detailed design; case
   IDs and the golden-trace *derivation convention* do not rebind. Any rebind
   is recorded in the results document dispositions.
2. **Example determinism:** the example must avoid dict-order dependence,
   wall-clock reads and unformatted floats; byte-reproduction (09b) is the
   enforceable form of "worked".
3. **Golden values must be independently derived** (see 09a note); if the
   executed example's output disagrees with the derived trace, that is a bug
   report to sw-tom, not a guide edit.
4. **arch.md is records/docs scope:** R3-TC-09m3 verifies content, but the
   file is produced by the delivery (sw-tom); this plan defines what "final
   architecture verification" must establish, citation-by-citation.
5. **No silent waivers:** any [PLAN] case left un-runnable at test time, any
   manual check without file:line citations, or any criterion without an
   evidence anchor fails the task.
6. **CI evidence discipline (R3-001 REC-1):** candidate-branch CI precedes
   integration; SHAs recorded; nothing pre-marked green.
7. **Out of task scope (recorded, not tested here):** Product Owner release
   acceptance, `main` promotion, Release 1 debt (OBS-004/005/006, DEFECT-2)
   remain tracked separately (sprint-board.md:143-145).
