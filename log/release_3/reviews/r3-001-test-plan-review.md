# R3-001 test plan review — implementability

Reviewer: sw-tom (implementer) | Date: 2026-10-06
Branch: `codex/r3-001-corrections` (initial review at `b1bed62`;
re-confirmation at `b5f2202`)
Input reviewed: `log/release_3/test/r3-001-test-plan.md` (with
`r3-001-baseline.md`, `log/release_3/prd.md` R3-AC-07/08, tasks.md R3-001
row, `docs/user-guide/simulation.md`, `arch.md`, and the cited code in
`softlab/tu/simulation/object.py`, `softlab/tu/simulation/__init__.py`,
`tests/test_tu_simulation.py`, `tests/test_tu_simulation_integration.py`)

**Verdict: APPROVED** — the single issue raised under
CHANGES-REQUESTED is resolved by R3-TC-07t (plan Revision 2); see the
closure log below. The plan is otherwise implementable: every automated
case is writable as a `unittest`-compatible test with stdlib + NumPy,
fails on the current baseline exactly where the plan claims it fails,
and confines production changes to `softlab/tu/` plus guide/arch/log
wording.

## What I inspected

- Test plan and baseline (full read), PRD R3-AC-07/08 (verbatim),
  tasks.md R3-001 scope row, Release 2 review convention
  (`sim-001-test-plan-review.md`).
- Code spot-checks against the baseline's line citations:
  - `softlab/tu/simulation/object.py` — `reset()` docstring claims at
    lines 660–661 and 666–669 (verified verbatim); failure-path text at
    663–674 also states "External side effects of user factories are
    outside this guarantee" — relevant to the R3-TC-07m wording scope;
  - `object.py:775–776` — `missing = sorted(expected - actual)` /
    `extra = sorted(actual - expected)` (verified exact);
  - `object.py` `_resolve_initial` (lines 388–401) — any callable
    declared initial value is unambiguously a factory: R3-TC-07l/07m's
    fixtures (call-counter factory, factory armed to raise on a chosen
    invocation) are constructible through the **public** constructor
    with no monkeypatching;
  - `object.py` `__init__` (282–386), `set_input` (479), `get_input`
    (517), `evolve_once` (539), `observe_outputs` (608), `reset` (649)
    — the APIs every R3-TC-07 case exercises exist with the signatures
    the plan assumes;
  - `softlab/tu/simulation/__init__.py` — `SimulatedObject` is imported
    directly from `.object`, so R3-TC-08c's import-identity assertion
    (`softlab.tu.simulation.SimulatedObject is
    softlab.tu.simulation.object.SimulatedObject`) holds today and is a
    valid no-behavior-change pin;
  - `docs/user-guide/simulation.md` — the overclaims at line 132
    ("behaviorally identical to a newly constructed one") and line 243
    ("equals the object's current (pending) input") verified verbatim;
    the §9 set-order ("permission → validate → decode → `before_set` →
    store → `after_set`") and two-gate asymmetry text verified;
  - `arch.md:279` — "making the object behaviorally identical to a newly
    constructed one" verified verbatim;
  - `tests/test_tu_simulation_integration.py:99` — `assertEqual(drive(),
    1.0)  # read-back contract` verified at the cited line (SIM-TC-06a);
  - `tests/test_tu_simulation.py` SIM-TC-05i (line 745+) — already pins
    factory-armed failed reset with `assertIs` identity, snapshot
    equality and later-reset success via public fixtures, confirming
    R3-TC-07m is implementable as written;
  - `grep -rn "must return exactly" tests/` — zero hits, confirming the
    baseline's B-2 coverage-gap claim.

## Issue list

### 1. [minor] Guide §9 read-back wording correction is in scope but pinned by no check case

- **Location:** R3-TC-07n checklist (area 3, reset claims) vs. plan risk
  note 1 and PRD gap 1; traceability table.
- **Description:** PRD gap 1 requires standalone readback to be
  "characterized and distinguished from pending-source readback in a
  connected run", and tasks.md scope explicitly includes guide wording
  corrections. The plan's risk note 1 correctly states "the guide's §9
  pattern and error table are part of the correction scope" — but the
  only wording check in the plan is R3-TC-07n, whose checklist is
  explicitly limited to reset claims (`object.py` `reset()` docstring,
  guide §5, arch.md Simulation section). Nothing pins the corrected
  guide §9 wording (line 243 currently overpromises "equals the object's
  current (pending) input") or the two-gate asymmetry wording that risk
  1 says "must be re-verified" if the bridge pattern changes. The
  behavior is fully covered by R3-TC-07a–07d; only the documentation
  half of gap 1 lacks a checkable assertion, so a correction that
  silently re-overpromises (or distinguishes nothing) would not be
  caught by any case.
- **Required change:** extend the R3-TC-07n checklist (or add a sibling
  manual record check) to pin: guide §9 states the corrected read-back
  semantics (control reads reflect the object's authoritative input,
  standalone), explicitly distinguishes standalone readback from
  pending-source readback across a connected edge (deferring the
  connected half's details to R3-003 as the plan already does), and — if
  the approved design changes the wiring — the two-gate asymmetry and
  error-table wording still match the implemented set order. Update the
  traceability row for R3-AC-07 (bridge readback) to reference the
  extended wording check.
- **Owner:** sw-mike (tester).
- **Status: CLOSED** — resolved by R3-TC-07t (plan Revision 2), added
  per the reviewer's "or add a sibling manual record check" option.
  Re-confirmed by sw-tom on 2026-10-06 (see closure log below).

No other issues found. See the point-by-point answers below for the
checks that came back clean.

## Answers to the five review points

### 1. Acceptance-criteria coverage

**R3-AC-07 — fully covered.** All four sub-clauses have unambiguous
coverage: bridge readback (R3-TC-07a–07d), mixed callback keys /
documented error (07e–07j), reset documentation claims (07k automated
regression + 07l/07m characterization + 07n manual wording check), and
Release 2 closure evidence (07o–07s manual git/gh record checks, with
REC-1/2/3 each mapped to a specific case: 07p/07q/07r). **R3-AC-08
standalone portion — covered.** Standalone compatibility and imports
(08c), regression/compile/import/warning evidence (08a/08b), and no new
dependencies (08d). The worked connected example, clock, delayed
feedback and connected mock interaction are correctly excluded per the
R3-003/R3-004 scope split, and the plan says so explicitly. The only
partially-covered element is the guide §9 wording half of PRD gap 1 —
issue 1 above; it is a documentation-verification gap, not a behavior
gap. No criterion is untestable as specified.

### 2. Implementability of the automated cases

All 13 R3-TC-07 automated cases (07a–07m) and the four R3-TC-08 cases
(08a–08d) are implementable as written against existing public APIs
(verified by signature inspection; fixtures need nothing beyond the
existing `make_accumulator`-style synthetic objects and factory closures
the public constructor already supports). Assertion precision, checked
case by case:

- **Fail-on-baseline claims verified:** 07a/07b/07c fail today (baseline
  §A a1–a3 verbatim: stale 1.0 vs 5.0; stale 3.0 vs 0.0; drive1 stale
  1.0 vs 2.0) and pin observable read-back equality only — no dependence
  on the fix mechanism, so whichever wiring the approved design chooses
  (`before_get` closure or bridge support) satisfies them. 07e/07f fail
  today (baseline b3: incidental `TypeError` on mixed str/int extra
  keys) and require `ValueError` — precise enough to fail on the defect
  and pass on the sort-safe fix.
- **No false security:** 07g/07h/07i intentionally pass on the baseline
  (b1/b2/b6–b9 verbatim) and pin the existing correct contracts — key
  names preserved (`extra [2]`), pure-string discrepancies stay named
  `ValueError`, non-mapping stays `TypeError` — guarding precisely
  against the overcorrection modes a naive `sorted(key=repr)` or
  exception-remap fix could introduce. 07j pins build-then-swap
  atomicity under the malformed-key path, which the fix must not
  disturb. 07k re-runs existing SIM-TC-04a/05i. 07l/07m are
  characterizations of behavior that is already correct (factory
  re-invocation per reset; failed-reset store preservation with
  `assertIs` identity and later-reset success) — they are honest
  keep-correct pins, not defect detectors, and 07m is implementable
  exactly as SIM-TC-05i already demonstrates (public factory closure
  armed via a counter, no monkeypatching).
- **No over-constraint:** 07a's "exactly one `set_input` per parameter
  set; no evolution on set or read" constrains only the bridge contract,
  not unrelated behavior; 08c's import-identity assertion is a true
  no-op today. 08e is correctly classified as a reviewer-owned
  checklist (CHK-08-1), not counted in the automated tally — this
  applies the SIM-TC-06f lesson from the Release 2 review.
- Fixture-rebinding risk (SIM-TC-06a) is acknowledged with an explicit
  rebind-and-record convention — implementable and consistent with
  AGENTS.md.

### 3. Baseline consistency with code

**All cited references verified, zero mismatches.** `object.py:660`
("On success the object is behaviorally identical to a newly constructed
one", spanning 660–661), `object.py:666–669` (failed-reset "inputs,
states and therefore all subsequent observations are exactly as before"),
and `object.py:775–776` (the two `sorted(...)` calls — the defect's root
cause) all match verbatim. Guide §5 line 132, guide §9 line 243, and
`arch.md:279` match verbatim. `tests/test_tu_simulation_integration.py`
line 99 is exactly the `assertEqual(drive(), 1.0)` the baseline quotes.
The B-2 claim of zero existing callback-key test coverage is confirmed
by grep. One observation (not a mismatch): the `reset()` docstring
already carries the sentence "External side effects of user factories
are outside this guarantee" (line 673) — the R3-001 wording correction
will edit around this, and R3-TC-07n should treat that sentence as the
anchor the corrected text must stay consistent with.

### 4. Scope check

The plan stays within R3-001 scope. Production runtime changes implied
by the cases are confined to `softlab/tu/` (bridge behavior, docstring
corrections), with guide/arch/log edits explicitly acknowledged as in
scope per tasks.md; CHK-08-1 mechanically gates exactly this boundary
(the same convention as Release 2 CHK-06-1). No case exercises connected
or networked simulation, real instruments, or `huo`/`jin`/`shui`/`mu`
behavior. Release 1 debt (OBS-004/005/006, DEFECT-2) is not repaired or
waived — 08b only requires verbatim recording and workflow disposition
if a warning fires, and the baseline confirms none fired. Closure
reconciliation (07o–07s) corrects record statements against verified
evidence and explicitly forbids invented `dev`→`main` promotion
evidence (07s, risk 5). No case forces an out-of-scope change.

### 5. Manual record checks (R3-TC-07o–07s)

All five are verifiable as described. 07o (`git log -1` per cited hash)
and 07p (`gh run view` on runs 37298623628 / 37298431248) were already
executed verbatim in baseline §D with recorded outputs, and the plan
pins the exact corrected-record expectations (pre-merge run 37298431248
at `cd90ff6` vs post-merge dev run at `9408c79`; REC-2 board header vs
committed ACCEPTED verdict at `c69272a`; REC-3 baseline parent `f04e783`
with `80b0b05` explainable as an ancestor). 07q/07r are file-comparison
checks with baseline-recorded line references. 07s (`git log main..dev`)
is a pure git inspection. Preconditions (git, gh CLI access) are stated
and were demonstrated in the baseline. All five are correctly
BASELINE-READY expected-to-fail until the reconciliation edits land, and
each names the evidence that constitutes a pass.

## Over-testing / under-testing assessment

- **Under-testing:** issue 1 — the guide §9 / two-gate asymmetry wording
  correction half of PRD gap 1 has no pinning check (behavior half fully
  covered). This is the only coverage gap found.
- **Over-testing:** none found. 07m partially overlaps existing
  SIM-TC-05i (both pin failed-reset atomicity and exception identity);
  the overlap is deliberate traceability under an R3-TC ID and is
  harmless — noted, no change requested. 07a's combined assertions are
  all bridge-contract properties, not unrelated behavior.

## Implementability confirmation

Every [PLAN] automated case is writable today under `tests/` with stdlib
+ NumPy only, synthetic fixtures, and temp dirs. No case requires edits
outside `softlab/tu/` production code (plus the acknowledged
guide/arch/log wording). New test methods are appended to the two
existing simulation test files without weakening any existing method, as
the plan states. The 135-test baseline arithmetic (99 + 36) is
reproduced in the baseline record and matches R3-TC-08a's expectation.
Once issue 1 is closed, this plan is ready for implementation.

## Closure log

- **2026-10-06 — Issue 1 [minor] — RESOLVED (closed by sw-mike,
  tester).** The required wording pin was added as a sibling manual
  record check **R3-TC-07t** ("Bridge documentation wording, guide §9")
  in the area 1 table of `log/release_3/test/r3-001-test-plan.md`, per
  the reviewer's "or add a sibling manual record check" option
  (extending R3-TC-07n was rejected because 07n is scoped to reset
  claims; §9 read-back wording is a bridge-readback concern). The
  checklist pins exactly the three required expectations: (a) the
  corrected guide §9 read-back wording — a control read reflects the
  object's authoritative input store, with the line-243 overpromise
  ("equals the object's current (pending) input") gone; (b) the
  standalone vs pending-source-across-a-connected-edge distinction,
  deferring the connected half's semantics to R3-003; (c) the
  conditional check that if the approved design changes the wiring,
  the two-gate validation asymmetry paragraph and the guide error table
  (§6) still match the implemented `Parameter.set` order (permission →
  validate → decode → `before_set` → store → `after_set`) and error
  contract. Traceability updated as required: the R3-AC-07 (bridge
  readback) row now references R3-TC-07t; the manual-vs-automated
  paragraph and the planned-files table row include it (the table
  row's stale `07m–07q` range was corrected to `07n–07t` to match the
  plan body, where 07l/07m are automated); area 1 note and risk note 1
  cross-reference the new case. No existing case ID, assertion, or
  verdict changed — the CHANGES-REQUESTED verdict stands for the
  reviewer to re-confirm. Commit reference: `docs(log): close r3-001
  test plan review issue 1` on `codex/r3-001-corrections` (single
  commit containing both this entry and the test-plan change; full
  hash reported in the tester handoff note).
- **2026-10-06 — Issue 1 [minor] — CLOSED (re-confirmed by sw-tom,
  reviewer).** Verified against the closure commit
  (`b5f2202`) on `origin/codex/r3-001-corrections`:
  - **(a) Wording pin** — R3-TC-07t checklist item (a) requires §9 to
    state that a control read via `get()` reflects the object's
    **authoritative input store** and that the line-243 overpromise
    ("equals the object's current (pending) input") is gone. Matches
    the required change verbatim in substance.
  - **(b) Standalone-vs-pending-source distinction** — item (b) pins
    the distinction and defers the connected half's semantics to
    R3-003, exactly as required; no connected semantics are pulled
    into R3-001 scope.
  - **(c) Conditional two-gate/error-table check** — item (c) is
    correctly conditional on the design changing the wiring, pins the
    two-gate validation asymmetry paragraph and the guide §6 error
    table against the implemented `Parameter.set` order (permission →
    validate → decode → `before_set` → store → `after_set`) and the
    implemented error contract. Precise and verifiable.
  - **Verifiability** — each checklist item requires file/line
    citations recorded in the test-results document ("nothing silently
    waived"), so the check cannot self-grade.
  - **No collateral change** — `git diff b1bed62..b5f2202` on the plan
    shows only the 07t addition, cross-references, and a genuine
    traceability-table correction (`07m–07q` → `07n–07t`; 07l/07m are
    automated, 07q never existed). Every assertion in R3-TC-07a–07s
    and R3-TC-08a–08e is byte-identical; traceability rows are
    internally consistent (automated = 07a–07m + 08a–08d; manual =
    07n/07t/07o–07s + CHK-08-1).
  - **Verdict: APPROVED.** The plan is ready for implementation; all
    other aspects approved in the sections above stand.
