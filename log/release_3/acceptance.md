# Acceptance Document — Release 3 (connected simulation preparation)

## Acceptance Information
- **Reviewer**: sw-camille (Product Owner)
- **Date**: 2026-10-16
- **Release**: 3
- **Tasks**: R3-001 (Release 2 gap corrections + closure reconciliation),
  R3-002 (object builders and station membership), R3-003 (cohesive
  coordinated simulation), R3-004 (executed connected example, guide,
  final architecture and release evidence)
- **Branch reviewed**: `dev` at `74c99ce` (task merges `6dc4d36`,
  `0bd97d5`, `2991a63`, `86ba510`; release-completion principle
  inspection at `ed51609`; subsequent commits are records-only)
- **Requirements baseline**: `log/release_3/prd.md` (R3-AC-01 through
  R3-AC-08, all Must)
- **Authorization**: user authorization of 2026-10-16, including
  ratification of the R3-TC-09m6 scope gate (snapshot/describe additive
  simulation-description node excluded from Release 3, deferred as a
  separately authorized future change)

## Review scope and evidence examined

I read the actual artifacts directly; no report was taken at face value
without corroborating source. Where a runtime re-execution was needed, I
relied on the verbatim recorded commands re-executed independently by the
principle inspector at the `dev` head (see "Verification method" below).

| Evidence | Reference | What I verified first-hand |
| --- | --- | --- |
| Requirements | `log/release_3/prd.md` | All eight acceptance criteria, the behavioral requirements (clock, delayed edges, mock staging, atomicity), the four Release 2 gap inclusions, the scope boundary (production confined to `softlab/tu/`) and the exclusion list |
| Release-wide matrix | `log/release_3/test/r3-004-test-results.md` §09m5 | All 8 rows PASS with task + results-doc anchors; R3-AC-07 explicitly flagged single-task-reliant and re-verified at release level (09d + 09m2) |
| Per-task results | `r3-001-test-results.md`, `r3-002-test-results.md`, `r3-003-test-results.md`, `r3-004-test-results.md` | Verdicts PASS: 21/21, 24/24, 41/41 + CHK-03-1, 14/14 case IDs; suite growth 135 → 147 → 165 → 207 → 210, each green under `-W error::Warning`; verbatim commands and outputs recorded |
| Implementation | `softlab/tu/simulation/{object,builder,coordination,mock}.py`, `softlab/tu/station/station.py`, `softlab/tu/simulation/__init__.py` | Spot-read the load-bearing anchors: `station.py:105-168` (`sim_dt` property/setter with the documented validation order bool→`TypeError`, non-real→`TypeError`, non-finite/non-positive→`ValueError`, prepared/busy→`RuntimeError`); `coordination.py:1104-1159` (freeze → candidates → measurement/clock candidates → publication; deliveries read only frozen boundary values; `OverflowError` overflow gate before publication); `mock.py:142-215` (`MockMeasurement` tick-zero constructor initial, delivered-only reads); `object.py:947` (`_validate_callback_result`, the malformed callback-key error site); eight-name export surface in `simulation/__init__.py` matching the arch.md claim |
| User guide | `docs/user-guide/simulation.md` | Full read of §9 (standalone bridge, corrected readback wording, "Both patterns are implemented and both are stated here"), §12 (coordinated simulation: composition/ownership, clock, delayed edges, mock interaction, interval binding, reset, connected-vs-standalone) and §13 (executed connected example with verbatim recorded output) |
| Example trace | guide §13 output block | Independently re-derived the arithmetic: step 0→1 delivers A.u=100.0 (C's tick-0 output through the feedback edge), C.u=10.0 (B's *previous* value, never the same-tick 11.0), D.u=2.0 (staged pending), probe=100.0; between-ticks write 3.0 shows readback=3.0 with tick and D.x unchanged; step 1→2 delivers D.u=3.0; reset restores tick/time/states/pendings/samples and the re-run reproduces the trace. The trace is self-consistent and demonstrates clock + delayed feedback + mock interaction + reset |
| Architecture record | `arch.md` | Header dual-pinning (Release 2 + Release 3 candidate points, fill-commit convention self-disclosed); `tu` row updated; coordinated-simulation record with a 20-row claim-by-claim traceability table; `[PLANNED]` markers only on genuine future work; exclusions restated including the snapshot/describe deferral recorded as a *decision*; Release 1 debt still tracked |
| Principle compliance | `log/release_3/principle_compliance_report.md` | Six sections (preparation + R3-001/002/003/004 task inspections + release-completion inspection), all PASS; release-level inspection re-executed the 210-test warnings-as-errors run, compileall, import smoke and `gh run view` re-queries at `ed51609`; six-claim no-invented-evidence spot check all verified |
| Sprint records | `log/release_3/sprint_summary.md`, `sprint-board.md` | All four tasks Done with merge commits and gate hashes; 20 review issues raised, all closed and reviewer-reconfirmed, zero open; serial execution with one active task; the 09m6 ratification now recorded (board lines 174-177, r3-004 results §09m6) |
| Release 2 reconciliation | `log/release_2/acceptance.md` (reconciliation addendum) | CI-run reference corrected (pre-merge 37298431248 vs post-merge 37298623628); verdict otherwise unchanged; no invented `main` promotion |

### Verification method (disclosed limitation)

My acceptance environment provides no shell, so I did not personally
re-run the test suite or `git`/`gh` commands. My first-hand verification
is document- and code-level (full reads of the PRD, all four results
documents, the principle report, guide §9/§12/§13, arch.md, and direct
reads of the implementation anchors listed above, plus an independent
re-derivation of the §13 golden trace). The runtime claims I accept rest
on (a) verbatim recorded commands in the test-results documents and
(b) the release-completion principle inspection's independent
re-execution of every evidence class at the `dev` head (210 tests OK
under `-W error::Warning`, compileall exit 0, import smoke `0.3.0`, all
four CI runs re-queried via `gh run view`). No evidence was accepted
that exists only as uncorroborated prose.

## Test report verification
- [x] All unit/integration tests pass: 210 tests, OK, exit 0 at the
  release head (207 pre-existing byte-identical to `dev` + 3 new
  R3-004), recorded verbatim in `r3-004-test-results.md` and
  independently re-run by the release-completion principle inspection
- [x] Warning gate green: `-W error::Warning` full suite → OK, exit 0;
  zero warnings is proven (every warning class is an error under this
  gate); no Release 1 debt warning fired; nothing waived
- [x] Compile check: `python -m compileall -q softlab` exit 0
- [x] Import smoke: `import softlab` → `0.3.0`; the eight-name public
  export surface verified (I read `simulation/__init__.py` myself)
- [x] CI green on Python 3.9 + 3.13 at every integration candidate and
  at the `dev` head (run 38829927217 at `ed51609`, re-queried by the
  inspector); the fill-commit convention is honored — no CI result is
  marked green before it exists
- [x] Integration tests cover the PRD flows: delayed chain/fan-out/
  feedback networks, tick-zero preparation, mock staging and delivery,
  shared clock rules, atomic tick/reset, standalone compatibility, and
  the executed connected example (byte-reproduced)

## Per-criterion verdict (R3-AC-01 – R3-AC-08)

| ID | Requirement (user need) | Verdict | Evidence verified |
| --- | --- | --- | --- |
| R3-AC-01 | Uniform delayed network: chain, fan-out and feedback produce previous-boundary results regardless of registration/iteration order; no same-tick propagation | **MET** | r3-003 cases 01a–01e PASS; code read confirms the mechanism: `step()` freezes boundary sources before any user code runs and deliveries read only frozen values (`coordination.py:1104-1135`); participants and edges iterate in `sorted()` order, so insertion order cannot affect results; the §13 golden trace re-derived by me shows C.u=10.0 (B's previous value, never the same-tick 11.0) and A.u=100.0 (feedback through C's tick-0 output); re-demonstrated at release level by 09a/09b |
| R3-AC-02 | Defined initial state and device timing: documented tick-zero outputs/samples; writes stage, readback shows pending, measurements show delivered; reads/writes never tick | **MET** | r3-003 02a–02f PASS; guide §12 documents tick-zero semantics; `mock.py:142-215` class docstring verified — tick-zero sample is the constructor initial, `prepare()` delivers nothing to measurements, reads never fetch the current source; the example shows probe=-1.0 at prepare and readback=3.0 with tick and D.x unchanged after a between-ticks write |
| R3-AC-03 | Shared clock: valid explicit `sim_dt`; invalid/nonfinite/zero/negative/bool rejected; one interval per successful tick; mid-run changes fail until reset; documented model-dt binding | **MET** | r3-003 03a–03f PASS; `station.py:105-168` read first-hand — the setter implements exactly the documented validation order (bool→`TypeError`, non-real→`TypeError`, non-finite/non-positive→`ValueError`, prepared/busy→`RuntimeError`), `Decimal` rejected via the non-real check; `tick`/`sim_time` computed as `float(tick) * sim_dt` with an atomic `OverflowError` gate before publication (`coordination.py:1144-1150`, read); `bind_interval` explicit, nothing inferred from names (guide §12) |
| R3-AC-04 | Station membership and builders: device use compatible; membership, explicit lookup, naming, builder discovery/construction, failed-build safety, independent instances; connected-removal and shared-ownership conflicts rejected | **MET** | r3-002 04a–04r PASS (18 new-capability cases vacuous-fail on baseline, red state independently reproduced by the inspector) + r3-003 04a–04f/08a/08c; `station.py:100-102` (`objects` property) and the `ObjectBuilder` registry exports read first-hand; the five-mutation kill table in the R3-002 code review pins the failure-atomicity claims |
| R3-AC-05 | Valid safe connections: endpoint/direction/value compatibility, single-driver, fan-out copy safety, ndarray isolation, unconnected inputs retained, atomic graph changes | **MET** | r3-003 05a–05g PASS; code read confirms each destination receives `_copy_value(prepared)` (`coordination.py:1131`) so fan-out deliveries are independent copies; delivery validation failures are wrapped with the offending connection named (`coordination.py:1124-1130`); graph edits are STOPPED-only with validate-before-mutate (arch.md anchor `coordination.py:750-856`) |
| R3-AC-06 | Atomic tick/reset: failures preserve the committed network and clock with original exception identity; reset rebuilds participants/buffers/clock; accepted pending writes survive failed ticks; side-effect limits explicit | **MET** | r3-003 06a–06e PASS; the two-phase structure (all candidates computed before any publication) verified by code read — a failure anywhere in phases 1–3 leaves committed state untouched; the external-side-effect limit is stated in the `reset` docstring (`coordination.py:1194-1197`) and guide §12; reset is demonstrated live in §13 (tick/time/states/pendings/samples restored; re-run reproduces the trace bit-identically) |
| R3-AC-07 | Close Release 2 gaps: bridge readback; malformed callback keys; honest reset claims; Release 2 board/records reconciled with verified evidence | **MET** | r3-001 07a–07m (automated) + 07n–07t (manual) + CHK-08-1 all PASS; `object.py:947` (`_validate_callback_result`) read first-hand; guide §9 wording read first-hand — the authoritative-input-store rule, the standalone-vs-connected distinction and "Both patterns are implemented and both are stated here"; the Release 2 acceptance reconciliation addendum read (CI run reference corrected, acceptance vs promotion distinguished, no invented promotion); flagged single-task reliance in the 09m5 matrix is discharged by the release-level re-verification (byte-identical test files in the 210 suite + 09m2 re-check) |
| R3-AC-08 | Compatibility and usable delivery: standalone behavior/imports preserved except the corrected gaps; a worked connected example demonstrates clock, delayed feedback, mock interaction and reset; regression/compile/import/warning/CI evidence precedes integration; architecture describes implemented capability only | **MET** | All 207 pre-existing test files byte-identical to `dev` and green inside the 210 suite (mechanically checked, r3-004 §5); guide §13 is an executed, byte-reproduced example covering all four required elements (verified against the recorded output by my own re-derivation); arch.md describes the implemented capability with claim-by-claim anchors, `[PLANNED]` only on genuine future work, and the snapshot/describe deferral recorded as a decision; CI evidence precedes every integration with the fill-commit convention honored |

## Scope and exclusion confirmation

**PASS — scope respected, including the ratified 09m6 exclusion.**

- Production changes are confined to `softlab/tu/` (the simulation
  package plus `station/station.py`); `git diff` over
  `softlab/huo`/`jin`/`shui`/`mu`, `pyproject.toml` and `setup.py` is
  empty, verified by every task-level scope gate and re-verified at
  release level by the principle inspection. Zero new dependencies, no
  Python-support change.
- **R3-TC-09m6 ratification**: the additive, inert simulation-description
  node for `Station.snapshot()`/`describe()` is consciously excluded
  from Release 3 and deferred as a separately authorized future change.
  The user ratified this on 2026-10-16; the disposition text is recorded
  in r3-004-test-results.md §09m6, the decision is visible in
  sprint-board.md and arch.md (392-398, 411-414, 583-586), and the
  mechanical guard (empty production diff) makes the exclusion
  structural, not just documentary. As Product Owner I co-sign this
  ratification: the exclusion is justified (no R3-AC criterion requires
  the node; membership and network visibility are already observable via
  `Station.objects` and the coordinator's participant/connection
  surfaces) and it is honestly recorded rather than silently omitted.
- PRD exclusions remain excluded and are restated in arch.md: no
  concrete `huo` experiment process, no scheduler/background workers or
  automatic advancement, no real-time pacing, no thread safety, no
  solvers, no instant edges, no configurable multi-tick delays, no graph
  persistence, no noise framework, no physical-fidelity claims, no real
  instrument access, no UI.
- Release 1 debt (OBS-004/005/006, DEFECT-2) remains tracked, untouched
  and out of scope; no warning was waived anywhere (waiver grep over the
  release records returns only "no waiver"/"not waived" statements).
- No invented evidence: the release-completion inspection's six-claim
  spot check re-verified CI runs, the red commit, the deferral records
  and file anchors against live `gh`/`git` results.

## PRD gate-list verification

| Gate (required by PRD/workflow) | Status | Evidence |
| --- | --- | --- |
| TDD test-plan review per task | DONE ×4 | Approvals `d657e13`, `5a982d5`, `8b34583`, `c17abdd`; all review issues closed and reviewer-reconfirmed |
| Detailed design + architect review per task | DONE ×4 | Designs `88ca37d`, `fefe992`, `f5d8680`, `4ed0ccc`; architect rulings all ACCEPT; R3-003 design proofs withstood attack and golden traces were re-derived by the reviewer |
| Independent code review per task | DONE ×4 | APPROVED `90d5bca`, `8d20542`, `7a4a30b`, `b82f5ec`; mutation-kill evidence in R3-002 (5/5) and R3-003 (6 kill-checks); R3-004 recorded output re-executed byte-identically by the reviewer |
| Tester execution per task | DONE ×4 | 21/21, 24/24, 41/41 + CHK-03-1, 14/14; suites 147/165/207/210 all OK under `-W error::Warning` |
| Principle inspection per task + release | DONE ×5 | `ba61b52`, `a4d3ad1`, `ceea4c6`, `4bcb05f` + release-completion section, all PASS |
| CI before each `dev` integration | DONE ×4 | Candidate CI green before every merge; dev CI green at every integration incl. run 38829927217 at the release head |
| arch.md actual-implementation update | DONE | `f32b796` + fill commit `45d4244`; convention self-disclosed |
| UI/Figma and real-hardware gates | N/A | PRD-declared not applicable; no UI change, no hardware access |
| Release acceptance | THIS DOCUMENT | — |

## User experience assessment

- [x] The guide opens with the distinction a user actually needs
  (object vs. mock device, standalone vs. connected) and states each
  contract with its rationale — §12 reads as a user document, not an
  implementation dump
- [x] The worked example is honest and complete: it constructs a fresh
  station (no global state), demonstrates the shared clock (`sim_dt`,
  `tick`, `sim_time`), the delay rule (C.u=10.0, never the same-tick
  11.0), feedback through the initialized tick-zero value, mock write
  staging with immediate readback and unchanged state, delivered
  measurement samples, and a coordinated reset whose re-run reproduces
  the trace — everything R3-AC-08 clause 2 asks for, with verbatim
  recorded output that the code reviewer re-executed byte-identically
- [x] Failure behavior is documented where the user will look for it:
  validation errors name the offending connection; the mid-run `sim_dt`
  rejection says "reset it first"; the clock-overflow failure is
  documented as atomic; busy-guard `RuntimeError`s cover writes during
  an in-flight tick/reset
- [x] Honesty requirements are met: reset is explicitly *not* claimed to
  equal fresh construction; external callback side effects are placed
  outside every rollback guarantee; the standalone-vs-connected readback
  difference is stated in both patterns' sections; no stale "planned
  Release 3 work" wording survives anywhere in the guide
- [x] Opt-in ergonomics are preserved: `Station(name)` requires no new
  argument and ordinary device use is untouched — a non-simulation user
  pays nothing for this release
- [x] The capability answers the PRD's core promise: an experiment
  author can assemble objects and mocks, connect them, and evolve the
  network with one shared clock where every connection carries
  previous-step values — deterministically, and with a reset that truly
  restarts the experiment

## Issues found

### Issue 1: arch.md header cites the Release 3 candidate point, not the final dev integration
- **Severity**: Minor (non-blocking)
- **Related Requirement**: R3-AC-08 (evidence honesty)
- **Description**: the arch.md status header records the Release 3 state
  at the R3-004 *candidate* point (`f32b796`, run 38826003029) per the
  two-stage fill-commit convention. The final dev-integration CI runs
  (38829754958 at merge `86ba510`, 38829927217 at head `ed51609`) are
  recorded in the sprint board and principle report but not in arch.md.
- **Expected Behavior**: ideally the architecture record's header would
  also reference the final integration evidence.
- **Actual Behavior**: the header is honest and self-discloses the
  convention ("the cited candidate run certifies an ancestor of this
  fill commit"); a reader following the records finds the final-run
  evidence immediately.
- **User Impact**: negligible — the claim is accurate as labeled; this
  is a completeness nicety, not a correctness or honesty defect.
- **Recommendation**: a future docs commit may add the final
  dev-integration run reference; does not gate this acceptance.

No Blocker or Major issues were found.

## Acceptance Conclusion

### Pass Conditions Verification
| Condition | Status |
| --- | --- |
| ALL acceptance criteria (R3-AC-01–08, all Must) met | YES |
| ALL tests passing (210, zero warnings, warning-gate green) | YES |
| CI green on both matrix versions at every integration and at the dev head | YES |
| Zero open review issues, zero test failures | YES |
| Documentation complete, accurate and user-facing (guide §12–§13 executed and byte-reproduced) | YES |
| Architecture record reflects implemented capability only | YES |
| Scope discipline: production confined to `softlab/tu/`, exclusions respected, 09m6 ratified | YES |
| No invented evidence; standalone compatibility preserved | YES |
| User experience acceptable | YES |

### Overall Verdict

**Status: ✅ ACCEPTED (PASS)**

**Summary**: Release 3 delivers exactly what the PRD promised — a
coordinated simulation with one shared explicit clock in which every
connection carries previous-step values, mock endpoints with honest
pending/delivered semantics, atomic ticks and resets, station object
membership and builders, the four scoped Release 2 corrections, and an
executed connected example that demonstrates all of it — and nothing
more. Every Must criterion is met with evidence I traced to its source
and, for the load-bearing claims, verified first-hand in the code and
documentation, corroborated by the release-completion principle
inspection's independent re-execution at the `dev` head. The ratified
09m6 exclusion is recorded as a conscious decision in three places and
enforced mechanically by an empty production diff. The single finding is
a Minor documentation nicety that does not affect delivered behavior or
evidence honesty. Release 3 is accepted; `main` promotion is unblocked
from the Product Owner side, subject to separate user authorization.

**Signed**: sw-camille, Product Owner — 2026-10-16
