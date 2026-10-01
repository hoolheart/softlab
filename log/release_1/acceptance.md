# Acceptance Document — Release 1 (TU-001–TU-008)

## Acceptance Information
- **Reviewer**: sw-camille (Product Owner)
- **Date**: 2026-10-01
- **Release**: 1 — compatible `tu` foundations
- **Basis**: `dev` tip `21983cafaf43f3d7eb22170bf95586197917bcd7`
  (all TU-001–TU-008 integrated). `main` verified untouched
  (`f288b2f34f34782a31637363c51ccc6409ebee2f`, packed ref unchanged).
- **Method**: evidence review only — recorded gates, reviews, CI links and
  documents were read; no test was re-run and no code was executed by the
  product owner.

## Test Report Verification
- [x] All unit/acceptance tests pass per recorded gates
  (`log/release_1/tests/TU-00*.md`): TU-001 22/22, TU-002 33/33,
  TU-003 40/40, TU-004 50/50, TU-005 60/60, TU-006 77/77, TU-007 92/92,
  TU-008 99/99 final suite — each with zero skips, zero warnings under
  `-W error`, independent tester reruns recorded.
- [x] Integration coverage of user flows: `tests/test_tu_integration.py`
  (7 cases) verifies public imports, combined `huo` count/scan usage with
  the new contracts, virtual device lifecycle journey, model round trip and
  migration-free legacy usage (TU-008 final green evidence).
- [x] Compilation: sprint-end forced compile of `softlab` AND `tests`
  under `-W error` exits 0 with zero output (TU-008 P1; DEFECT-1 raw-string
  sites fixed in `6c7af65`/`b57b136`, zero behavior change).
- [x] Import smoke under `warnings.simplefilter('error')`: PASS (TU-008 P1
  step 3).
- [x] Package-artifact inspection: isolated PEP 517 build produced sdist
  113739 B and wheel 111480 B; `VERSION.txt`, `softlab/tu/station/*` and
  `softlab/tu/theory/*` present in both; wheel carries 0 test files and
  `METADATA Version: 0.3.0` equals `softlab.__version__`; artifacts built
  in a temp directory, none committed (TU-008 P2).

## User Story / Acceptance Criterion Verification (PRD row by row)

| Task | Criterion (summary) | Evidence on record | Status |
| --- | --- | --- | --- |
| TU-001 | Compatibility matrix links public behavior to callers and assertions; characterization on unchanged production code; defects recorded separately; no production change | `log/release_1/compatibility.md` (16 contract rows + OBS-001–006); `tests/test_tu_contracts.py` 17 focused tests; 22/22 local pass at `b5599ff`; review APPROVED `1c2886e` (both issues closed with added assertions); arch review APPROVED; CI run 36302785650; integrated `6f39f78`. Production `tu` files unchanged at characterization time | PASS |
| TU-002 | Opt-in versioned descriptions; documented metadata rules; no device I/O; unsupported metadata defined; legacy snapshots/import paths compatible | 11 description cases + 33/33 suite (`tests/TU-002.md` green evidence, `86a6063`); metadata TypeError/ValueError policy and recursion cases 9–11; review APPROVED `b613f2a`; arch APPROVED; CI 36306104192; integrated `8794cb7` | PASS |
| TU-003 | Explicit operation path; discoverable side-effect semantics; legacy `VisaCommand` invocation/permissions/execution counts compatible; description lookup executes nothing | `execute()`/`describe_operation()` 7/7 cases + 40/40 suite (`ffc5f25`); guards pin legacy get-executes-once/write-denied and zero-I/O description; review APPROVED `9002b68`; arch APPROVED `14a7d9d`; CI 36320621031; integrated `d24da5c` | PASS |
| TU-004 | No meaningless connection ops for virtual devices; explicit readiness/ownership; repeated cleanup, failed preparation, shared/borrowed handling verified; unsupported capabilities detectable; non-owner never releases | 10/10 lifecycle cases + 50/50 suite (`a9589eb`); idempotence, failed-prepare release-exactly-once, borrowed-resource and recovery-cycle cases; review APPROVED `b1d8780`; arch APPROVED `898d471`; CI 36323399137; integrated `c989d4c` | PASS |
| TU-005 | Optional unit/type/shape/channel descriptions; richer reading with acquisition time and quality; optional uncertainty/calibration; legacy value-only reads and arbitrary values work; explicit serialization limits | 10/10 measurement cases + 60/60 suite (`7b7d87d`); failed-acquisition error identity, single acquisition, proxy forwarding, denied read, TypeError serialization limit; review APPROVED `1faf8dd`; arch APPROVED `7967464`; CI 36325439753; integrated `18bfe64` | PASS |
| TU-006 | VISA adopts lifecycle/operation contracts compatibly; timeout units/defaults, blocking, concurrency, interruption, errors documented and verified with mocks and `@sim`; original causes preserved; abort request never conflated with confirmed stop | 17/17 VISA cases + 77/77 suite (`2ba02fd`); `@sim` genuineness independently verified; OBS-001 resolved (sentinel contract), OBS-002 fixed (`write_raw`), OBS-003 closed (exact-once failure cleanup); `AbortReport` separation + `confirm_stop()` `*OPC?` latch; TU-001 supersession sanctioned and recorded; review APPROVED `888d7ba`; arch APPROVED `e8528f3`; CI 36725443721; integrated `45a995b` | PASS |
| TU-007 | Model identity, supported serializable configuration, semantic descriptions; opt-in strict evaluation exposes errors; legacy `features`, ndarray mappings and imports compatible; shape checks retained not duplicated; configuration round trips verified | 15/15 theory cases + 92/92 suite (`f02ec2c`); strict/lenient error-identity policy, atomic configure, KeyError/TypeError rejection, validator reuse; OBS-005 disposition pinned (swallow only on legacy/lenient paths); review APPROVED `7920d45`; arch APPROVED `9e69110`; CI 36860164362; integrated `54a195c` | PASS |
| TU-008 | Public imports, `huo` count/scan usage, virtual/simulated device usage and a numerical model verified; added APIs/limitations/migration-free usage documented; regression/compile/import checks and package-artifact inspection have recorded results; final architecture describes implemented behavior and acceptance references actual evidence | 7/7 integration cases + 99/99 suite under `-W error`; P1 PASS (forced zero-warning compile, DEFECT-1 closed); P2 PASS (isolated build, artifact contents, metadata version); DC-2 documentation walk 8/8 with 30 live checks plus all three worked examples executed; `docs/tu_extensions.md` shipped (353 lines) with README pointer; review APPROVED `4292952`; arch APPROVED `94f17ef`; CI 36866455300; integrated `0cc40ea`. Release-end `arch.md` + `log/release_1/arch-review.md` record appended by sw-jerry (`525ffc9`, `e04e341`, CI 36867316907 green) | PASS |

## User Journey Verification (PRD §User journeys)

| Journey | Evidence | Status |
| --- | --- | --- |
| 1. Existing user: unchanged observable behavior | Migration-free case `test_migration_free_legacy_usage_unchanged` (green); legacy guard cases in every TU suite; full 99/99 suite includes pre-existing `test_compatibility` (5) | PASS |
| 2. New client: portable description, discover optional behavior, explicit operation with stated outcome | TU-002 description cases (schema v1, no I/O), TU-003 `describe_operation()`, TU-004 `supports()`, TU-005 `describe_reading()`; user guide §§2–6 with executed worked examples | PASS |
| 3. Resource owner: prepare/use/release after success or failure without disrupting borrowed resources | TU-004 cases 4–8 + recovery cycle; TU-006 cases 3–5, 17 (borrowed non-owner cleanup verified against real `@sim` session); integration case 4 end-to-end owner journey | PASS |
| 4. Model user: describe/configure, explicit error reporting, legacy callers unchanged | TU-007 groups A–C; integration case 5 round trip; legacy `features` fallback pinned green | PASS |

## Non-Functional Requirement Verification

| NFR (PRD §Compatibility and non-functional requirements) | Evidence | Status |
| --- | --- | --- |
| Preserve existing parameter/permission/validation/codec/hook/proxy/snapshot/command/model behavior | TU-001 characterization 17 cases green; legacy guards green in every later suite; only sanctioned change is the recorded TU-006 timeout-sentinel supersession (architecture-sanctioned, documented) | PASS |
| Five-element module responsibilities and public import paths; production confined to `tu`; no `huo`/`mu` production edits | Principle report per task ("no huo/mu change"); TU-008 import identity case; arch record | PASS |
| No new runtime dependencies; no Python support change; actual environments recorded | Principle report (stdlib-only additions); environments recorded per gate (Python 3.13.15 local, 3.9/3.13 CI) | PASS |
| Description-only operations perform no hardware access; synthetic/mocks/`@sim` only; no real hardware | Every gate records mocks/pyvisa-sim only; `@sim` genuineness probed independently in TU-006 | PASS |
| Lightweight virtual-device use preserved; inspection requires no acquisition or connection | TU-004 case 1 (zero resource-manager activity); integration case 2 (describe with zero acquisitions, before_get counter) | PASS |
| Public additions have English docstrings (args/returns/errors/side effects); opt-in vs legacy distinguished | Code reviews APPROVED with docstring scrutiny (TU-006 docstring fix `3314718`; TU-008 doc inaccuracy fixed `829cffd`); user guide distinguishes opt-in and legacy paths | PASS |

## Edge Cases & Error Handling (PRD §Edge cases and errors)

| Scenario | Covered by | Status |
| --- | --- | --- |
| Denied reads/writes | TU-003 case 3/5; TU-005 case 10 | PASS |
| Failed validation/codecs/hooks | TU-001 matrix rows; TU-007 case 9 | PASS |
| Unsupported metadata | TU-002 case 8 (TypeError/ValueError, no coercion) | PASS |
| Arbitrary nonserializable values | TU-001 arbitrary-value row; TU-005 case 6 (explicit TypeError limit) | PASS |
| Nested device lookup | TU-001 delegation row; TU-002 case 4 (rename-survival) | PASS |
| Repeated cleanup / failed initialization | TU-004 cases 4–7; TU-006 case 4 (exact-once close before propagation) | PASS |
| Borrowed/shared handles | TU-004 case 8; TU-006 case 5/17 (`borrow()`, non-owner never closes) | PASS |
| Communication timeouts | TU-006 group B (sentinel contract, ms conversion, error identity) | PASS |
| Unsupported capabilities | TU-004 case 3; TU-006 case 2 | PASS |
| Model evaluation/shape errors | TU-007 cases 2, 10, 11; strict path exposes original error object | PASS |
| No claim that cancelling software stops equipment | TU-006 case 12: abort is bookkeeping-only, `stopped` always `False`; user guide §5 states it explicitly | PASS |

## User Experience Assessment (from the library user's perspective)
- [x] A user can adopt the extensions via `docs/tu_extensions.md`: every new
  API is documented with semantics, and all three worked examples were
  executed as written under `-W error` (DC-2 walk, 8/8 items, 30 checks).
- [x] Limitations are honest: OBS-004 (init-value hazard), OBS-005 (lenient
  `{}` swallow), OBS-006 (name collisions), DEFECT-2 (programmer-error
  warning) and description serialization limits are stated plainly in the
  user guide §8 with workarounds.
- [x] The migration-free guarantee (§9) is pinned by an automated case, not
  just asserted.
- [x] Versioning is explicit (`schema_version: 1` in every description
  namespace), so client authors can rely on a stable contract.

## Documentation Review
- [x] `docs/tu_extensions.md`: complete, accurate against live objects
  (DC-2 walk), README pointer added (`95119c6`).
- [x] `arch.md` (project root): updated to implemented behavior, future work
  explicitly marked **[PLANNED]**; OBS dispositions table current.
- [x] `log/release_1/arch-review.md`: TU-001 review preserved; end-of-release
  record appended per DC-4.
- [x] `log/release_1/compatibility.md`: OBS-001/002/003 dispositions
  resolved/closed; OBS-004/005/006 + DEFECT-2 recorded as known limitations.
- [x] Code/API documentation: docstrings reviewed in all code reviews.

## Evidence-Integrity Rules Verification

| Rule | Result |
| --- | --- |
| TU-001 characterization assertions ran against unchanged production code | YES — test-only task; production `tu` unchanged at characterization; the one later supersession (timeout implicit default) was architecture-sanctioned, test-file-only, and recorded in both `tests/TU-006.md` and `compatibility.md` |
| No unrun gate marked passed | YES — every gate file records actual commands, outputs and environments; DC-4 handoff was explicitly NOT claimed by the tester and was later completed by sw-jerry with its own CI run (36867316907); this acceptance document itself references only recorded evidence |
| No real hardware | YES — mocks + pyvisa-sim `@sim` only; sim genuineness independently probed (real session handle, real Weinschel IDN) |
| CI run IDs exist for each integration | YES — 36302785650, 36306104192, 36320621031, 36323399137, 36325439753, 36725443721, 36860164362, 36866455300, plus 36867316907 for the arch record |
| `main` untouched | YES — `main` packed ref `f288b2f` unchanged; all integrations were fast-forwards to `dev` |
| Existing defects recorded with explicit disposition, never silently promoted | YES — OBS-001–006, DEFECT-1 (fixed, zero behavior change), DEFECT-2 (documented deliberate warning) all carry recorded dispositions |

## Issues Found

None blocking. Reservations (non-blocking, recorded for transparency):

1. **Known limitations remain open by design**: OBS-004, OBS-005, OBS-006
   and DEFECT-2 are recorded technical debt with documented workarounds in
   `docs/tu_extensions.md` §8. The PRD explicitly requires recording defects
   with disposition rather than silently fixing them; correcting them is
   future work, not a Release 1 gap.
2. **Build-time setuptools license-classifier deprecation notice** observed
   during P2 package build (tooling metadata warning, not a runtime warning;
   P1 zero-warning gate unaffected). Recorded; recommend addressing in a
   future packaging hygiene task.
3. **Acceptance method limitation**: per the review mandate, this verdict is
   based on reading recorded evidence, not re-executing tests. CI run links
   reference external GitHub Actions runs whose green status is taken from
   the sprint-board records.
4. **Commit execution**: this verdict and the PRD status update were authored
   by sw-camille; the committing/pushing of this record is performed as the
   accompanying `docs(log):` action per the release workflow.

## Acceptance Conclusion

### Pass Conditions Verification
| Condition | Status |
| --- | --- |
| ALL user stories / PRD rows verified (TU-001–TU-008) | YES |
| ALL user journeys verified | YES |
| ALL functional requirements met | YES |
| ALL non-functional requirements met | YES |
| ALL edge cases covered | YES |
| ALL tests passing (99/99, zero warnings, zero skips) | YES |
| Evidence-integrity rules satisfied | YES |
| Documentation complete, accurate and honest about limitations | YES |
| User experience acceptable (adoption path verified end-to-end) | YES |
| `main` untouched; promotion NOT authorized by this verdict | YES |

### Overall Verdict

**Status**: ✅ **ACCEPTED (PASS)**

**Summary**: Every acceptance criterion in `log/release_1/prd.md` has actual
recorded evidence — characterization, implementation, independent code review
(zero open issues across all eight tasks), tester-executed green gates under
warnings-as-errors, CI run IDs for every integration, and a release-end
architecture record describing the implemented behavior. The evidence-integrity
rules hold: TU-001 ran against unchanged production code, no unrun gate is
marked passed, no real hardware was touched, and `main` is untouched. From the
user's perspective, the extensions are adoptable through an executed,
honest user guide with an explicit migration-free guarantee and plainly stated
limitations. Release 1 is accepted. **This verdict does not authorize promotion
to `main`**; per the PRD, that requires the separate release-promotion step.

**Signed**: sw-camille, Product Owner — 2026-10-01
