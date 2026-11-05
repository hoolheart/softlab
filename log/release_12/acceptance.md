# Acceptance Document — Release 12 (M03 reproducible run record)

## Acceptance Information

- **Reviewer**: sw-camille, Product Owner
- **Date**: 2026-11-05
- **Release**: 12 / M03
- **Review branch and supplied revision**: `codex/m03-p4-acceptance`, `90dc1dc`
- **PRD**: `log/release_12/prd.md`
- **Review type**: final product acceptance; review only, no product-code changes
- **Method**: requirement-to-evidence tracing plus static inspection of the delivered codec, group/storage seams, client wiring, walkthrough assertions, guide, and architecture record. I did not execute tests or runtime workflows and do not claim an independent runtime rerun. Git/CI/worktree operations were not available in this review environment.
- **Current status after re-review**: **FAIL — REJECTED (renewed 2026-11-05 at `218d8fc`)**. Issues 2–4 are closed. Issue 1 remains open because the fresh-environment execution is substantially evidenced, but the required verbatim W1–W3 per-session driver/inspection command record is still absent. AC07's later closure gates remain **PENDING** and are not claimed. See “Acceptance Re-review — 2026-11-05 (`218d8fc`)” below; that section controls over the preserved initial-review record.

## Product Outcome Assessment

The delivered implementation and the W1–W3 evidence demonstrate the core bounded workflow in the recorded project environment: supported synthetic thermal coordinated/direct and signal results can be produced, reopened through the safe separate-process inspection path, and compared with independent frozen expected values. The records expose the reviewed client declarations, units, observed software/source identity, acquisition-only outcome, samples, and distinct execution/group identities.

The release nevertheless **does not meet the complete PRD acceptance contract**. The approved fresh-environment walkthrough was narrowed in execution, while its result and principle inspection still claim a pass. Documentation also contains a false data-safety claim and is not fully current. The product outcome is therefore demonstrated only partially, not accepted for release.

## Test and Review Report Verification

| Phase / gate | Recorded evidence | Acceptance assessment |
| --- | --- | --- |
| Phase 1 characterization | 47 characterization tests; 405 total tests, compile/import checks; C1–C6 all characterized; independent evidence-decision review ACCEPTED | Evidence supports AC01. Historical CI failure corrections and warnings are recorded, not hidden. |
| Phase 2 schema/codec | 54/54 schema tests; 459 total tests; code review APPROVED with zero defects; candidate CI recorded green on Python 3.9/3.13 at `cb2c8ca` | Evidence supports AC02. |
| Phase 3 storage/inspection | 48/48 storage tests; 507 total tests on Python 3.9.23 and 3.13.15; design and code reviews APPROVED | Evidence supports AC03/AC04 within the HDF5-only, new-run, copied-inspection boundary. |
| Phase 4 comparison/walkthrough | 521 total tests reported OK in the project environment; W1/W2/W3 original-versus-rerun comparisons PASS; code review issue closed | Supports AC05 and the project-environment portion of AC06. It does not satisfy the missing fresh-environment commands/scenarios identified below. |
| Fresh-environment W4 | Real venv creation, isolation, editable install, versions, and one thermal coordinated production/inspection workflow recorded | **Incomplete and overclaimed.** The results expressly admit that full unittest discovery, compileall, the exact import smoke command, and W1–W3 A/B sessions did not run in the fresh environment. |
| Independent reviews and failure closure | Phase design/code reviews end APPROVED; earlier findings have owner-closed dispositions | Reviewed evidence is reusable for unchanged content. Acceptance findings in this document remain open and are not waived by those historical approvals. |
| UI gates | No UI work | Not applicable. |
| Compilation/warnings | Executed compileall checks report no output/errors; actual runtime warnings are recorded | No silent zero-runtime-warning claim is accepted. The pandas, sqlite3, pyparsing, and deliberate DEFECT-2 records retain their stated applicability. |

## Acceptance Criterion Mapping

| Criterion | Concrete evidence | Status |
| --- | --- | --- |
| **M03-AC01** | `test/characterization-test-plan.md` and `test/characterization-results.md` cover legacy v1/v2 readers and validation, safe direct HDF5 inspection on disposable copies with hashes, HDF5/SQLite capabilities, production callers, both clients' field ownership, and independent Euler and tick/offset oracles. `design/p1-boundary-decision.md` selects one additive HDF5 JSON envelope before schema implementation and contains explicit Product Owner concurrence. `reviews/p1-evidence-decision-review.md` independently accepts the evidence and decision. | **PASS** |
| **M03-AC02** | `design/p2-schema-design.md` freezes `softlab.run-record`, format/payload versions, closed required fields, bounded values/limits, absent/null/unknown/unavailable semantics, and error taxonomy. `run_record.py` implements strict allowlisting, canonical deterministic JSON, duplicate/nonfinite/unsupported/version rejection, and separate identities without stringification fallback. `test/schema-test-results.md` records 54/54 schema tests and 459 total tests; the sprint board records exact-candidate CI on both configured interpreters. | **PASS** |
| **M03-AC03** | Phase 2/3 designs and `run_record.py`/`run_group.py` preserve closed thermal and signal declarations, model/procedure/timing, ordered semantic columns and units, software/source facts, counts, and acquisition-only outcome. `run_storage.py` checks group identity, attachment, columns, and row count. Execution and group UUIDs remain separate; persistence/plotting/cleanup/overall remain `not_yet_established`. Storage A1/A5 and walkthrough W1–W3 supply stored-artifact evidence. | **PASS** |
| **M03-AC04** | `design/p3-storage-design.md` selects HDF5-only owned writes and copied, read-only preflight plus one legacy load and explicit hydration. `run_storage.py` and `tests/run_record_inspect_bootstrap.py` implement separate-process inspection, legacy-absence versus invalid-present behavior, original re-hashing, and independent primary/cleanup reports. Storage A2–A4/A6 cover repeated inspection, malformed/unsupported reloads, serialization/connect/save/disconnect faults, multi-group selection, SQLite rejection, and excluded generic io helpers; 48/48 tests and the 507-test runs are recorded on both interpreters. | **PASS within the recorded new-run/HDF5 scope**. This does not certify generic metadata round-trip, original-file plain loads, SQLite, or same-ID atomic publication. The guide's broader failed-save safety sentence is separately rejected in Issue 2. |
| **M03-AC05** | `design/p4-oracle-design.md` freezes both thermal adapters and the signal scenario, independent numeric/timing tables, exact comparisons plus thermal `abs_tol=1e-10`, exclusions, and a two-session procedure. Architecture independently recomputed the oracle values and approved the revised freeze. W1/W2/W3 create distinct A/B artifacts in fresh processes, inspect each in separate processes, compare each to the independent oracle, and compare A with B. Legacy suites and the 521-test project-environment run are recorded green. | **PASS for clean-session rerun and compatibility evidence**. The missing fresh-environment repetition is assessed under AC06 rather than hidden here. |
| **M03-AC06** | Fresh venv creation/isolation, editable installation, dependency/version records, and one thermal coordinated workflow exist. Project-environment unittest/compile/import checks and independent code review pass. However, `test/walkthrough-results.md` lines 107–125 and 208–218 explicitly admit that the fresh environment did not run full unittest discovery, compileall, the exact import smoke check, or W1–W3 A/B sessions. The approved `design/p4-oracle-design.md` §6 item 2 and `test/walkthrough-test-plan.md` W4 item 3 require those executions in the fresh environment. Documentation defects also remain open. | **FAIL — Issue 1 blocks acceptance; Issues 2–4 also require documentation correction.** |
| **M03-AC07** | Four principle inspections are recorded PASS, and prior phase candidate revisions/CI links are recorded. This document maps all phases and debt, but acceptance is REJECTED. Inspection 4's statement “no gate skipped” is contradicted by the W4 deviation. Final acceptance-candidate CI, integration to `dev`, promotion to `main`, promoted-state verification, authoritative closure, and clean-worktree verification have not been supplied and remain pending. | **NOT MET / RELEASE REMAINS OPEN**. Later closure steps must not be represented as already passed. |

## Ordering Addendum Assessment

The Phase 1 boundary addendum is **accepted as interpretation-only**:

- It adds no client, backend, field, version, persistence, recovery, or environment capability.
- The envelope sequence remains the canonical semantic declaration and is still checked order-sensitively against the in-memory record before save.
- Physical HDF5 enumeration is treated as non-semantic; read-side verification still requires equal column-name sets and exact four-attribute matches by name.
- Sample ordering remains observable by row index, and the approved Phase 4 oracles reconstruct by declared name rather than enumeration order.
- Thermal's six declarations are already lexicographic, so legacy v1/v2 behavior is unaffected.

The addendum and the corresponding test-plan deltas are explicit and reviewed; this is not a silent broadening or a backend repair.

## Limits and Debt Assessment

| Item | Acceptance disposition |
| --- | --- |
| R4-BASE-SCHED-01 / F-01 / F-02 | Tracked avoidance remains: supported clients use `run_count` and its owned serial seam; no generic scheduler repair or new dependency is claimed. |
| R4-BASE-META-01 / D1 | Constructor metadata loss/mutation and the non-string-key failure remain characterized debt. Supported construction uses an empty group plus encoded text through `set_meta`, not `DataGroup(meta=...)`. |
| R4-BASE-HDF5-01 / F-A | Destructive plain HDF5 loading remains unfixed. Supported inspection uses a fresh disposable copy, preflight, at most one legacy load, explicit hydration, and original-byte re-verification. |
| D2 | SQLite's empty plain-load metadata behavior remains tracked; SQLite is explicitly unsupported for this record. |
| F-B | Custom-key loss is not repaired or generalized away. Only `run_record` and the declared per-profile legacy keys are captured/hydrated. |
| F-C / F-D | `io.reload_group` and `io.load_groups` remain excluded; their `None`/empty outputs are not treated as absence or successful inspection. |
| OBS-004 / OBS-005 / OBS-006 | Existing avoid/fallback/escape-hatch dispositions remain; no M03 path silently changes them. |
| DEFECT-2 | The deliberate neither-readable-nor-writable `Parameter` warning remains documented and was observed again; it is not suppressed or silently closed. |
| **`set_meta(None)` mismatch — separate assessment** | Phase 1 C2.2 establishes that the key remains with `None` on an existing key and is created with `None` on an absent key, contrary to the removal docstring. The mismatch remains tracked and unrepaired. Supported records do not use this method for absence/null; JSON null stays inside validated encoded text. This is non-blocking only under that explicit avoidance, not a waiver. |
| Scope exclusions | No environment/dependency lock, physical validation, real-instrument/VISA qualification, secret collection, arbitrary object/callable serialization, general provenance framework, backend rewrite, retry/rollback, durable partial recovery, later-outcome history, or atomic publication is accepted as delivered scope. |

Nothing in the accepted implementation evidence silently repairs or broadens those items. Issue 2 is a separate documentation overclaim about save safety and must not be used to enlarge the proven boundary.

## Guide and Documentation Review

### Maintainer guide

`docs/run-record.md` accurately describes the supported clients, HDF5-only backend, closed additive envelope, acquisition-only outcome, distinct identities, copied inspection, major exclusions, signal source-versus-derived relation, and the location of the frozen rerun oracle. W7 supplies executed evidence for its thermal/signal production examples and both bootstrap profiles, with substitutions and harness necessities recorded.

The guide is **not acceptable as a whole** while Issue 2's categorical failed-save safety statement remains. Its accurate statements do not cure that contradictory claim.

### Other documentation

- `arch.md` is not current for M03; see Issue 3.
- `examples/signal_monitor.py` contains a contradictory read-semantics docstring; see Issue 4.
- The general README need not enumerate every feature; no separate README acceptance defect is asserted here.
- Several historical plan/design headers retain DRAFT/PENDING wording even though later review sections record approval. Those stale labels are administrative inconsistencies, not the basis of this rejection; the substantive evidence and final review verdicts were used instead.

## Issues Found

### Issue 1: Approved fresh-environment AC06 execution was narrowed but still reported as PASS

- **Severity**: **Blocker**
- **Related requirement**: M03-AC06; Phase 4; frozen oracle design §6 item 2; approved walkthrough plan W4 item 3 and W6 item 2
- **Expected behavior/evidence**: In the genuinely fresh environment, record actual execution of full unittest discovery, `compileall`, the import smoke command, and the W1–W3 original/rerun sessions and inspections for the approved scope, with versions, warnings, limitations, and exact commands. Unrun gates must remain pending.
- **Actual behavior/evidence**: `test/walkthrough-results.md` expressly states that only the minimal thermal coordinated CLI plus bootstrap inspection ran in the fresh venv. Full unittest discovery, compileall, the exact import smoke, and W1–W3 A/B sessions ran only in the project environment or not at all in the fresh venv. Nevertheless, W4 is marked PASS, the AC traceability matrix claims items 1–6, and `principle_compliance_report.md` Inspection 4 says “no gate skipped.” The committed results also summarize session PIDs and driver forms without supplying the complete verbatim command transcript required for every W1–W3 session.
- **User impact**: A maintainer lacks the required proof that the complete supported thermal coordinated/direct and signal workflow independently repeats from the fresh documented environment. The report's PASS wording gives false confidence that this gate was completed.
- **Required correction**: Keep M03-AC06 open. Execute and record the missing fresh-environment commands and all three A/B scenarios on the frozen acceptance candidate, including exact commands, outcomes, versions, warnings, skips, and limitations. Install/record any test dependencies actually needed by the full suite rather than hiding environment failures. If a different scope is intended, obtain the necessary reviewed requirements/plan change instead of unilaterally narrowing execution. Tester-owned closure must correct the W4/AC traceability and affected principle-inspection overclaims before re-review.

### Issue 2: Guide falsely guarantees that a failed save never deletes or truncates the target

- **Severity**: **Major**
- **Related requirement**: PRD Limits/debt; M03-AC04 truthful failure evidence; documentation accuracy gate
- **Expected behavior**: Describe only the evidenced boundary: new-run HDF5 writes, no rollback or atomic publication, and no same-ID deletion/recreation guarantee. Do not imply protection of pre-existing target content that the writer does not provide.
- **Actual behavior**: `docs/run-record.md` §6 states “a failed save never deletes or truncates the target.” The unchanged HDF5 writer (`softlab/shui/data/hdf5.py` `save_impl`, lines 73–76) deletes an existing same-ID group before recreating it. A subsequent save failure can therefore lose the previous group content. The recorded A4.2 empty-new-file and A6.3 successful same-ID probes do not establish the guide's categorical failure-safety claim.
- **User impact**: A maintainer could mistakenly rely on M03 to preserve existing artifact content after a failed save, contrary to the explicit no-atomicity/no-rollback boundary.
- **Required correction**: Correct the guide and any matching storage docstring/design wording to state the actual target/content risk and the unsupported same-ID case. Do not introduce a backend repair under acceptance; any stronger guarantee requires separately reviewed scope, implementation, and failure tests. Re-run the affected guide/content validation after correction.

### Issue 3: Architecture record does not describe the delivered M03 public API and ownership

- **Severity**: **Major**
- **Related requirement**: M03-AC06/AC07 documentation and release-checklist requirement for current architecture/guide records
- **Expected behavior**: `arch.md` reflects the current implemented state, including the `shui` ownership and public seams delivered by this release, with supported limits identified truthfully.
- **Actual behavior**: `arch.md` ends its implemented execution history at M02-W04. Its `shui` boundary lists the older backend/data/profile capabilities but does not describe `run_record.py`, `run_group.py`, `run_storage.py`, their public exports, or the M03 additive-record/safe-inspection boundary. Phase 2/3 design references to the planned additions being marked in `arch.md` are not borne out by the current file.
- **User impact**: A maintainer reading the architecture record cannot discover the actual M03 module responsibilities and public persistence/inspection contracts from the authoritative architecture document.
- **Required correction**: Update `arch.md` to record the implemented M03 modules, public entry points, ownership, HDF5-only/copy-inspection scope, and explicit limits, linking the maintainer guide as appropriate. Perform direct content/link/diff validation; do not fabricate new runtime evidence for a factual documentation correction.

### Issue 4: Signal example docstring contradicts the verified non-consumptive read policy

- **Severity**: **Minor** documentation defect; correct before acceptance re-review
- **Related requirement**: M03-AC05 declared read semantics; documentation accuracy
- **Expected behavior**: The example describes the same non-consumptive `tick + offset` semantics characterized in C5b, frozen in the signal payload, and stated in the guide.
- **Actual behavior**: The module docstring begins “The source is intentionally consumptive,” while `read()` only records the channel/tick and returns `state['tick'] + offset`; it consumes no queued value. The tests, stored payload (`consumptive: False`), oracle, and guide all say non-consumptive.
- **User impact**: The client’s own documentation gives a maintainer the opposite mental model for a central rerun/oracle assumption.
- **Required correction**: Correct the stale docstring without changing acquisition behavior, and verify consistency with the guide and frozen payload. No new runtime claim is needed for the text-only correction; existing behavioral evidence remains applicable.

## Acceptance Conclusion

### Pass Conditions Verification

| Condition | Status |
| --- | --- |
| AC01 characterization and evidence decision | YES |
| AC02 strict bounded schema/codec | YES |
| AC03 stored declarations, units, identity, samples, and stage honesty | YES |
| AC04 owned HDF5 storage, safe inspection, and tested failures within scope | YES, subject to correcting the broader guide overclaim |
| AC05 independent oracles, compatibility, and clean-session A/B comparison | YES |
| AC06 complete fresh-environment/independent-repeat and documentation gate | **NO** |
| AC07 acceptance/principle mapping and final release closure | **NO — acceptance rejected; later closure gates pending** |
| All required reviews/tests applicable to unchanged evidence | Recorded pass, except for the missing AC06 execution identified above |
| Documentation complete, current, and accurate | **NO — Issues 2–4** |
| User experience acceptable within the claimed scope | **NO — the evidence/report overclaim and save-safety documentation are not acceptable** |
| Exact final-candidate CI / promotion / promoted checks / clean worktree | PENDING; none is claimed as passed |

### Overall Verdict — Initial Review (Historical)

**Status: FAIL — REJECTED (initial verdict at `90dc1dc`; renewed at `218d8fc` below)**

Release 12 is **not accepted** and M03 must remain **OPEN**. AC01–AC05 have substantive supporting evidence within the recorded scope, but AC06 is not satisfied and its current PASS/no-gate-skipped presentation is contradicted by the recorded execution deviation. The documentation issues must also be corrected. Final candidate CI, integration, promotion, promoted-state checks, and clean-worktree/closure verification remain pending and cannot proceed as release closure on this rejection.

Re-review must cover Issue 1's new execution evidence and corrected reports, Issue 2–4 documentation corrections, and any affected evidence applicability. No blocker or major issue is waived.

**Signed**: sw-camille, Product Owner

## Commit / Closure Status

This initial acceptance document was written to `log/release_12/acceptance.md`. No commit, push, CI, merge, promotion, or clean-worktree verification was performed because git tooling is unavailable in this review environment. The document therefore requires committing/pushing by the coordinator before it can serve as committed release evidence.

## Acceptance Re-review — 2026-11-05 (`218d8fc`)

This section records the re-review of the initial FAIL at `06ce39c`. It controls over the preserved initial-review sections wherever their findings or verdict differ.

### Re-review Information

- **Reviewer**: sw-camille, Product Owner
- **Date**: 2026-11-05
- **Review branch**: `codex/m03-p4-acceptance`
- **Revision reviewed**: `218d8fc` (`218d8fc6d772cf2e56118afb6a0c11ffe62500cf`, confirmed by the branch ref)
- **Review type**: acceptance re-review only; no product-code changes
- **Method**: requirement-to-evidence tracing, static inspection of the corrected records and source/docstrings, commit-diff applicability checking, and cross-document consistency checks. I did not independently execute tests or runtime workflows. Git commit/push, CI, integration, promotion, and clean-worktree operations were unavailable and were not performed.
- **Evidence applicability**: the complete fresh-environment run names `6fe5acb`. The subsequent `2697fd0` and `9a69cfb` commits change only the walkthrough/principle records, and `218d8fc` changes only docstring whitespace. The runtime evidence therefore remains applicable to the reviewed revision; this is not treated as a candidate mismatch.

### Issue 1 Verification — Fresh-environment AC06 evidence

**Status: OPEN — Blocker; substantially corrected but not complete.**

The following corrections are truthful and retained:

- The earlier narrowed W4 PASS is explicitly superseded in `test/walkthrough-results.md` lines 4–7 and 16–28, in the corrected W4 row, in the closed deviation, and in the AC-matrix preamble. Inspection 4 in `principle_compliance_report.md` now expressly acknowledges that the original “no gate skipped” statement was inaccurate rather than hiding it.
- The tester-created fresh environment records editable installation plus the actually required `pyvisa-sim==0.7.1` test dependency and its transitive packages. It records no propagated `PYTHONPATH`/`PYTHONHOME` and distinguishes this environment from the in-suite nested W4 environment.
- F4 records fresh-environment full discovery: `Ran 521 tests in 480.586s`, `OK`, with zero failures, errors, or skips in the outer run. The nested W6 run's single re-entrancy-guard skip is disclosed and is not represented as an outer-suite skip-free result.
- F5 records `python -m compileall -q softlab` with exit 0 and no output; F6 records the exact import smoke command with exit 0 and softlab `0.3.0`.
- F7 records the explicit W1/W2/W3 test-class invocation in the fresh environment: three tests ran and passed. Its per-scenario evidence supplies distinct A/B process IDs and identities, four successful separate-process inspections per scenario, oracle/A-versus-B outcomes, signal samples/read trace/non-consumptive facts, and original-hash preservation. The execution itself is therefore substantially evidenced; this re-review does not allege that those scenarios did not run.
- Fresh-environment Python/dependency versions, the resolved installation set, warnings and their dispositions, the single-interpreter limitation, optional-extra distinctions, synthetic-data limits, and the post-correction 13-test sanity run are recorded. No undispositioned new warning is claimed away.

The required evidence is nevertheless still incomplete:

1. **The actual W1–W3 session and inspection commands are not recorded verbatim.** The approved plan's pinned subprocess invocation form (lines 68–69) requires the exact command line in the results report **for every session**, and W4 item 3 requires the W1–W3 sessions A/B **driver invocations and bootstrap inspection commands** in the fresh environment, with command, exit code, and key output. The correction supplies only the outer F7 `python -m unittest ...` wrapper command. It does not supply the six actual A/B driver argv lines or the twelve actual bootstrap-inspection argv lines (two per artifact), including their concrete fresh-interpreter, driver/artifact/copy paths, arguments, and outcomes. PIDs, truncated IDs/hashes, driver-form descriptions, and test outcomes do not replace that command transcript. The test helpers construct these argv values at runtime, but their emitted session/inspection evidence does not include the command arrays; source inspection cannot turn them into a record of the commands actually executed.
2. **The claimed verbatim fresh-creation record is abbreviated.** F1 is shown as `python -m venv <tmp>/m03-ac1-correction/venv`, not the literal creation argv and selected interpreter path. The separate in-suite W4 creation record cannot substitute for the distinct tester-created environment.
3. **The replacement completeness wording overclaims the committed transcript.** The statement that the F1–F7 full outputs were “transcribed here verbatim” is not borne out by the `<tmp>` placeholder, ellipsized identities/hashes, and absent driver/bootstrap commands. A reference to temporary external run logs is not the committed verbatim evidence required by the approved plan. Consequently, Inspection 4's historical correction is accurate, but its present `PASS (corrected)` and the results' “complete”/“recorded verbatim” conclusions remain premature until this record gap is closed.

**AC06 traceability matrix, cell-by-cell:**

| Matrix row | Re-review assessment |
| --- | --- |
| W1/W2/W3 → AC06 | **PARTIAL / OVERCLAIMED AS COMPLETE.** FE execution and outcomes are recorded through F7, including under FE discovery, but the required actual A/B driver and bootstrap commands are absent. |
| W4 → AC06 | **PARTIAL / OVERCLAIMED AS COMPLETE.** Full discovery, compileall, import smoke, versions, warnings, and limitations are present, but item 3 lacks the per-session command transcript and F1 is not a literal creation-command record. The item-3 “complete sequence … verbatim” cell therefore cannot yet be accepted. |
| W5 → AC06 | **SUPPORTED.** W5 is included in the recorded FE 521-test discovery run; the process-separation evidence is not newly overclaimed. |
| W6 → AC06 | **SUPPORTED.** FE legacy discovery is green; the outer/nested skip distinction and DEFECT-2 warning are explicit. |
| W7 → AC06 | **SUPPORTED.** The PE guide execution is identified as PE and W7 also ran within FE discovery. The corrected guide was already present at `6fe5acb`. |

The AC05 cells remain supported by the original and fresh A/B outcomes. The matrix's environment labeling does not cure the two incomplete AC06 cells above.

**Remaining required correction:** add the actual fresh-environment command/exit/key-output record for all six A/B driver invocations and all twelve bootstrap inspections, and replace F1 shorthand with the literal creation command and selected interpreter path. If the retained raw logs contain these facts, transcribe them truthfully into committed evidence; otherwise rerun F7 with command-level logging. Remove or qualify the “complete”/“transcribed here verbatim” claims until then, and update the affected matrix and Inspection 4 conclusions. No additional backend or product-code change is authorized by this documentation/evidence correction.

### Issue 2 Verification — Same-ID failed-save risk

**Status: CLOSED.**

- `docs/run-record.md` §6 now states that supported writes are new-run HDF5 writes, that the unchanged writer deletes an existing same-ID group before recreating it, and that a failed save over that group can lose its previous content. It recommends a fresh target path when prior content matters; it no longer gives a categorical failed-save preservation guarantee.
- `softlab/shui/run_storage.py` lines 23–30 and 414–418 distinguish the save seam's own direct actions from the characterized writer's delete-then-recreate behavior and explicitly limit byte-preservation claims to inspection.
- `design/p3-storage-design.md` §2.2 lines 251–259 and §6.3 lines 1214–1222 expressly scope the historical “never deletes or truncates” sentence to the seam's own actions/pinned cases, then disclose the same-ID loss risk and unsupported overwrite case. §7 lines 1264–1269 retains non-atomic same-ID deletion/recreation as unsupported.
- This matches the unchanged implementation: `softlab/shui/data/hdf5.py` lines 73–76 still delete an existing same-ID group before creating its replacement. No rollback, atomic-publication, same-ID preservation, or backend-repair guarantee has been introduced.

The guide, storage docstrings, design clarification, and implementation now convey the same bounded risk. No new contradiction was found in this correction.

### Issue 3 Verification — Architecture currency

**Status: CLOSED.**

- `arch.md` now records `run_record.py`, `run_group.py`, and `run_storage.py` as implemented M03 modules under `shui`, their ownership and public entry points, and links to the relevant designs and maintainer guide.
- The documented package exports agree with `softlab/shui/__init__.py`; module-only constants and non-re-exported storage types are not falsely listed as package exports.
- The HDF5-only/new-run/copy-inspection boundary, acquisition-only outcome, unsupported clients/backends and generic helpers, non-atomic same-ID behavior, and unfixed destructive-load/custom-key limitations are explicit.
- The M03 history entry at lines 972–976 says this is not release acceptance or closure and preserves M03 as OPEN with final candidate CI, `dev` integration, `main` promotion, promoted-state verification, and clean-worktree closure unclaimed. That status remains accurate under this renewed FAIL.
- `tests/test_arch_links.py` checks Markdown links and repository-path spans, and its M03 targets were statically spot-checked against existing files. It was also included in the recorded 521-test fresh discovery at `6fe5acb`, whose result applies to `218d8fc` as explained above. I did not independently rerun the test and make no separate runtime claim.

### Issue 4 Verification — Signal read semantics

**Status: CLOSED.**

`examples/signal_monitor.py` lines 3–5 now say the source is intentionally **non-consumptive**, with one read per channel per cycle and `tick + offset` values (signal 5, reference 2), consuming no queued value. This agrees with `read()` at lines 54–56, C5b in `test/characterization-results.md`, the frozen payload literal `consumptive: False`, the W3 assertions, and `docs/run-record.md` §4. The correction changes wording only and introduces no acquisition-behavior claim or contradiction.

### Cross-document Consistency Assessment

The Issue 2 storage corrections consistently distinguish inspection-time original-byte preservation from save-time same-ID deletion/recreation risk. The Issue 4 example, guide, characterization, schema, and walkthrough consistently use non-consumptive `tick + offset` semantics. The M03 architecture modules/exports/limits agree with the current package initializer and source. No new product-behavior contradiction was introduced by the corrections. The only remaining acceptance defect is Issue 1's incomplete fresh-environment execution transcript and the associated completeness wording.

### Re-reviewed Pass Conditions

| Condition | Status after re-review |
| --- | --- |
| AC01 characterization and evidence decision | YES — unchanged evidence remains applicable |
| AC02 strict bounded schema/codec | YES — unchanged evidence remains applicable |
| AC03 stored declarations, units, identity, samples, and stage honesty | YES — unchanged evidence remains applicable |
| AC04 owned HDF5 storage, safe inspection, and tested failures within scope | YES — the broader guide overclaim is corrected; no stronger guarantee is accepted |
| AC05 independent oracles, compatibility, and clean-session A/B comparison | YES — original and fresh A/B outcomes support this within scope |
| AC06 complete fresh-environment/independent-repeat and documentation gate | **NO — Issue 1's verbatim per-session command record remains incomplete** |
| AC07 acceptance/principle mapping and final release closure | **NO / RELEASE OPEN — product acceptance is not granted; later gates remain pending** |
| Required reviews/tests applicable to unchanged evidence | Recorded pass, but the W4 evidence-completeness gate is not satisfied |
| Documentation complete, current, and accurate | **NO — Issues 2–4 are corrected; the fresh-environment results still overclaim a complete verbatim transcript** |
| User experience acceptable within the claimed scope | **NO — a maintainer still cannot audit the required fresh A/B and inspection commands from the committed record** |
| Exact final-candidate CI / `dev` integration / promotion / promoted checks / clean worktree / closure | **PENDING — none is claimed as passed or completed** |

### Overall Verdict After Re-review (Current)

**Status: FAIL — REJECTED (renewed at `218d8fc`)**

Release 12 remains **unaccepted and OPEN**. Issues 2, 3, and 4 are closed. Issue 1 remains a Blocker solely because the approved AC06 fresh-environment command evidence is still incomplete and is described more completely than the committed transcript supports. AC01–AC05 retain their supporting evidence within the recorded scope; AC06 is not met, and AC07 release closure is not met.

Final acceptance-candidate CI, integration to `dev`, promotion to `main`, promoted-state verification, clean-worktree verification, and authoritative closure remain **PENDING**. They are not authorized as completed release closure by this verdict, and no CI, merge, promotion, or clean-worktree result is claimed. No blocker or major issue is waived.

**Signed**: sw-camille, Product Owner

### Re-review Commit / Closure Status

This re-review was appended to `log/release_12/acceptance.md`. No commit or push was performed because git execution tooling is unavailable in this review environment. The revised document must be committed and pushed by the coordinator before it can serve as committed release evidence. No final candidate CI, integration, promotion, promoted-state check, clean-worktree verification, or closure action was performed.

## Acceptance Re-review — 2026-11-05 (`9913905`, Issue 1 Closure)

This focused re-review covers only the remaining Issue 1 evidence gap from the renewed FAIL at `fefecf3`. It controls over the preserved earlier sections wherever their Issue 1 findings or verdict differ. Issues 2–4 remain **CLOSED** under the previous re-review; their dispositions are unchanged.

### Re-review Information

- **Reviewer**: sw-camille, Product Owner
- **Date**: 2026-11-05
- **Review branch**: `codex/m03-p4-acceptance`
- **Revision reviewed**: `9913905` (`9913905008ddfd7f5bc2d2a80f920215713165d3`, confirmed by the local branch and origin refs)
- **Review type**: focused acceptance re-review; no product-code or test changes
- **Method**: static inspection of the committed walkthrough record, cross-checking representative argv against the instrumented walkthrough helpers and bootstrap CLI, and inspection of the `6fe5acb...9913905` commit diff for evidence applicability. I did not independently execute tests or runtime workflows. Git commit/push, CI, integration, promotion, and clean-worktree operations were unavailable and were not performed.

### Issue 1 Verification — Fresh-environment AC06 Evidence

**Status: CLOSED.**

1. **The required per-session command transcript is now present and auditable.** `test/walkthrough-results.md` lines 160–203 record all six actual W1–W3 A/B session-driver argv arrays and all twelve bootstrap-inspection argv arrays, with arguments, exit 0, and key outputs. `$FE` is explicitly resolved to the concrete fresh-environment interpreter `/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/opencode/m03-ac1-transcript/venv/bin/python`; driver, output, bootstrap-script, and inspection-copy paths name their real temporary roots. The artifact names reported in the key outputs are consistent with each driver's explicit output directory.
2. **Representative plausibility checks pass.** W1 A uses `driver_01.py`, adapter `coordinated`, root `softlab-m03-p4-7ps0felx`, and group `eee3fce4-ae21-406f-b5f3-a6beff63dd0d`; its copies `inspect-copy-03.h5`/`04.h5` use `--profile thermal` and PIDs 98035/98039. W2 B uses `driver_10.py`, adapter `direct`, root `softlab-m03-p4-bxdvuoqo`, and group `7f127cf4-da1b-49d8-9138-38313a29ada2`; copies `13`/`14` use `thermal` and PIDs 98109/98119. W3 A/B use `driver_17.py`/`18.py`, groups `e335e998-162e-4ec5-ad92-d9f248c0f9c5`/`648876a7-b1e4-4030-9367-0ebd52020f09`, and copies `19`/`20` and `21`/`22` with `--profile signal` and the recorded PIDs. These forms match `_run_python`, `_run_driver`, `_inspect`, the copy counter, and the bootstrap parser. The actual inspection argv does not pass the optional `--group-id`; this matches the instrumented helper and the approved pinned invocation, so there is no group-selector mismatch to reconcile. Each recorded inspection exits 0 with `status: ok` on its own fresh copy.
3. **F1 is literal.** It records `/Users/edward/workspace/softlab/.venv/bin/python -m venv /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/opencode/m03-ac1-transcript/venv`, including the real selected interpreter and target, exit 0, and empty stdout/stderr. No `<tmp>` placeholder remains in F1.
4. **Completeness wording is now supported at the required command/exit/key-output granularity.** The committed transcript, rather than the external raw log, is sufficient for the required audit. The raw-log reference at lines 201–203 is expressly provenance-only. Earlier narrowed claims are retained only as explicitly superseded history; the current W4 verdict, correction record, and AC matrix identify the `93336e0` transcript run and the standing `6fe5acb` F4–F6 evidence rather than conflating them.
5. **The AC06 matrix is supported.** The W1/W2/W3 cell is backed by the six fresh-environment driver commands, twelve inspection commands, scenario outcomes, and A/B assertions. The W4 cell is backed by literal creation/install records, standing full-discovery/compile/import evidence, the new W1–W3 transcript, and recorded versions/warnings/limitations. The W5/W6/W7 environment labels remain consistent with the full-discovery and guide evidence.
6. **F4–F6 evidence reuse is honest and applicable.** The record expressly says those commands were not re-run in the new transcript venv and identifies their `6fe5acb` provenance. The `6fe5acb...9913905` diff contains only acceptance/principle/walkthrough records, one `softlab/shui/run_storage.py` docstring-whitespace correction, and test-only argv/evidence instrumentation in `tests/test_m03_p4_walkthrough.py`. There is no product-behavior drift; the final `9913905` change is the walkthrough record itself. Reusing F4–F6 therefore does not hide a changed runtime path or an unrun gate.

The remaining evidence defect identified at `fefecf3` is corrected without broadening the supported product scope. No Issue 1 blocker remains.

### Focused Pass Conditions

| Condition | Status after Issue 1 closure |
| --- | --- |
| AC01–AC05 | **YES** — previously accepted evidence remains applicable |
| AC06 complete fresh-environment/independent-repeat and documentation gate | **YES** — Issue 1 is closed; F4–F6 applicability is explicit |
| Issues 1–4 | **ALL CLOSED** — Issues 2–4 retain their prior closures |
| Required reviews/tests applicable to unchanged evidence | **YES** — recorded passes are reused only where applicable |
| Documentation complete, current, and accurate for product acceptance | **YES** — the remaining transcript/completeness defect is corrected |
| User experience within the claimed scope | **YES** — a maintainer can now audit the fresh-environment commands and outcomes from committed evidence |
| AC07 release closure | **PENDING / RELEASE OPEN** — product acceptance is granted, but the later gates below are not complete |
| Exact final-candidate CI / `dev` integration / promotion / promoted-state checks / clean worktree / authoritative closure | **PENDING** — none is claimed as passed or completed |

### Overall Verdict After Issue 1 Closure (Current)

**Status: PASS — ACCEPTED (product acceptance at `9913905`)**

Release 12 receives Product Owner acceptance within the PRD's recorded scope. Issues 1–4 are closed, and AC01–AC06 are satisfied by the committed evidence and the explicitly applicable standing records. This verdict supersedes the earlier FAIL verdicts for product acceptance; it does not waive any recorded limitation or debt.

**AC07's later gates remain explicitly PENDING:** exact final-candidate CI, integration to `dev`, promotion to `main`, promoted-state verification and required promoted-state checks, clean-worktree verification, and authoritative release closure. The release therefore remains **OPEN**, not CLOSED. No CI, merge, promotion, promoted-state, clean-worktree, or closure result is claimed by this acceptance verdict.

**Signed**: sw-camille, Product Owner

### Issue 1 Closure Commit / Release Status

This focused re-review was appended to `log/release_12/acceptance.md`. No commit or push was performed because git execution tooling is unavailable in this review environment. The coordinator must commit and push this document before it can serve as committed release evidence. All later AC07 gates and release closure remain **PENDING**.
